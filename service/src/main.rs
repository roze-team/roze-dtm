use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use roze_dtm::{
    Branch, BranchKind, BranchStatus, BranchUrlPolicy, Dtm, DtmOptions, HttpBranchInvoker,
    InMemoryTransactionStore, MySqlTransactionStore, PostgresTransactionStore,
    SqliteTransactionStore, Transaction, TransactionKind, TransactionStatus, TransactionStore,
};
use roze_http::{
    rest::{self, HttpResponse, RestServer, RestService},
    routing::{get, post},
    Json, Path, Query, Router, State,
};
use roze_service::{LifecycleState, ServiceGroup};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationConfig {
    #[serde(default)]
    dtm: DtmConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DtmConfig {
    #[serde(default)]
    store: StoreConfig,
    #[serde(default = "default_max_attempts")]
    max_attempts: u32,
    #[serde(default = "default_retry_backoff_ms")]
    retry_backoff_ms: u64,
    #[serde(default = "default_max_retry_backoff_ms")]
    max_retry_backoff_ms: u64,
    #[serde(default = "default_branch_call_timeout_ms")]
    branch_call_timeout_ms: u64,
    #[serde(default = "default_transaction_timeout_ms")]
    transaction_timeout_ms: u64,
    #[serde(default)]
    control_token: Option<String>,
    #[serde(default = "default_recover_interval_ms")]
    recover_interval_ms: u64,
    #[serde(default = "default_recovery_lease_ttl_ms")]
    recovery_lease_ttl_ms: u64,
    #[serde(default = "default_worker_id")]
    worker_id: String,
    #[serde(default)]
    allowed_branch_origins: Vec<String>,
}

impl Default for DtmConfig {
    fn default() -> Self {
        Self {
            store: StoreConfig::default(),
            max_attempts: default_max_attempts(),
            retry_backoff_ms: default_retry_backoff_ms(),
            max_retry_backoff_ms: default_max_retry_backoff_ms(),
            branch_call_timeout_ms: default_branch_call_timeout_ms(),
            transaction_timeout_ms: default_transaction_timeout_ms(),
            control_token: None,
            recover_interval_ms: default_recover_interval_ms(),
            recovery_lease_ttl_ms: default_recovery_lease_ttl_ms(),
            worker_id: default_worker_id(),
            allowed_branch_origins: Vec::new(),
        }
    }
}

impl DtmConfig {
    fn validate(&self, production: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_attempts > 0,
            "application.dtm.max_attempts must be positive"
        );
        anyhow::ensure!(
            self.retry_backoff_ms > 0,
            "application.dtm.retry_backoff_ms must be positive"
        );
        anyhow::ensure!(
            self.max_retry_backoff_ms >= self.retry_backoff_ms,
            "application.dtm.max_retry_backoff_ms must be at least retry_backoff_ms"
        );
        anyhow::ensure!(
            self.branch_call_timeout_ms > 0,
            "application.dtm.branch_call_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.transaction_timeout_ms >= self.branch_call_timeout_ms,
            "application.dtm.transaction_timeout_ms must be at least branch_call_timeout_ms"
        );
        if production {
            anyhow::ensure!(
                self.control_token
                    .as_deref()
                    .is_some_and(|token| token.len() >= 32),
                "application.dtm.control_token must contain at least 32 bytes in production"
            );
        }
        anyhow::ensure!(
            (100..=3_600_000).contains(&self.recover_interval_ms),
            "application.dtm.recover_interval_ms must be between 100 and 3600000"
        );
        anyhow::ensure!(
            self.recovery_lease_ttl_ms >= self.recover_interval_ms.saturating_mul(2)
                && self.recovery_lease_ttl_ms <= 86_400_000,
            "application.dtm.recovery_lease_ttl_ms must be at least twice recover_interval_ms and at most 86400000"
        );
        anyhow::ensure!(
            !self.worker_id.trim().is_empty() && self.worker_id.len() <= 128,
            "application.dtm.worker_id must contain between 1 and 128 bytes"
        );
        if production {
            anyhow::ensure!(
                self.worker_id != default_worker_id(),
                "application.dtm.worker_id must be deployment-unique in production"
            );
        }
        anyhow::ensure!(
            !self.allowed_branch_origins.is_empty(),
            "application.dtm.allowed_branch_origins is required"
        );
        BranchUrlPolicy::from_allowed_origins(&self.allowed_branch_origins)
            .context("application.dtm.allowed_branch_origins is invalid")?;
        match self.store.kind {
            StoreKind::Memory => anyhow::ensure!(
                !production,
                "application.dtm.store.kind=memory is forbidden in production"
            ),
            StoreKind::Sqlite => self.store.validate_url("sqlite", &["sqlite:"])?,
            StoreKind::Postgres => self
                .store
                .validate_url("postgres", &["postgres://", "postgresql://"])?,
            StoreKind::Mysql => self.store.validate_url("mysql", &["mysql://"])?,
        }
        Ok(())
    }

    fn options(&self) -> DtmOptions {
        DtmOptions {
            max_attempts: self.max_attempts,
            retry_backoff_millis: self.retry_backoff_ms,
            max_retry_backoff_millis: self.max_retry_backoff_ms,
            branch_call_timeout_millis: self.branch_call_timeout_ms,
            transaction_timeout_millis: self.transaction_timeout_ms,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreConfig {
    #[serde(default)]
    kind: StoreKind,
    #[serde(default)]
    database_url: Option<String>,
    #[serde(default = "default_store_max_connections")]
    max_connections: u32,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            kind: StoreKind::Memory,
            database_url: None,
            max_connections: default_store_max_connections(),
        }
    }
}

impl StoreConfig {
    fn validate_url(&self, kind: &str, schemes: &[&str]) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=1_000).contains(&self.max_connections),
            "application.dtm.store.max_connections must be between 1 and 1000"
        );
        let url = self
            .database_url
            .as_deref()
            .filter(|url| !url.trim().is_empty())
            .with_context(|| {
                format!("application.dtm.store.database_url is required for {kind}")
            })?;
        anyhow::ensure!(
            schemes.iter().any(|scheme| url.starts_with(scheme)),
            "application.dtm.store.database_url scheme does not match store kind {kind}"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum StoreKind {
    #[default]
    Memory,
    Sqlite,
    Postgres,
    Mysql,
}

type DtmRuntime = Dtm<Arc<dyn TransactionStore>, HttpBranchInvoker>;

#[derive(Clone)]
struct ControlState {
    dtm: Arc<DtmRuntime>,
    branch_url_policy: BranchUrlPolicy,
    control_token: Option<Arc<str>>,
    lifecycle: Option<LifecycleState>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitTransactionRequest {
    gid: String,
    #[serde(default)]
    kind: Option<TransactionKind>,
    branches: Vec<BranchRequest>,
    #[serde(default)]
    timeout_millis: Option<u64>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchRequest {
    id: String,
    #[serde(default)]
    kind: Option<BranchKind>,
    action: String,
    #[serde(default)]
    compensate: Option<String>,
    #[serde(default)]
    confirm: Option<String>,
    #[serde(default)]
    cancel: Option<String>,
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default)]
    dependencies: Vec<String>,
}

#[derive(Deserialize)]
struct GidPath {
    gid: String,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TransactionQuery {
    gid: Option<String>,
    kind: Option<String>,
    status: Option<String>,
    offset: usize,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct TransactionPage {
    items: Vec<Transaction>,
    offset: usize,
    limit: usize,
    total: usize,
}

#[derive(Serialize)]
struct TransactionStats {
    total: usize,
    by_kind: BTreeMap<String, usize>,
    by_status: BTreeMap<String, usize>,
}

#[derive(Serialize)]
struct RecoveryResult {
    recovered: Vec<Transaction>,
    count: usize,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct CompatQuery {
    gid: Option<String>,
    #[serde(rename = "transType", alias = "trans_type")]
    trans_type: Option<String>,
    status: Option<String>,
    position: usize,
    limit: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct CompatTransactionRequest {
    gid: String,
    trans_type: String,
    steps: Vec<BTreeMap<String, String>>,
    payloads: Vec<serde_json::Value>,
    timeout_to_fail: Option<u64>,
    rollback_reason: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct CompatBranchRequest {
    gid: String,
    trans_type: String,
    branch_id: String,
    data: Option<String>,
    confirm: Option<String>,
    cancel: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = roze_config::service_config_path(env!("CARGO_MANIFEST_DIR"));
    let config = roze_config::load_service_with_application::<ApplicationConfig>(&path)?;
    let production = config.profile == roze_config::ServiceProfile::Production;
    config.application.dtm.validate(production)?;
    let rest = config
        .rest
        .as_ref()
        .context("roze-dtm requires rest config")?;
    let _tracing_guard = roze_log::init_tracing_with_config(&config.service)?;

    let store: Arc<dyn TransactionStore> = match config.application.dtm.store.kind {
        StoreKind::Memory => Arc::new(InMemoryTransactionStore::new()),
        StoreKind::Sqlite => {
            let pool = roze_sqlx::connect_sqlite(
                store_url(&config.application.dtm.store)?,
                config.application.dtm.store.max_connections,
            )
            .await?;
            let store = SqliteTransactionStore::from_pool(pool);
            store.migrate().await?;
            Arc::new(store)
        }
        StoreKind::Postgres => {
            let pool = roze_sqlx::connect_postgres(
                store_url(&config.application.dtm.store)?,
                config.application.dtm.store.max_connections,
            )
            .await?;
            let store = PostgresTransactionStore::from_pool(pool);
            store.migrate().await?;
            Arc::new(store)
        }
        StoreKind::Mysql => {
            let pool = roze_sqlx::connect_mysql(
                store_url(&config.application.dtm.store)?,
                config.application.dtm.store.max_connections,
            )
            .await?;
            let store = MySqlTransactionStore::from_pool(pool);
            store.migrate().await?;
            Arc::new(store)
        }
    };
    let branch_url_policy =
        BranchUrlPolicy::from_allowed_origins(&config.application.dtm.allowed_branch_origins)?;
    let invoker = HttpBranchInvoker::with_timeout_and_policy(
        Duration::from_millis(config.application.dtm.branch_call_timeout_ms),
        branch_url_policy.clone(),
    )?;
    let dtm: Arc<DtmRuntime> = Arc::new(Dtm::with_options(
        store,
        invoker,
        config.application.dtm.options(),
    ));
    let addr = rest.addr;
    tracing::info!(
        event = roze_log::events::SERVICE_CONFIG_LOADED,
        service = %config.name,
        protocol = "http",
        config_path = %path.display(),
        "DTM service configuration loaded"
    );

    let mut group = ServiceGroup::new();
    let recovery_dtm = Arc::clone(&dtm);
    let state = ControlState {
        dtm,
        branch_url_policy,
        control_token: config
            .application
            .dtm
            .control_token
            .clone()
            .map(Arc::<str>::from),
        lifecycle: Some(group.lifecycle()),
    };
    let service = control_router(state);
    tracing::info!(
        event = roze_log::events::SERVICE_STARTING,
        service = %config.name,
        protocol = "http",
        addr = %addr,
        "DTM service starting"
    );
    group.add(RestService::new(
        config.name.clone(),
        RestServer::new(addr, service),
    ));
    let recovery_interval = Duration::from_millis(config.application.dtm.recover_interval_ms);
    let recovery_lease_ttl_ms = config.application.dtm.recovery_lease_ttl_ms;
    let recovery_worker_id = config.application.dtm.worker_id.clone();
    group.add_fn("dtm-recovery", move |shutdown| {
        let dtm = Arc::clone(&recovery_dtm);
        let worker_id = recovery_worker_id.clone();
        async move {
            let mut ticker = tokio::time::interval(recovery_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.clone().wait() => break,
                    _ = ticker.tick() => {
                        match dtm
                            .tick_recover_once_with_lease(&worker_id, recovery_lease_ttl_ms)
                            .await
                        {
                            Ok(recovered) if !recovered.is_empty() => {
                                tracing::info!(
                                    event = "dtm.recovery.completed",
                                    recovered_count = recovered.len(),
                                    "DTM recovery worker advanced transactions"
                                );
                                roze_log::audit_info!(
                                    event = "dtm.recovery.completed",
                                    actor_kind = "recovery_worker",
                                    operation = "recover",
                                    outcome = "success",
                                    transaction_count = recovered.len(),
                                    "DTM recovery worker advanced transactions"
                                );
                            }
                            Ok(_) => {}
                            Err(_) => tracing::error!(
                                event = "dtm.recovery.failed",
                                error_kind = "recovery_tick_failed",
                                "DTM recovery tick failed"
                            ),
                        }
                    }
                }
            }
            tracing::info!(
                event = "dtm.recovery.stopped",
                "DTM recovery worker stopped"
            );
            Ok(())
        }
    });
    let result = group.start().await;
    match &result {
        Ok(()) => tracing::info!(
            event = roze_log::events::SERVICE_STOPPED,
            service = %config.name,
            protocol = "http",
            "DTM service stopped"
        ),
        Err(_) => tracing::error!(
            event = roze_log::events::SERVICE_FAILED,
            service = %config.name,
            protocol = "http",
            error_kind = "lifecycle",
            "DTM service failed"
        ),
    }
    result
}

fn control_router(state: ControlState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/startupz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/tcc", post(submit_tcc))
        .route("/v1/tcc/{gid}/prepare", post(prepare_tcc))
        .route("/v1/tcc/{gid}/confirm", post(confirm_tcc))
        .route("/v1/tcc/{gid}/cancel", post(cancel_tcc))
        .route("/v1/saga", post(submit_saga))
        .route("/v1/saga/{gid}/start", post(start_saga))
        .route("/v1/saga/{gid}/abort", post(abort_saga))
        .route("/v1/workflows", post(submit_workflow))
        .route("/v1/workflows/{gid}/start", post(start_workflow))
        .route("/v1/workflows/{gid}/abort", post(abort_workflow))
        .route("/v1/messages", post(submit_message))
        .route("/v1/messages/{gid}/prepare", post(prepare_message))
        .route("/v1/messages/{gid}/dispatch", post(dispatch_message))
        .route("/v1/messages/{gid}/abort", post(abort_message))
        .route("/v1/xa", post(submit_xa))
        .route("/v1/xa/{gid}/prepare", post(prepare_xa))
        .route("/v1/xa/{gid}/commit", post(commit_xa))
        .route("/v1/xa/{gid}/rollback", post(rollback_xa))
        .route("/v1/transactions", get(list_transactions))
        .route("/v1/transactions/{gid}", get(get_transaction))
        .route("/v1/transactions/{gid}/recover", post(recover_transaction))
        .route("/v1/transactions/{gid}/force-stop", post(force_stop_transaction))
        .route("/v1/transactions/{gid}/reset-retry", post(reset_retry_transaction))
        .route("/v1/recover", post(recover_all))
        .route("/v1/stats", get(stats))
        .route("/api/dtmsvr/version", get(compat_version))
        .route("/api/dtmsvr/newGid", get(compat_new_gid))
        .route("/api/dtmsvr/query", get(compat_query))
        .route("/api/dtmsvr/all", get(compat_all))
        .route("/api/dtmsvr/prepare", post(compat_prepare))
        .route("/api/dtmsvr/submit", post(compat_submit))
        .route("/api/dtmsvr/abort", post(compat_abort))
        .route("/api/dtmsvr/registerBranch", post(compat_register_branch))
        .route("/api/dtmsvr/registerTccBranch", post(compat_register_branch))
        .route("/api/dtmsvr/registerXaBranch", post(compat_register_branch))
        .route("/api/dtmsvr/forceStop", post(compat_force_stop))
        .route("/api/dtmsvr/resetNextCronTime", post(compat_reset_retry))
        .route("/api/dtmsvr/resetCronTime", get(compat_reset_retry_batch))
        .route("/api/json-rpc", post(json_rpc))
        .with_state(state)
}

async fn health() -> HttpResponse {
    ok_response("ok")
}

async fn metrics() -> String {
    roze_metrics::http_metrics()
}

async fn compat_version() -> HttpResponse {
    compat_response(serde_json::json!({"version": env!("CARGO_PKG_VERSION")}))
}

async fn compat_new_gid(State(state): State<ControlState>, headers: HeaderMap) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    compat_response(serde_json::json!({
        "gid": generate_gid(),
        "dtm_result": "SUCCESS"
    }))
}

fn generate_gid() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{millis:x}{sequence:08x}")
}

async fn compat_query(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Query(query): Query<CompatQuery>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    let Some(gid) = query.gid else {
        return compat_failure("no gid specified");
    };
    match state.dtm.store().get_transaction(&gid).await {
        Ok(Some(transaction)) => compat_response(serde_json::json!({
            "transaction": &transaction,
            "branches": &transaction.branches,
            "dtm_result": "SUCCESS"
        })),
        Ok(None) => compat_failure("transaction not found"),
        Err(_) => compat_failure("storage failure"),
    }
}

async fn compat_all(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Query(query): Query<CompatQuery>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    match state.dtm.list().await {
        Ok(transactions) => {
            let items = transactions
                .into_iter()
                .filter(|tx| {
                    query.gid.as_deref().is_none_or(|gid| tx.gid == gid)
                        && query
                            .trans_type
                            .as_deref()
                            .is_none_or(|kind| kind_name(tx.kind) == kind)
                        && query
                            .status
                            .as_deref()
                            .is_none_or(|status| status_name(tx.status) == status)
                })
                .skip(query.position)
                .take(limit)
                .collect::<Vec<_>>();
            compat_response(
                serde_json::json!({"transactions": items, "next_position": query.position + limit, "dtm_result": "SUCCESS"}),
            )
        }
        Err(_) => compat_failure("storage failure"),
    }
}

fn compat_response(value: serde_json::Value) -> HttpResponse {
    rest::json_response(StatusCode::OK, &value)
}

fn compat_failure(message: &str) -> HttpResponse {
    rest::json_response(
        StatusCode::BAD_REQUEST,
        &serde_json::json!({"dtm_result": "FAILURE", "message": message}),
    )
}

async fn compat_force_stop(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatTransactionRequest>,
) -> HttpResponse {
    compat_admin_operation(&state, &headers, &request.gid, true).await
}

async fn compat_reset_retry(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatTransactionRequest>,
) -> HttpResponse {
    compat_admin_operation(&state, &headers, &request.gid, false).await
}

async fn compat_admin_operation(
    state: &ControlState,
    headers: &HeaderMap,
    gid: &str,
    force_stop: bool,
) -> HttpResponse {
    if !authorize(state, headers) {
        return unauthorized_response();
    }
    let result = if force_stop {
        state.dtm.force_stop(gid).await
    } else {
        state.dtm.reset_retry(gid).await
    };
    match result {
        Ok(_) => compat_response(serde_json::json!({"dtm_result": "SUCCESS"})),
        Err(_) => compat_failure("administrative operation failed"),
    }
}

async fn compat_reset_retry_batch(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Query(query): Query<CompatQuery>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1_000);
    match state.dtm.reset_retry_batch(limit).await {
        Ok(reset) => compat_response(serde_json::json!({
            "succeed_count": reset.len(),
            "has_remaining": reset.len() == limit,
            "dtm_result": "SUCCESS"
        })),
        Err(_) => compat_failure("batch retry reset failed"),
    }
}

async fn compat_prepare(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatTransactionRequest>,
) -> HttpResponse {
    compat_write(&state, &headers, request, CompatOperation::Prepare).await
}

async fn compat_submit(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatTransactionRequest>,
) -> HttpResponse {
    compat_write(&state, &headers, request, CompatOperation::Submit).await
}

async fn compat_abort(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatTransactionRequest>,
) -> HttpResponse {
    compat_write(&state, &headers, request, CompatOperation::Abort).await
}

#[derive(Clone, Copy)]
enum CompatOperation {
    Prepare,
    Submit,
    Abort,
}

async fn compat_write(
    state: &ControlState,
    headers: &HeaderMap,
    request: CompatTransactionRequest,
    operation: CompatOperation,
) -> HttpResponse {
    if !authorize(state, headers) {
        return unauthorized_response();
    }
    let gid = request.gid.clone();
    let result = compat_apply(state, request, operation).await;
    match result {
        Ok(_) => compat_response(serde_json::json!({"dtm_result": "SUCCESS"})),
        Err(_) => {
            tracing::warn!(event = "dtm.compat.failed", gid, error_kind = "compat_operation", "DTM compatibility operation failed");
            compat_failure("compatibility operation failed")
        }
    }
}

async fn compat_apply(
    state: &ControlState,
    request: CompatTransactionRequest,
    operation: CompatOperation,
) -> anyhow::Result<Transaction> {
    let kind = parse_kind(&request.trans_type).map_err(anyhow::Error::msg)?;
    let gid = request.gid.trim().to_owned();
    anyhow::ensure!(!gid.is_empty() && gid.len() <= 128, "invalid gid");
    let existing = state.dtm.store().get_transaction(&gid).await?;
    if existing.is_none() {
        let mut transaction = compat_transaction(kind, &request, &state.branch_url_policy)?;
        if let Some(seconds) = request.timeout_to_fail {
            transaction.timeout_millis = Some(seconds.saturating_mul(1_000));
        }
        if let Some(reason) = request.rollback_reason {
            transaction.metadata.insert("rollback_reason".to_string(), reason);
        }
        state.dtm.submit(transaction).await?;
    }
    match (kind, operation) {
        (TransactionKind::Tcc, CompatOperation::Prepare) => state.dtm.prepare_tcc(&gid).await,
        (TransactionKind::Tcc, CompatOperation::Submit) => state.dtm.confirm_tcc(&gid).await,
        (TransactionKind::Tcc, CompatOperation::Abort) => state.dtm.cancel_tcc(&gid).await,
        (TransactionKind::Xa, CompatOperation::Prepare) => state.dtm.prepare_xa(&gid).await,
        (TransactionKind::Xa, CompatOperation::Submit) => state.dtm.commit_xa(&gid).await,
        (TransactionKind::Xa, CompatOperation::Abort) => state.dtm.rollback_xa(&gid).await,
        (TransactionKind::Message, CompatOperation::Prepare) => state.dtm.prepare_message(&gid).await,
        (TransactionKind::Message, CompatOperation::Submit) => state.dtm.dispatch_message(&gid).await,
        (TransactionKind::Message, CompatOperation::Abort) => state.dtm.abort_message(&gid).await,
        (TransactionKind::Saga, CompatOperation::Submit) => state.dtm.start_saga(&gid).await,
        (TransactionKind::Saga, CompatOperation::Abort) => state.dtm.abort_saga(&gid).await,
        (TransactionKind::Workflow, CompatOperation::Submit) => state.dtm.start_workflow(&gid).await,
        (TransactionKind::Workflow, CompatOperation::Abort) => state.dtm.abort_workflow(&gid).await,
        (_, CompatOperation::Prepare) => state
            .dtm
            .store()
            .get_transaction(&gid)
            .await?
            .context("transaction not found"),
    }
}

async fn compat_register_branch(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<CompatBranchRequest>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    let gid = request.gid.clone();
    let branch = match compat_branch_from_request(&state, request) {
        Ok(branch) => branch,
        Err(_) => return compat_failure("invalid branch registration"),
    };
    match state.dtm.register_branch(&gid, branch).await {
        Ok(_) => compat_response(serde_json::json!({"dtm_result": "SUCCESS"})),
        Err(_) => compat_failure("branch registration failed"),
    }
}

fn compat_branch_from_request(
    state: &ControlState,
    request: CompatBranchRequest,
) -> anyhow::Result<Branch> {
    let payload = request
        .data
        .as_deref()
        .and_then(|data| serde_json::from_str(data).ok())
        .unwrap_or(serde_json::Value::Null);
    let branch = match parse_kind(&request.trans_type) {
        Ok(TransactionKind::Tcc) => {
            let confirm = request.confirm.context("TCC confirm URL is required")?;
            let cancel = request.cancel.context("TCC cancel URL is required")?;
            if state.branch_url_policy.validate(&confirm).is_err()
                || state.branch_url_policy.validate(&cancel).is_err()
            {
                anyhow::bail!("branch URL is not allowed");
            }
            let mut branch = Branch::tcc_try(request.branch_id, "", confirm, cancel, payload);
            branch.status = BranchStatus::Succeeded;
            branch
        }
        Ok(TransactionKind::Xa) => {
            let url = request.url.context("XA phase2 URL is required")?;
            if state.branch_url_policy.validate(&url).is_err() {
                anyhow::bail!("branch URL is not allowed");
            }
            Branch::xa(request.branch_id, &url, url, payload)
        }
        _ => anyhow::bail!("dynamic registration supports tcc and xa"),
    };
    Ok(branch)
}

async fn json_rpc(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> HttpResponse {
    let id = request.id.clone();
    if !authorize(&state, &headers) {
        return json_rpc_error(id, -32001, "unauthorized");
    }
    if request.jsonrpc != "2.0" || request.id.is_null() {
        return json_rpc_error(id, -32600, "invalid request");
    }
    let result = match request.method.as_str() {
        "newGid" => Ok(serde_json::json!({"gid": generate_gid()})),
        "prepare" | "submit" | "abort" => {
            let operation = match request.method.as_str() {
                "prepare" => CompatOperation::Prepare,
                "submit" => CompatOperation::Submit,
                _ => CompatOperation::Abort,
            };
            match serde_json::from_value::<CompatTransactionRequest>(request.params) {
                Ok(params) => compat_apply(&state, params, operation)
                    .await
                    .map(|_| serde_json::json!({})),
                Err(error) => Err(error.into()),
            }
        }
        "registerBranch" => {
            match serde_json::from_value::<CompatBranchRequest>(request.params) {
                Ok(params) => {
                    let gid = params.gid.clone();
                    match compat_branch_from_request(&state, params) {
                        Ok(branch) => state
                            .dtm
                            .register_branch(&gid, branch)
                            .await
                            .map(|_| serde_json::json!({})),
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error.into()),
            }
        }
        _ => return json_rpc_error(id, -32601, "method not found"),
    };
    match result {
        Ok(result) => rest::json_response(
            StatusCode::OK,
            &serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}),
        ),
        Err(_) => json_rpc_error(id, -32603, "operation failed"),
    }
}

fn json_rpc_error(id: serde_json::Value, code: i32, message: &str) -> HttpResponse {
    rest::json_response(
        StatusCode::OK,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message}
        }),
    )
}

fn compat_transaction(
    kind: TransactionKind,
    request: &CompatTransactionRequest,
    policy: &BranchUrlPolicy,
) -> anyhow::Result<Transaction> {
    let mut branches = Vec::with_capacity(request.steps.len());
    for (index, step) in request.steps.iter().enumerate() {
        let id = format!("{:02}", index + 1);
        let payload = request
            .payloads
            .get(index)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let payload = match payload {
            serde_json::Value::String(encoded) => {
                serde_json::from_str(&encoded).unwrap_or(serde_json::Value::String(encoded))
            }
            payload => payload,
        };
        let action = step.get("action").context("step action is required")?;
        policy.validate(action)?;
        let branch = match kind {
            TransactionKind::Saga => {
                let compensate = step.get("compensate").context("step compensate is required")?;
                policy.validate(compensate)?;
                Branch::saga(id, action, compensate, payload)
            }
            TransactionKind::Workflow => {
                let compensate = step.get("compensate").context("step compensate is required")?;
                policy.validate(compensate)?;
                let dependencies = if index == 0 { Vec::new() } else { vec![format!("{:02}", index)] };
                Branch::workflow(id, action, compensate, dependencies, payload)
            }
            TransactionKind::Message => Branch::message(id, action, payload),
            TransactionKind::Tcc | TransactionKind::Xa => {
                anyhow::bail!("tcc and xa branches must be registered dynamically")
            }
        };
        branches.push(branch);
    }
    match kind {
        TransactionKind::Tcc => Ok(Transaction::tcc(&request.gid, Vec::new())),
        TransactionKind::Xa => Ok(Transaction::xa(&request.gid, Vec::new())),
        TransactionKind::Saga => Ok(Transaction::saga(&request.gid, branches)),
        TransactionKind::Workflow => Ok(Transaction::workflow(&request.gid, branches)),
        TransactionKind::Message => Ok(Transaction::message(&request.gid, branches)),
    }
}

async fn ready(State(state): State<ControlState>) -> HttpResponse {
    if state
        .lifecycle
        .as_ref()
        .is_some_and(|lifecycle| !lifecycle.is_ready())
    {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "DTM service is not ready");
    }
    match state.dtm.store().get_transaction("__roze_health__").await {
        Ok(_) => ok_response("ready"),
        Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "DTM store is not ready"),
    }
}

async fn submit_tcc(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<SubmitTransactionRequest>,
) -> HttpResponse {
    submit_transaction(&state, &headers, TransactionKind::Tcc, request).await
}

async fn submit_saga(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Json(request): Json<SubmitTransactionRequest>,
) -> HttpResponse {
    submit_transaction(&state, &headers, TransactionKind::Saga, request).await
}

macro_rules! submit_handler {
    ($name:ident, $kind:expr) => {
        async fn $name(
            State(state): State<ControlState>,
            headers: HeaderMap,
            Json(request): Json<SubmitTransactionRequest>,
        ) -> HttpResponse {
            submit_transaction(&state, &headers, $kind, request).await
        }
    };
}

submit_handler!(submit_workflow, TransactionKind::Workflow);
submit_handler!(submit_message, TransactionKind::Message);
submit_handler!(submit_xa, TransactionKind::Xa);

async fn submit_transaction(
    state: &ControlState,
    headers: &HeaderMap,
    kind: TransactionKind,
    request: SubmitTransactionRequest,
) -> HttpResponse {
    if !authorize(state, headers) {
        return unauthorized_response();
    }
    let gid = request.gid.clone();
    let transaction = match build_transaction(kind, request, &state.branch_url_policy) {
        Ok(transaction) => transaction,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    match state.dtm.submit(transaction).await {
        Ok(transaction) => {
            audit_transition("dtm.transaction.submit", &transaction);
            ok_response(transaction)
        }
        Err(error) => operation_error("dtm.transaction.submit", Some(&gid), &error),
    }
}

macro_rules! transition_handler {
    ($name:ident, $method:ident, $event:literal) => {
        async fn $name(
            State(state): State<ControlState>,
            headers: HeaderMap,
            Path(path): Path<GidPath>,
        ) -> HttpResponse {
            if !authorize(&state, &headers) {
                return unauthorized_response();
            }
            match state.dtm.$method(&path.gid).await {
                Ok(transaction) => {
                    audit_transition($event, &transaction);
                    ok_response(transaction)
                }
                Err(error) => operation_error($event, Some(&path.gid), &error),
            }
        }
    };
}

transition_handler!(prepare_tcc, prepare_tcc, "dtm.tcc.prepare");
transition_handler!(confirm_tcc, confirm_tcc, "dtm.tcc.confirm");
transition_handler!(cancel_tcc, cancel_tcc, "dtm.tcc.cancel");
transition_handler!(start_saga, start_saga, "dtm.saga.start");
transition_handler!(abort_saga, abort_saga, "dtm.saga.abort");
transition_handler!(start_workflow, start_workflow, "dtm.workflow.start");
transition_handler!(abort_workflow, abort_workflow, "dtm.workflow.abort");
transition_handler!(prepare_message, prepare_message, "dtm.message.prepare");
transition_handler!(dispatch_message, dispatch_message, "dtm.message.dispatch");
transition_handler!(abort_message, abort_message, "dtm.message.abort");
transition_handler!(prepare_xa, prepare_xa, "dtm.xa.prepare");
transition_handler!(commit_xa, commit_xa, "dtm.xa.commit");
transition_handler!(rollback_xa, rollback_xa, "dtm.xa.rollback");
transition_handler!(recover_transaction, recover, "dtm.transaction.recover");
transition_handler!(force_stop_transaction, force_stop, "dtm.transaction.force_stop");
transition_handler!(reset_retry_transaction, reset_retry, "dtm.transaction.reset_retry");

async fn get_transaction(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(path): Path<GidPath>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    match state.dtm.store().get_transaction(&path.gid).await {
        Ok(Some(transaction)) => ok_response(transaction),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "transaction not found"),
        Err(error) => operation_error("dtm.transaction.get", Some(&path.gid), &error),
    }
}

async fn list_transactions(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Query(query): Query<TransactionQuery>,
) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    let limit = query.limit.unwrap_or(50);
    if limit == 0 || limit > 200 || query.offset > 1_000_000 {
        return error_response(StatusCode::BAD_REQUEST, "invalid pagination");
    }
    let kind = match query.kind.as_deref().map(parse_kind).transpose() {
        Ok(kind) => kind,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let status = match query.status.as_deref().map(parse_status).transpose() {
        Ok(status) => status,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    match state.dtm.list().await {
        Ok(transactions) => {
            let filtered = transactions
                .into_iter()
                .filter(|transaction| {
                    query
                        .gid
                        .as_deref()
                        .is_none_or(|gid| transaction.gid == gid)
                        && kind.is_none_or(|kind| transaction.kind == kind)
                        && status.is_none_or(|status| transaction.status == status)
                })
                .collect::<Vec<_>>();
            let total = filtered.len();
            let items = filtered
                .into_iter()
                .skip(query.offset)
                .take(limit)
                .collect();
            ok_response(TransactionPage {
                items,
                offset: query.offset,
                limit,
                total,
            })
        }
        Err(error) => operation_error("dtm.transaction.list", None, &error),
    }
}

async fn recover_all(State(state): State<ControlState>, headers: HeaderMap) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    match state.dtm.tick_recover_once().await {
        Ok(recovered) => {
            let count = recovered.len();
            roze_log::audit_info!(
                event = "dtm.recovery.tick",
                actor_kind = "control_token",
                operation = "recover",
                outcome = "success",
                transaction_count = count,
                "DTM recovery tick completed"
            );
            ok_response(RecoveryResult { recovered, count })
        }
        Err(error) => operation_error("dtm.recovery.tick", None, &error),
    }
}

async fn stats(State(state): State<ControlState>, headers: HeaderMap) -> HttpResponse {
    if !authorize(&state, &headers) {
        return unauthorized_response();
    }
    match state.dtm.list().await {
        Ok(transactions) => {
            let mut by_kind = BTreeMap::new();
            let mut by_status = BTreeMap::new();
            for transaction in &transactions {
                *by_kind
                    .entry(kind_name(transaction.kind).to_string())
                    .or_insert(0) += 1;
                *by_status
                    .entry(status_name(transaction.status).to_string())
                    .or_insert(0) += 1;
            }
            ok_response(TransactionStats {
                total: transactions.len(),
                by_kind,
                by_status,
            })
        }
        Err(error) => operation_error("dtm.stats.get", None, &error),
    }
}

fn build_transaction(
    kind: TransactionKind,
    request: SubmitTransactionRequest,
    branch_url_policy: &BranchUrlPolicy,
) -> Result<Transaction, &'static str> {
    let gid = request.gid.trim();
    if gid.is_empty() || gid.len() > 128 {
        return Err("gid must contain between 1 and 128 bytes");
    }
    if request
        .kind
        .is_some_and(|request_kind| request_kind != kind)
    {
        return Err("transaction kind does not match endpoint");
    }
    if request.branches.is_empty() || request.branches.len() > 100 {
        return Err("branches must contain between 1 and 100 items");
    }
    if request.metadata.len() > 32
        || request
            .metadata
            .iter()
            .any(|(key, value)| key.is_empty() || key.len() > 64 || value.len() > 256)
    {
        return Err("metadata exceeds bounded key or value limits");
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut branches = Vec::with_capacity(request.branches.len());
    for branch in request.branches {
        if branch.id.trim().is_empty()
            || branch.id.len() > 128
            || !ids.insert(branch.id.clone())
            || branch_url_policy.validate(&branch.action).is_err()
        {
            return Err("branch id or action URL is invalid");
        }
        let branch = match kind {
            TransactionKind::Tcc => {
                if branch.kind.is_some_and(|kind| kind != BranchKind::TccTry) {
                    return Err("TCC branches must use TccTry kind");
                }
                let confirm = branch
                    .confirm
                    .filter(|url| branch_url_policy.validate(url).is_ok());
                let cancel = branch
                    .cancel
                    .filter(|url| branch_url_policy.validate(url).is_ok());
                if confirm.is_none() || cancel.is_none() {
                    return Err("TCC branches require valid confirm and cancel URLs");
                }
                Branch::tcc_try(
                    branch.id,
                    branch.action,
                    confirm.expect("validated confirm URL"),
                    cancel.expect("validated cancel URL"),
                    branch.payload,
                )
            }
            TransactionKind::Saga => {
                if branch
                    .kind
                    .is_some_and(|kind| kind != BranchKind::SagaAction)
                {
                    return Err("Saga branches must use SagaAction kind");
                }
                let compensate = branch
                    .compensate
                    .filter(|url| branch_url_policy.validate(url).is_ok());
                if compensate.is_none() {
                    return Err("Saga branches require a valid compensate URL");
                }
                Branch::saga(
                    branch.id,
                    branch.action,
                    compensate.expect("validated compensate URL"),
                    branch.payload,
                )
            }
            TransactionKind::Workflow => {
                let compensate = branch
                    .compensate
                    .filter(|url| branch_url_policy.validate(url).is_ok())
                    .ok_or("Workflow branches require a valid compensate URL")?;
                Branch::workflow(
                    branch.id,
                    branch.action,
                    compensate,
                    branch.dependencies,
                    branch.payload,
                )
            }
            TransactionKind::Message => {
                if !branch.dependencies.is_empty() {
                    return Err("Message branches do not support dependencies");
                }
                Branch::message(branch.id, branch.action, branch.payload)
            }
            TransactionKind::Xa => {
                let rollback = branch
                    .cancel
                    .or(branch.compensate)
                    .filter(|url| branch_url_policy.validate(url).is_ok())
                    .ok_or("XA branches require a valid rollback URL")?;
                Branch::xa(branch.id, branch.action, rollback, branch.payload)
            }
        };
        branches.push(branch);
    }
    let mut transaction = Transaction::new(gid, kind, branches);
    transaction.metadata = request.metadata;
    if let Some(timeout) = request.timeout_millis {
        if !(1_000..=86_400_000).contains(&timeout) {
            return Err("timeout_millis must be between 1000 and 86400000");
        }
        transaction.timeout_millis = Some(timeout);
    }
    Ok(transaction)
}

fn authorize(state: &ControlState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.control_token.as_deref() else {
        return true;
    };
    let provided = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    provided.is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
}

fn unauthorized_response() -> HttpResponse {
    error_response(StatusCode::UNAUTHORIZED, "unauthorized")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn parse_kind(value: &str) -> Result<TransactionKind, &'static str> {
    match value.to_ascii_lowercase().as_str() {
        "tcc" => Ok(TransactionKind::Tcc),
        "saga" => Ok(TransactionKind::Saga),
        "workflow" => Ok(TransactionKind::Workflow),
        "message" | "msg" => Ok(TransactionKind::Message),
        "xa" => Ok(TransactionKind::Xa),
        _ => Err("kind must be tcc, saga, workflow, message, or xa"),
    }
}

fn parse_status(value: &str) -> Result<TransactionStatus, &'static str> {
    match value.to_ascii_lowercase().as_str() {
        "submitted" => Ok(TransactionStatus::Submitted),
        "trying" => Ok(TransactionStatus::Trying),
        "prepared" => Ok(TransactionStatus::Prepared),
        "succeeding" => Ok(TransactionStatus::Succeeding),
        "succeeded" => Ok(TransactionStatus::Succeeded),
        "aborting" => Ok(TransactionStatus::Aborting),
        "aborted" => Ok(TransactionStatus::Aborted),
        "failed" => Ok(TransactionStatus::Failed),
        _ => Err("invalid transaction status"),
    }
}

const fn kind_name(kind: TransactionKind) -> &'static str {
    match kind {
        TransactionKind::Tcc => "tcc",
        TransactionKind::Saga => "saga",
        TransactionKind::Workflow => "workflow",
        TransactionKind::Message => "message",
        TransactionKind::Xa => "xa",
    }
}

const fn status_name(status: TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Submitted => "submitted",
        TransactionStatus::Trying => "trying",
        TransactionStatus::Prepared => "prepared",
        TransactionStatus::Succeeding => "succeeding",
        TransactionStatus::Succeeded => "succeeded",
        TransactionStatus::Aborting => "aborting",
        TransactionStatus::Aborted => "aborted",
        TransactionStatus::Failed => "failed",
    }
}

fn audit_transition(event: &'static str, transaction: &Transaction) {
    if transaction.branches.iter().any(|branch| {
        matches!(
            branch.status,
            BranchStatus::Failed | BranchStatus::Compensating
        )
    }) {
        roze_log::audit_warn!(
            event = event,
            actor_kind = "control_token",
            resource_type = "distributed_transaction",
            resource_id = transaction.gid.as_str(),
            operation = event,
            outcome = "pending_retry",
            transaction_status = status_name(transaction.status),
            "DTM control operation requires retry"
        );
    } else {
        roze_log::audit_info!(
            event = event,
            actor_kind = "control_token",
            resource_type = "distributed_transaction",
            resource_id = transaction.gid.as_str(),
            operation = event,
            outcome = "success",
            transaction_status = status_name(transaction.status),
            "DTM control operation completed"
        );
    }
}

fn operation_error(
    operation: &'static str,
    gid: Option<&str>,
    error: &anyhow::Error,
) -> HttpResponse {
    let message = error.to_string();
    let (status, public_message) = if message.starts_with("transaction not found:") {
        (StatusCode::NOT_FOUND, "transaction not found")
    } else if message.contains("expected") || message.contains("non-replayable state") {
        (StatusCode::CONFLICT, "transaction state conflict")
    } else {
        (StatusCode::BAD_GATEWAY, "DTM operation failed")
    };
    tracing::error!(
        event = "dtm.control.failed",
        operation,
        error_kind = "dtm_operation_failed",
        status = status.as_u16(),
        "DTM control operation failed"
    );
    roze_log::audit_warn!(
        event = operation,
        actor_kind = "control_token",
        resource_type = "distributed_transaction",
        resource_id = gid.unwrap_or("multiple"),
        operation,
        outcome = "failed",
        error_kind = "dtm_operation_failed",
        "DTM control operation failed"
    );
    error_response(status, public_message)
}

fn ok_response<T: Serialize>(data: T) -> HttpResponse {
    rest::json_response(StatusCode::OK, &roze_result::ApiResponse::ok(data))
}

fn error_response(status: StatusCode, message: impl Into<String>) -> HttpResponse {
    rest::json_response(
        status,
        &roze_result::ApiResponse::<serde_json::Value>::error(status.as_u16() as i32, message),
    )
}

const fn default_max_attempts() -> u32 {
    5
}
const fn default_retry_backoff_ms() -> u64 {
    1_000
}
const fn default_max_retry_backoff_ms() -> u64 {
    30_000
}
const fn default_branch_call_timeout_ms() -> u64 {
    5_000
}
const fn default_transaction_timeout_ms() -> u64 {
    60_000
}
const fn default_recover_interval_ms() -> u64 {
    1_000
}
const fn default_recovery_lease_ttl_ms() -> u64 {
    5_000
}
fn default_worker_id() -> String {
    "roze-dtm-local".to_string()
}

const fn default_store_max_connections() -> u32 {
    10
}

fn store_url(config: &StoreConfig) -> anyhow::Result<&str> {
    config
        .database_url
        .as_deref()
        .context("validated DTM database URL missing")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt as _;

    #[test]
    fn production_rejects_memory_store() {
        assert!(DtmConfig::default().validate(true).is_err());
    }

    #[test]
    fn sqlite_requires_database_url() {
        let mut config = DtmConfig::default();
        config.store.kind = StoreKind::Sqlite;
        assert!(config.validate(false).is_err());
        config.store.database_url = Some("sqlite://roze-dtm.db?mode=rwc".to_string());
        config.control_token = Some("x".repeat(32));
        config.worker_id = "dtm-test-1".to_string();
        assert!(config.validate(true).is_err());
        config.allowed_branch_origins = vec!["http://inventory".to_string()];
        config.validate(true).expect("valid production config");
    }

    #[test]
    fn production_database_kinds_validate_url_scheme_and_pool_size() {
        let mut config = DtmConfig {
            allowed_branch_origins: vec!["http://inventory".to_string()],
            ..DtmConfig::default()
        };

        config.store.kind = StoreKind::Postgres;
        config.store.database_url = Some("postgres://dtm:secret@db/roze_dtm".to_string());
        config.validate(false).expect("valid postgres config");

        config.store.kind = StoreKind::Mysql;
        config.store.database_url = Some("mysql://dtm:secret@db/roze_dtm".to_string());
        config.validate(false).expect("valid mysql config");

        config.store.database_url = Some("sqlite://roze-dtm.db".to_string());
        assert!(config.validate(false).is_err());

        config.store.database_url = Some("mysql://dtm:secret@db/roze_dtm".to_string());
        config.store.max_connections = 0;
        assert!(config.validate(false).is_err());
    }

    #[test]
    fn checked_in_development_config_is_typed_and_valid() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.yaml");
        let config = roze_config::load_service_with_application::<ApplicationConfig>(&path)
            .expect("load checked-in DTM config");
        assert_eq!(config.profile, roze_config::ServiceProfile::Development);
        config
            .application
            .dtm
            .validate(false)
            .expect("validate DTM config");
    }

    #[test]
    fn production_requires_strong_control_token() {
        let mut config = DtmConfig::default();
        config.store.kind = StoreKind::Sqlite;
        config.store.database_url = Some("sqlite://roze-dtm.db?mode=rwc".to_string());
        assert!(config.validate(true).is_err());
        config.control_token = Some("short".to_string());
        assert!(config.validate(true).is_err());
        config.control_token = Some("x".repeat(32));
        config.worker_id = "dtm-test-1".to_string();
        assert!(config.validate(true).is_err());
        config.allowed_branch_origins = vec!["http://inventory".to_string()];
        config.validate(true).expect("valid production config");
    }

    #[test]
    fn transaction_submission_is_bounded_and_typed() {
        let request = SubmitTransactionRequest {
            gid: "order-1001".to_string(),
            kind: None,
            branches: vec![BranchRequest {
                id: "inventory".to_string(),
                kind: None,
                action: "http://inventory/try".to_string(),
                compensate: None,
                confirm: Some("http://inventory/confirm".to_string()),
                cancel: Some("http://inventory/cancel".to_string()),
                payload: serde_json::json!({"sku": "A", "count": 1}),
                dependencies: Vec::new(),
            }],
            timeout_millis: Some(30_000),
            metadata: BTreeMap::new(),
        };
        let policy = BranchUrlPolicy::from_allowed_origins(["http://inventory"]).expect("policy");
        let transaction =
            build_transaction(TransactionKind::Tcc, request, &policy).expect("transaction");
        assert_eq!(transaction.kind, TransactionKind::Tcc);
        assert_eq!(transaction.branches.len(), 1);
        assert_eq!(transaction.timeout_millis, Some(30_000));

        let request = SubmitTransactionRequest {
            gid: "order-1002".to_string(),
            kind: None,
            branches: Vec::new(),
            timeout_millis: None,
            metadata: BTreeMap::new(),
        };
        assert!(build_transaction(TransactionKind::Tcc, request, &policy).is_err());

        let request = SubmitTransactionRequest {
            gid: "order-1003".to_string(),
            kind: None,
            branches: vec![BranchRequest {
                id: "metadata".to_string(),
                kind: None,
                action: "http://169.254.169.254/latest".to_string(),
                compensate: None,
                confirm: Some("http://169.254.169.254/confirm".to_string()),
                cancel: Some("http://169.254.169.254/cancel".to_string()),
                payload: serde_json::json!({}),
                dependencies: Vec::new(),
            }],
            timeout_millis: None,
            metadata: BTreeMap::new(),
        };
        assert!(build_transaction(TransactionKind::Tcc, request, &policy).is_err());
    }

    #[test]
    fn token_comparison_checks_content_and_length() {
        assert!(constant_time_eq(b"same-token", b"same-token"));
        assert!(!constant_time_eq(b"same-token", b"other-token"));
        assert!(!constant_time_eq(b"same-token", b"same-token-longer"));
    }

    #[tokio::test]
    async fn control_routes_require_token_but_health_is_public() {
        let token = "control-token-with-at-least-32-bytes";
        let router = test_router(Some(token));
        let health = router
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/healthz")
                    .body(rest::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let unauthorized = router
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri("/v1/stats")
                    .body(rest::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = router
            .oneshot(
                http::Request::builder()
                    .uri("/v1/stats")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(rest::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn native_routes_submit_and_query_transaction() {
        let token = "control-token-with-at-least-32-bytes";
        let router = test_router(Some(token));
        let request = serde_json::json!({
            "gid": "order-2001",
            "branches": [{
                "id": "inventory",
                "kind": "TccTry",
                "action": "http://inventory/try",
                "confirm": "http://inventory/confirm",
                "cancel": "http://inventory/cancel",
                "payload": {"sku": "A", "count": 1}
            }]
        });
        let submitted = router
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/v1/tcc")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(rest::full_body(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submitted.status(), StatusCode::OK);

        let fetched = router
            .oneshot(
                http::Request::builder()
                    .uri("/v1/transactions/order-2001")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(rest::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
    }

    fn test_router(token: Option<&str>) -> Router {
        let store: Arc<dyn TransactionStore> = Arc::new(InMemoryTransactionStore::new());
        let branch_url_policy =
            BranchUrlPolicy::from_allowed_origins(["http://inventory"]).expect("policy");
        let dtm = Arc::new(Dtm::with_options(
            store,
            HttpBranchInvoker::with_timeout_and_policy(
                Duration::from_secs(5),
                branch_url_policy.clone(),
            )
            .expect("invoker"),
            DtmOptions::default(),
        ));
        control_router(ControlState {
            dtm,
            branch_url_policy,
            control_token: token.map(Arc::<str>::from),
            lifecycle: None,
        })
    }
}
