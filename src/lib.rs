use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::{MySqlPool, MySqlRow},
    postgres::{PgPool, PgRow},
    sqlite::SqliteRow,
    Row, Sqlite, SqlitePool,
};
use tokio::sync::RwLock;

pub mod client;
pub mod grpc_client;
pub mod kv;
pub mod pb;

pub use kv::{KvEntry, Topic, TopicSubscriber, TOPICS_CATEGORY};

/// Reliable event delivery primitives used by DTM two-phase messages and outbox flows.
///
/// These are provided by Roze so DTM and generated services share one lease,
/// retry, idempotency, inbox, and publisher contract.
pub mod outbox {
    pub use roze_transaction::{
        relay_outbox_batch, InMemoryOutbox, InboxDeduper, InboxMessage, InboxStatus, OutboxMessage,
        OutboxRelayConfig, OutboxRelayReport, OutboxStatus, OutboxStore, TransactionalOutbox,
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionKind {
    Saga,
    Workflow,
    Message,
    Xa,
    #[default]
    Tcc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    Submitted,
    Trying,
    Prepared,
    Succeeding,
    Succeeded,
    Aborting,
    Aborted,
    Failed,
}

impl TransactionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Aborted | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchKind {
    SagaAction,
    SagaCompensate,
    TccTry,
    TccConfirm,
    TccCancel,
    WorkflowAction,
    MessageAction,
    XaAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchStatus {
    Pending,
    Running,
    Compensating,
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branch {
    pub id: String,
    pub kind: BranchKind,
    pub action: String,
    pub compensate: Option<String>,
    #[serde(default)]
    pub confirm: Option<String>,
    #[serde(default)]
    pub cancel: Option<String>,
    pub payload: serde_json::Value,
    pub status: BranchStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    #[serde(default)]
    pub next_retry_millis: Option<u64>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowProgressStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowProgress {
    pub branch_id: String,
    pub operation: String,
    pub status: WorkflowProgressStatus,
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
}

impl WorkflowProgress {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.branch_id.is_empty() && self.branch_id.len() <= 128,
            "workflow progress branch id must contain 1 to 128 bytes"
        );
        anyhow::ensure!(
            !self.operation.is_empty() && self.operation.len() <= 64,
            "workflow progress operation must contain 1 to 64 bytes"
        );
        anyhow::ensure!(
            self.data.len() <= 2 * 1024 * 1024,
            "workflow progress data exceeds 2 MiB"
        );
        Ok(())
    }
}

mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde::{de::Error as _, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        STANDARD.decode(value).map_err(D::Error::custom)
    }
}

impl Branch {
    pub fn saga(
        id: impl Into<String>,
        action: impl Into<String>,
        compensate: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: id.into(),
            kind: BranchKind::SagaAction,
            action: action.into(),
            compensate: Some(compensate.into()),
            confirm: None,
            cancel: None,
            payload,
            status: BranchStatus::Pending,
            attempts: 0,
            last_error: None,
            next_retry_millis: None,
            dependencies: Vec::new(),
        }
    }

    pub fn tcc_try(
        id: impl Into<String>,
        try_action: impl Into<String>,
        confirm_action: impl Into<String>,
        cancel_action: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: id.into(),
            kind: BranchKind::TccTry,
            action: try_action.into(),
            compensate: None,
            confirm: Some(confirm_action.into()),
            cancel: Some(cancel_action.into()),
            payload,
            status: BranchStatus::Pending,
            attempts: 0,
            last_error: None,
            next_retry_millis: None,
            dependencies: Vec::new(),
        }
    }

    pub fn message(
        id: impl Into<String>,
        action: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        let mut branch = Self::saga(id, action, "", payload);
        branch.kind = BranchKind::MessageAction;
        branch.compensate = None;
        branch
    }

    pub fn xa(
        id: impl Into<String>,
        commit: impl Into<String>,
        rollback: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        let mut branch = Self::tcc_try(id, "", commit, rollback, payload);
        branch.kind = BranchKind::XaAction;
        branch.action.clear();
        branch
    }

    pub fn workflow(
        id: impl Into<String>,
        action: impl Into<String>,
        compensate: impl Into<String>,
        dependencies: Vec<String>,
        payload: serde_json::Value,
    ) -> Self {
        let mut branch = Self::saga(id, action, compensate, payload);
        branch.kind = BranchKind::WorkflowAction;
        branch.dependencies = dependencies;
        branch
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub gid: String,
    pub kind: TransactionKind,
    pub status: TransactionStatus,
    pub branches: Vec<Branch>,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub timeout_millis: Option<u64>,
    #[serde(default)]
    pub options: TransactionOptions,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflow_progresses: Vec<WorkflowProgress>,
    pub metadata: BTreeMap<String, String>,
}

impl Transaction {
    pub fn default_tcc(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::tcc(gid, branches)
    }

    pub fn saga(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::new(gid, TransactionKind::Saga, branches)
    }

    pub fn tcc(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::new(gid, TransactionKind::Tcc, branches)
    }

    pub fn workflow(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::new(gid, TransactionKind::Workflow, branches)
    }

    pub fn message(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::new(gid, TransactionKind::Message, branches)
    }

    pub fn xa(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        Self::new(gid, TransactionKind::Xa, branches)
    }

    pub fn new(gid: impl Into<String>, kind: TransactionKind, branches: Vec<Branch>) -> Self {
        let now = current_millis();
        Self {
            gid: gid.into(),
            kind,
            status: TransactionStatus::Submitted,
            branches,
            created_at_millis: now,
            updated_at_millis: now,
            timeout_millis: None,
            options: TransactionOptions::default(),
            workflow_progresses: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionOptions {
    #[serde(default)]
    pub wait_result: bool,
    #[serde(default)]
    pub retry_interval_millis: Option<u64>,
    #[serde(default)]
    pub request_timeout_millis: Option<u64>,
    #[serde(default)]
    pub retry_limit: Option<u32>,
    #[serde(default)]
    pub branch_headers: BTreeMap<String, String>,
}

impl TransactionOptions {
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(value) = self.retry_interval_millis {
            anyhow::ensure!(
                (1..=86_400_000).contains(&value),
                "retry interval must be between 1 and 86400000 milliseconds"
            );
        }
        if let Some(value) = self.request_timeout_millis {
            anyhow::ensure!(
                (1..=86_400_000).contains(&value),
                "request timeout must be between 1 and 86400000 milliseconds"
            );
        }
        if let Some(value) = self.retry_limit {
            anyhow::ensure!(value <= 10_000, "retry limit must not exceed 10000");
        }
        anyhow::ensure!(
            self.branch_headers.len() <= 32
                && self.branch_headers.iter().all(|(name, value)| {
                    !name.is_empty()
                        && name.len() <= 64
                        && value.len() <= 1_024
                        && reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_ok()
                        && reqwest::header::HeaderValue::try_from(value.as_str()).is_ok()
                }),
            "branch headers exceed bounded name, value, or count limits"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtmOptions {
    pub max_attempts: u32,
    pub retry_backoff_millis: u64,
    pub max_retry_backoff_millis: u64,
    pub branch_call_timeout_millis: u64,
    pub transaction_timeout_millis: u64,
}

impl Default for DtmOptions {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            retry_backoff_millis: 1_000,
            max_retry_backoff_millis: 30_000,
            branch_call_timeout_millis: 5_000,
            transaction_timeout_millis: 60_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchBarrier {
    pub gid: String,
    pub branch_id: String,
    pub op: String,
}

impl BranchBarrier {
    pub fn new(
        gid: impl Into<String>,
        branch_id: impl Into<String>,
        op: impl Into<String>,
    ) -> Self {
        Self {
            gid: gid.into(),
            branch_id: branch_id.into(),
            op: op.into(),
        }
    }

    fn key(&self) -> String {
        format!("{}:{}:{}", self.gid, self.branch_id, self.op)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierDecision {
    Execute,
    SkipDuplicate,
    SkipNullCompensation,
    SkipCancelledTry,
}

#[async_trait]
pub trait TransactionStore: Send + Sync + 'static {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()>;
    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>>;
    async fn update_transaction(&self, tx: Transaction) -> anyhow::Result<()>;
    /// Atomically validates and appends a dynamic branch.
    ///
    /// Implementations must serialize concurrent registrations for the same
    /// transaction so one writer cannot overwrite a branch added by another.
    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction>;
    async fn record_workflow_progress(
        &self,
        _gid: &str,
        _progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        anyhow::bail!("workflow progress persistence is not supported by this store")
    }
    async fn finish_workflow(
        &self,
        _gid: &str,
        _status: TransactionStatus,
        _rollback_reason: Option<String>,
        _result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        anyhow::bail!("workflow completion persistence is not supported by this store")
    }
    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>>;
    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>>;
    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>>;
    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool>;
    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool>;
    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool>;
    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision>;
    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()>;
    async fn try_acquire_recovery_lease(
        &self,
        _name: &str,
        _owner: &str,
        _ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl<T> TransactionStore for Arc<T>
where
    T: TransactionStore + ?Sized,
{
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        (**self).insert_transaction(tx).await
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        (**self).get_transaction(gid).await
    }

    async fn update_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        (**self).update_transaction(tx).await
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        (**self).register_branch(gid, branch).await
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        (**self).record_workflow_progress(gid, progress).await
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        (**self)
            .finish_workflow(gid, status, rollback_reason, result)
            .await
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        (**self).list_transactions().await
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        (**self).get_kv(category, key).await
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        (**self).list_kv(category, key, offset, limit).await
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        (**self).create_kv(entry).await
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        (**self).update_kv(entry, expected_version).await
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        (**self).delete_kv(category, key).await
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        (**self).barrier(barrier).await
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        (**self).release_barrier(barrier).await
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        (**self)
            .try_acquire_recovery_lease(name, owner, ttl_millis)
            .await
    }
}

#[async_trait]
pub trait BranchInvoker: Clone + Send + Sync + 'static {
    async fn invoke(&self, url: &str, payload: &serde_json::Value) -> anyhow::Result<()>;

    async fn invoke_with_options(
        &self,
        url: &str,
        payload: &serde_json::Value,
        _options: &TransactionOptions,
    ) -> anyhow::Result<()> {
        self.invoke(url, payload).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopBranchInvoker;

#[async_trait]
impl BranchInvoker for NoopBranchInvoker {
    async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HttpBranchInvoker {
    client: reqwest::Client,
    url_policy: BranchUrlPolicy,
}

#[derive(Debug, Clone, Default)]
pub struct BranchUrlPolicy {
    allowed_origins: Option<Arc<BTreeSet<String>>>,
}

impl BranchUrlPolicy {
    pub fn allow_all() -> Self {
        Self::default()
    }

    pub fn from_allowed_origins(
        origins: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> anyhow::Result<Self> {
        let mut allowed_origins = BTreeSet::new();
        for origin in origins {
            let url = parse_branch_url(origin.as_ref())?;
            anyhow::ensure!(
                url.path() == "/" && url.query().is_none() && url.fragment().is_none(),
                "branch origin must not contain a path, query, or fragment"
            );
            allowed_origins.insert(url.origin().ascii_serialization());
        }
        Ok(Self {
            allowed_origins: Some(Arc::new(allowed_origins)),
        })
    }

    pub fn validate(&self, value: &str) -> anyhow::Result<()> {
        let url = parse_branch_url(value)?;
        if let Some(allowed_origins) = &self.allowed_origins {
            anyhow::ensure!(
                allowed_origins.contains(&url.origin().ascii_serialization()),
                "branch URL origin is not allowed"
            );
        }
        Ok(())
    }
}

impl HttpBranchInvoker {
    pub fn new() -> Self {
        Self {
            client: branch_http_client(None).expect("default HTTP client configuration is valid"),
            url_policy: BranchUrlPolicy::allow_all(),
        }
    }

    pub fn with_timeout(timeout: Duration) -> anyhow::Result<Self> {
        Self::with_timeout_and_policy(timeout, BranchUrlPolicy::allow_all())
    }

    pub fn with_timeout_and_policy(
        timeout: Duration,
        url_policy: BranchUrlPolicy,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: branch_http_client(Some(timeout))?,
            url_policy,
        })
    }
}

impl Default for HttpBranchInvoker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BranchInvoker for HttpBranchInvoker {
    async fn invoke(&self, url: &str, payload: &serde_json::Value) -> anyhow::Result<()> {
        self.invoke_with_options(url, payload, &TransactionOptions::default())
            .await
    }

    async fn invoke_with_options(
        &self,
        url: &str,
        payload: &serde_json::Value,
        options: &TransactionOptions,
    ) -> anyhow::Result<()> {
        self.url_policy.validate(url)?;
        options.validate()?;
        let mut request = self.client.post(url).json(payload);
        if let Some(timeout) = options.request_timeout_millis {
            request = request.timeout(Duration::from_millis(timeout));
        }
        for (name, value) in &options.branch_headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let response = request.send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("branch call {url} failed with status {}", response.status())
        }
    }
}

fn branch_http_client(timeout: Option<Duration>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    Ok(builder.build()?)
}

fn parse_branch_url(value: &str) -> anyhow::Result<reqwest::Url> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 2_048,
        "invalid branch URL"
    );
    let url = reqwest::Url::parse(value).map_err(|_| anyhow::anyhow!("invalid branch URL"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "invalid branch URL"
    );
    Ok(url)
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryTransactionStore {
    txs: Arc<RwLock<BTreeMap<String, Transaction>>>,
    kv: Arc<RwLock<BTreeMap<(String, String), KvEntry>>>,
    barriers: Arc<RwLock<BTreeSet<String>>>,
    leases: Arc<RwLock<BTreeMap<String, RecoveryLease>>>,
}

impl InMemoryTransactionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone)]
struct RecoveryLease {
    owner: String,
    expires_at_millis: u64,
}

#[async_trait]
impl TransactionStore for InMemoryTransactionStore {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        let mut txs = self.txs.write().await;
        if txs.contains_key(&tx.gid) {
            anyhow::bail!("transaction already exists: {}", tx.gid);
        }
        txs.insert(tx.gid.clone(), tx);
        Ok(())
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        Ok(self.txs.read().await.get(gid).cloned())
    }

    async fn update_transaction(&self, mut tx: Transaction) -> anyhow::Result<()> {
        tx.updated_at_millis = current_millis();
        self.txs.write().await.insert(tx.gid.clone(), tx);
        Ok(())
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        let mut txs = self.txs.write().await;
        let tx = txs
            .get_mut(gid)
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        append_dynamic_branch(tx, branch)?;
        Ok(tx.clone())
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        let mut txs = self.txs.write().await;
        let tx = txs
            .get_mut(gid)
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        append_workflow_progress(tx, progress)?;
        Ok(tx.clone())
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        let mut txs = self.txs.write().await;
        let tx = txs
            .get_mut(gid)
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        apply_workflow_completion(tx, status, rollback_reason, result)?;
        Ok(tx.clone())
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        Ok(self.txs.read().await.values().cloned().collect())
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        Ok(self
            .kv
            .read()
            .await
            .get(&(category.to_owned(), key.to_owned()))
            .cloned())
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        Ok(self
            .kv
            .read()
            .await
            .values()
            .filter(|entry| category.is_none_or(|category| entry.category == category))
            .filter(|entry| key.is_none_or(|key| entry.key == key))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        let mut kv = self.kv.write().await;
        let entry_key = (entry.category.clone(), entry.key.clone());
        if kv.contains_key(&entry_key) {
            return Ok(false);
        }
        kv.insert(entry_key, entry);
        Ok(true)
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        let mut kv = self.kv.write().await;
        let entry_key = (entry.category.clone(), entry.key.clone());
        let Some(existing) = kv.get(&entry_key) else {
            return Ok(false);
        };
        if existing.version != expected_version
            || entry.version != expected_version.saturating_add(1)
        {
            return Ok(false);
        }
        kv.insert(entry_key, entry);
        Ok(true)
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        Ok(self
            .kv
            .write()
            .await
            .remove(&(category.to_owned(), key.to_owned()))
            .is_some())
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        let mut barriers = self.barriers.write().await;
        let key = barrier.key();
        if barriers.contains(&key) {
            return Ok(BarrierDecision::SkipDuplicate);
        }

        let cancel_key = format!("{}:{}:cancel", barrier.gid, barrier.branch_id);
        let try_key = format!("{}:{}:try", barrier.gid, barrier.branch_id);
        if barrier.op == "try" && barriers.contains(&cancel_key) {
            return Ok(BarrierDecision::SkipCancelledTry);
        }
        if barrier.op == "cancel" && !barriers.contains(&try_key) {
            barriers.insert(cancel_key);
            return Ok(BarrierDecision::SkipNullCompensation);
        }

        barriers.insert(key);
        Ok(BarrierDecision::Execute)
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        self.barriers.write().await.remove(&barrier.key());
        Ok(())
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        let now = current_millis();
        let mut leases = self.leases.write().await;
        if let Some(lease) = leases.get(name) {
            if lease.owner != owner && lease.expires_at_millis > now {
                return Ok(false);
            }
        }

        leases.insert(
            name.to_owned(),
            RecoveryLease {
                owner: owner.to_owned(),
                expires_at_millis: now.saturating_add(ttl_millis),
            },
        );
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct SqliteTransactionStore {
    pool: SqlitePool,
}

impl SqliteTransactionStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_transactions (
                gid TEXT PRIMARY KEY NOT NULL,
                payload TEXT NOT NULL,
                updated_at_millis INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_barriers (
                barrier_key TEXT PRIMARY KEY NOT NULL,
                gid TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                op TEXT NOT NULL,
                created_at_millis INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_recovery_leases (
                name TEXT PRIMARY KEY NOT NULL,
                owner TEXT NOT NULL,
                expires_at_millis INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_kv (
                category TEXT NOT NULL,
                entry_key TEXT NOT NULL,
                entry_value TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at_millis INTEGER NOT NULL,
                updated_at_millis INTEGER NOT NULL,
                PRIMARY KEY (category, entry_key)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TransactionStore for SqliteTransactionStore {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&tx)?;
        let changed = sqlx::query(
            r#"
            INSERT OR IGNORE INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if changed == 0 {
            anyhow::bail!("transaction already exists: {}", tx.gid);
        }
        Ok(())
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .transpose()
    }

    async fn update_transaction(&self, mut tx: Transaction) -> anyhow::Result<()> {
        tx.updated_at_millis = current_millis();
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            r#"
            INSERT INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES (?, ?, ?)
            ON CONFLICT(gid) DO UPDATE SET
                payload = excluded.payload,
                updated_at_millis = excluded.updated_at_millis
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        for _ in 0..16 {
            let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
                .bind(gid)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let previous_payload = row.get::<&str, _>("payload").to_owned();
            let mut tx: Transaction = serde_json::from_str(&previous_payload)?;
            append_dynamic_branch(&mut tx, branch.clone())?;
            let payload = serde_json::to_string(&tx)?;
            let changed = sqlx::query(
                "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? \
                 WHERE gid = ? AND payload = ?",
            )
            .bind(payload)
            .bind(tx.updated_at_millis as i64)
            .bind(gid)
            .bind(previous_payload)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if changed == 1 {
                return Ok(tx);
            }
        }
        anyhow::bail!("transaction {gid} branch registration is contended")
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        for _ in 0..16 {
            let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
                .bind(gid)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let previous_payload = row.get::<&str, _>("payload").to_owned();
            let mut tx: Transaction = serde_json::from_str(&previous_payload)?;
            append_workflow_progress(&mut tx, progress.clone())?;
            let payload = serde_json::to_string(&tx)?;
            let changed = sqlx::query(
                "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? \
                 WHERE gid = ? AND payload = ?",
            )
            .bind(payload)
            .bind(tx.updated_at_millis as i64)
            .bind(gid)
            .bind(previous_payload)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if changed == 1 {
                return Ok(tx);
            }
        }
        anyhow::bail!("transaction {gid} workflow progress update is contended")
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        for _ in 0..16 {
            let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
                .bind(gid)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let previous_payload = row.get::<&str, _>("payload").to_owned();
            let mut tx: Transaction = serde_json::from_str(&previous_payload)?;
            apply_workflow_completion(
                &mut tx,
                status,
                rollback_reason.clone(),
                result.clone(),
            )?;
            let payload = serde_json::to_string(&tx)?;
            let changed = sqlx::query(
                "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? \
                 WHERE gid = ? AND payload = ?",
            )
            .bind(payload)
            .bind(tx.updated_at_millis as i64)
            .bind(gid)
            .bind(previous_payload)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if changed == 1 {
                return Ok(tx);
            }
        }
        anyhow::bail!("transaction {gid} workflow completion is contended")
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let rows =
            sqlx::query("SELECT payload FROM roze_dtm_transactions ORDER BY updated_at_millis ASC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .collect()
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        let row = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv WHERE category = ? AND entry_key = ?",
        )
        .bind(category)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| sqlite_kv_entry(&row)).transpose()
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let rows = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv \
             WHERE (? IS NULL OR category = ?) AND (? IS NULL OR entry_key = ?) \
             ORDER BY category, entry_key LIMIT ? OFFSET ?",
        )
        .bind(category)
        .bind(category)
        .bind(key)
        .bind(key)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(sqlite_kv_entry).collect()
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        let changed = sqlx::query(
            "INSERT OR IGNORE INTO roze_dtm_kv \
             (category, entry_key, entry_value, version, created_at_millis, updated_at_millis) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.category)
        .bind(entry.key)
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.created_at_millis as i64)
        .bind(entry.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        anyhow::ensure!(
            entry.version == expected_version.saturating_add(1),
            "invalid KV version transition"
        );
        let changed = sqlx::query(
            "UPDATE roze_dtm_kv SET entry_value = ?, version = ?, updated_at_millis = ? \
             WHERE category = ? AND entry_key = ? AND version = ?",
        )
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.updated_at_millis as i64)
        .bind(entry.category)
        .bind(entry.key)
        .bind(expected_version as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM roze_dtm_kv WHERE category = ? AND entry_key = ?")
            .bind(category)
            .bind(key)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        let key = barrier.key();
        let mut transaction = self.pool.begin().await?;
        if !insert_barrier(&mut transaction, &key, &barrier).await? {
            transaction.commit().await?;
            return Ok(BarrierDecision::SkipDuplicate);
        }

        if barrier.op == "try" {
            let cancel_key = format!("{}:{}:cancel", barrier.gid, barrier.branch_id);
            let cancelled: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&cancel_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if cancelled.is_some() {
                sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&key)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipCancelledTry);
            }
        }

        if barrier.op == "cancel" {
            let try_key = format!("{}:{}:try", barrier.gid, barrier.branch_id);
            let tried: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&try_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if tried.is_none() {
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipNullCompensation);
            }
        }

        transaction.commit().await?;
        Ok(BarrierDecision::Execute)
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = ?")
            .bind(barrier.key())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        let now = current_millis();
        let expires_at = now.saturating_add(ttl_millis);
        let renewed = sqlx::query(
            r#"
            UPDATE roze_dtm_recovery_leases
            SET owner = ?, expires_at_millis = ?
            WHERE name = ? AND (owner = ? OR expires_at_millis <= ?)
            "#,
        )
        .bind(owner)
        .bind(expires_at as i64)
        .bind(name)
        .bind(owner)
        .bind(now as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if renewed == 1 {
            return Ok(true);
        }

        let inserted = sqlx::query(
            r#"
            INSERT OR IGNORE INTO roze_dtm_recovery_leases (name, owner, expires_at_millis)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(name)
        .bind(owner)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(inserted == 1)
    }
}

#[derive(Debug, Clone)]
pub struct PostgresTransactionStore {
    pool: PgPool,
}

impl PostgresTransactionStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_transactions (
                gid VARCHAR(128) PRIMARY KEY NOT NULL,
                payload TEXT NOT NULL,
                updated_at_millis BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_barriers (
                barrier_key VARCHAR(512) PRIMARY KEY NOT NULL,
                gid VARCHAR(128) NOT NULL,
                branch_id VARCHAR(128) NOT NULL,
                op VARCHAR(32) NOT NULL,
                created_at_millis BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_recovery_leases (
                name VARCHAR(191) PRIMARY KEY NOT NULL,
                owner VARCHAR(128) NOT NULL,
                expires_at_millis BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_kv (
                category VARCHAR(191) NOT NULL,
                entry_key VARCHAR(191) NOT NULL,
                entry_value TEXT NOT NULL,
                version BIGINT NOT NULL,
                created_at_millis BIGINT NOT NULL,
                updated_at_millis BIGINT NOT NULL,
                PRIMARY KEY (category, entry_key)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TransactionStore for PostgresTransactionStore {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&tx)?;
        let changed = sqlx::query(
            r#"
            INSERT INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES ($1, $2, $3)
            ON CONFLICT(gid) DO NOTHING
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            anyhow::bail!("transaction already exists: {}", tx.gid);
        }
        Ok(())
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = $1")
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .transpose()
    }

    async fn update_transaction(&self, mut tx: Transaction) -> anyhow::Result<()> {
        tx.updated_at_millis = current_millis();
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            r#"
            INSERT INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES ($1, $2, $3)
            ON CONFLICT(gid) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at_millis = EXCLUDED.updated_at_millis
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        append_dynamic_branch(&mut tx, branch)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = $1, updated_at_millis = $2 WHERE gid = $3",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        append_workflow_progress(&mut tx, progress)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = $1, updated_at_millis = $2 WHERE gid = $3",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        apply_workflow_completion(&mut tx, status, rollback_reason, result)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = $1, updated_at_millis = $2 WHERE gid = $3",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let rows =
            sqlx::query("SELECT payload FROM roze_dtm_transactions ORDER BY updated_at_millis ASC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .collect()
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        let row = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv WHERE category = $1 AND entry_key = $2",
        )
        .bind(category)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| postgres_kv_entry(&row)).transpose()
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let rows = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv \
             WHERE ($1::text IS NULL OR category = $1) AND ($2::text IS NULL OR entry_key = $2) \
             ORDER BY category, entry_key LIMIT $3 OFFSET $4",
        )
        .bind(category)
        .bind(key)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(postgres_kv_entry).collect()
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        let changed = sqlx::query(
            "INSERT INTO roze_dtm_kv \
             (category, entry_key, entry_value, version, created_at_millis, updated_at_millis) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT(category, entry_key) DO NOTHING",
        )
        .bind(entry.category)
        .bind(entry.key)
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.created_at_millis as i64)
        .bind(entry.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        anyhow::ensure!(
            entry.version == expected_version.saturating_add(1),
            "invalid KV version transition"
        );
        let changed = sqlx::query(
            "UPDATE roze_dtm_kv SET entry_value = $1, version = $2, updated_at_millis = $3 \
             WHERE category = $4 AND entry_key = $5 AND version = $6",
        )
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.updated_at_millis as i64)
        .bind(entry.category)
        .bind(entry.key)
        .bind(expected_version as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM roze_dtm_kv WHERE category = $1 AND entry_key = $2")
            .bind(category)
            .bind(key)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        let key = barrier.key();
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO roze_dtm_barriers
                (barrier_key, gid, branch_id, op, created_at_millis)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(barrier_key) DO NOTHING
            "#,
        )
        .bind(&key)
        .bind(&barrier.gid)
        .bind(&barrier.branch_id)
        .bind(&barrier.op)
        .bind(current_millis() as i64)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 0 {
            transaction.commit().await?;
            return Ok(BarrierDecision::SkipDuplicate);
        }
        if barrier.op == "try" {
            let cancel_key = format!("{}:{}:cancel", barrier.gid, barrier.branch_id);
            let cancelled: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = $1")
                    .bind(&cancel_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if cancelled.is_some() {
                sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = $1")
                    .bind(&key)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipCancelledTry);
            }
        }
        if barrier.op == "cancel" {
            let try_key = format!("{}:{}:try", barrier.gid, barrier.branch_id);
            let tried: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = $1")
                    .bind(&try_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if tried.is_none() {
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipNullCompensation);
            }
        }
        transaction.commit().await?;
        Ok(BarrierDecision::Execute)
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = $1")
            .bind(barrier.key())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        let now = current_millis();
        let expires_at = now.saturating_add(ttl_millis);
        let renewed = sqlx::query(
            r#"
            UPDATE roze_dtm_recovery_leases
            SET owner = $1, expires_at_millis = $2
            WHERE name = $3 AND (owner = $4 OR expires_at_millis <= $5)
            "#,
        )
        .bind(owner)
        .bind(expires_at as i64)
        .bind(name)
        .bind(owner)
        .bind(now as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if renewed == 1 {
            return Ok(true);
        }
        let inserted = sqlx::query(
            r#"
            INSERT INTO roze_dtm_recovery_leases (name, owner, expires_at_millis)
            VALUES ($1, $2, $3)
            ON CONFLICT(name) DO NOTHING
            "#,
        )
        .bind(name)
        .bind(owner)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(inserted == 1)
    }
}

#[derive(Debug, Clone)]
pub struct MySqlTransactionStore {
    pool: MySqlPool,
}

impl MySqlTransactionStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MySqlPool::connect(database_url).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_transactions (
                gid VARCHAR(128) PRIMARY KEY NOT NULL,
                payload LONGTEXT NOT NULL,
                updated_at_millis BIGINT NOT NULL
            ) ENGINE=InnoDB
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_barriers (
                barrier_key VARCHAR(512) PRIMARY KEY NOT NULL,
                gid VARCHAR(128) NOT NULL,
                branch_id VARCHAR(128) NOT NULL,
                op VARCHAR(32) NOT NULL,
                created_at_millis BIGINT NOT NULL
            ) ENGINE=InnoDB
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_recovery_leases (
                name VARCHAR(191) PRIMARY KEY NOT NULL,
                owner VARCHAR(128) NOT NULL,
                expires_at_millis BIGINT NOT NULL
            ) ENGINE=InnoDB
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roze_dtm_kv (
                category VARCHAR(191) NOT NULL,
                entry_key VARCHAR(191) NOT NULL,
                entry_value LONGTEXT NOT NULL,
                version BIGINT NOT NULL,
                created_at_millis BIGINT NOT NULL,
                updated_at_millis BIGINT NOT NULL,
                PRIMARY KEY (category, entry_key)
            ) ENGINE=InnoDB
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TransactionStore for MySqlTransactionStore {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&tx)?;
        let changed = sqlx::query(
            r#"
            INSERT IGNORE INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            anyhow::bail!("transaction already exists: {}", tx.gid);
        }
        Ok(())
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .transpose()
    }

    async fn update_transaction(&self, mut tx: Transaction) -> anyhow::Result<()> {
        tx.updated_at_millis = current_millis();
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            r#"
            INSERT INTO roze_dtm_transactions (gid, payload, updated_at_millis)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE
                payload = VALUES(payload),
                updated_at_millis = VALUES(updated_at_millis)
            "#,
        )
        .bind(&tx.gid)
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        append_dynamic_branch(&mut tx, branch)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? WHERE gid = ?",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        append_workflow_progress(&mut tx, progress)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? WHERE gid = ?",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE",
        )
        .bind(gid)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        apply_workflow_completion(&mut tx, status, rollback_reason, result)?;
        let payload = serde_json::to_string(&tx)?;
        sqlx::query(
            "UPDATE roze_dtm_transactions SET payload = ?, updated_at_millis = ? WHERE gid = ?",
        )
        .bind(payload)
        .bind(tx.updated_at_millis as i64)
        .bind(gid)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(tx)
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let rows =
            sqlx::query("SELECT payload FROM roze_dtm_transactions ORDER BY updated_at_millis ASC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .collect()
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        let row = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv WHERE category = ? AND entry_key = ?",
        )
        .bind(category)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| mysql_kv_entry(&row)).transpose()
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let rows = sqlx::query(
            "SELECT category, entry_key, entry_value, version, created_at_millis, \
             updated_at_millis FROM roze_dtm_kv \
             WHERE (? IS NULL OR category = ?) AND (? IS NULL OR entry_key = ?) \
             ORDER BY category, entry_key LIMIT ? OFFSET ?",
        )
        .bind(category)
        .bind(category)
        .bind(key)
        .bind(key)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(mysql_kv_entry).collect()
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        let changed = sqlx::query(
            "INSERT IGNORE INTO roze_dtm_kv \
             (category, entry_key, entry_value, version, created_at_millis, updated_at_millis) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.category)
        .bind(entry.key)
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.created_at_millis as i64)
        .bind(entry.updated_at_millis as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        anyhow::ensure!(
            entry.version == expected_version.saturating_add(1),
            "invalid KV version transition"
        );
        let changed = sqlx::query(
            "UPDATE roze_dtm_kv SET entry_value = ?, version = ?, updated_at_millis = ? \
             WHERE category = ? AND entry_key = ? AND version = ?",
        )
        .bind(entry.value)
        .bind(entry.version as i64)
        .bind(entry.updated_at_millis as i64)
        .bind(entry.category)
        .bind(entry.key)
        .bind(expected_version as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM roze_dtm_kv WHERE category = ? AND entry_key = ?")
            .bind(category)
            .bind(key)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        let key = barrier.key();
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            r#"
            INSERT IGNORE INTO roze_dtm_barriers
                (barrier_key, gid, branch_id, op, created_at_millis)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&key)
        .bind(&barrier.gid)
        .bind(&barrier.branch_id)
        .bind(&barrier.op)
        .bind(current_millis() as i64)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 0 {
            transaction.commit().await?;
            return Ok(BarrierDecision::SkipDuplicate);
        }
        if barrier.op == "try" {
            let cancel_key = format!("{}:{}:cancel", barrier.gid, barrier.branch_id);
            let cancelled: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&cancel_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if cancelled.is_some() {
                sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&key)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipCancelledTry);
            }
        }
        if barrier.op == "cancel" {
            let try_key = format!("{}:{}:try", barrier.gid, barrier.branch_id);
            let tried: Option<(String,)> =
                sqlx::query_as("SELECT barrier_key FROM roze_dtm_barriers WHERE barrier_key = ?")
                    .bind(&try_key)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if tried.is_none() {
                transaction.commit().await?;
                return Ok(BarrierDecision::SkipNullCompensation);
            }
        }
        transaction.commit().await?;
        Ok(BarrierDecision::Execute)
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM roze_dtm_barriers WHERE barrier_key = ?")
            .bind(barrier.key())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        let now = current_millis();
        let expires_at = now.saturating_add(ttl_millis);
        let renewed = sqlx::query(
            r#"
            UPDATE roze_dtm_recovery_leases
            SET owner = ?, expires_at_millis = ?
            WHERE name = ? AND (owner = ? OR expires_at_millis <= ?)
            "#,
        )
        .bind(owner)
        .bind(expires_at as i64)
        .bind(name)
        .bind(owner)
        .bind(now as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if renewed == 1 {
            return Ok(true);
        }
        let inserted = sqlx::query(
            r#"
            INSERT IGNORE INTO roze_dtm_recovery_leases (name, owner, expires_at_millis)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(name)
        .bind(owner)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(inserted == 1)
    }
}

#[derive(Debug, Clone)]
pub struct Dtm<S, I = NoopBranchInvoker> {
    store: S,
    invoker: I,
    options: DtmOptions,
}

impl<S> Dtm<S, NoopBranchInvoker>
where
    S: TransactionStore,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            invoker: NoopBranchInvoker,
            options: DtmOptions::default(),
        }
    }
}

impl<S, I> Dtm<S, I>
where
    S: TransactionStore,
    I: BranchInvoker,
{
    pub fn with_invoker(store: S, invoker: I) -> Self {
        Self {
            store,
            invoker,
            options: DtmOptions::default(),
        }
    }

    pub fn with_options(store: S, invoker: I, options: DtmOptions) -> Self {
        Self {
            store,
            invoker,
            options,
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn subscribe_topic(
        &self,
        topic: &str,
        url: &str,
        remark: &str,
    ) -> anyhow::Result<Topic> {
        kv::subscribe(&self.store, topic, url, remark).await
    }

    pub async fn unsubscribe_topic(&self, topic: &str, url: &str) -> anyhow::Result<Topic> {
        kv::unsubscribe(&self.store, topic, url).await
    }

    pub async fn get_topic(&self, topic: &str) -> anyhow::Result<Option<Topic>> {
        kv::get_topic(&self.store, topic).await
    }

    pub async fn delete_topic(&self, topic: &str) -> anyhow::Result<bool> {
        kv::delete_topic(&self.store, topic).await
    }

    pub async fn submit(&self, tx: Transaction) -> anyhow::Result<Transaction> {
        let mut tx = tx;
        tx.options.validate()?;
        if tx.kind == TransactionKind::Message {
            self.expand_message_topics(&mut tx).await?;
        }
        if !is_callback_workflow(&tx) {
            tx.timeout_millis
                .get_or_insert(self.options.transaction_timeout_millis);
        }
        self.store.insert_transaction(tx.clone()).await?;
        Ok(tx)
    }

    async fn expand_message_topics(&self, tx: &mut Transaction) -> anyhow::Result<()> {
        let mut expanded = Vec::new();
        for branch in std::mem::take(&mut tx.branches) {
            let Some(topic_name) = branch.action.strip_prefix("topic://") else {
                expanded.push(branch);
                continue;
            };
            let topic = self
                .get_topic(topic_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("topic not found: {topic_name}"))?;
            anyhow::ensure!(
                !topic.subscribers.is_empty(),
                "topic has no subscribers: {topic_name}"
            );
            let subscriber_count = topic.subscribers.len();
            for (index, subscriber) in topic.subscribers.into_iter().enumerate() {
                let mut resolved = branch.clone();
                if subscriber_count > 1 {
                    resolved.id = format!("{}-{:02}", resolved.id, index + 1);
                }
                resolved.action = subscriber.url;
                expanded.push(resolved);
            }
        }
        let mut branch_ids = BTreeSet::new();
        anyhow::ensure!(
            expanded
                .iter()
                .all(|branch| branch_ids.insert(branch.id.clone())),
            "message topic expansion produced duplicate branch ids"
        );
        tx.branches = expanded;
        Ok(())
    }

    pub async fn submit_default_tcc(
        &self,
        gid: impl Into<String>,
        branches: Vec<Branch>,
    ) -> anyhow::Result<Transaction> {
        self.submit(Transaction::default_tcc(gid, branches)).await
    }

    /// Registers a branch while a TCC or XA transaction is still prepared.
    /// Re-registering the same branch is idempotent when its definition matches.
    pub async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        self.store.register_branch(gid, branch).await
    }

    pub async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        self.store.record_workflow_progress(gid, progress).await
    }

    pub async fn finish_callback_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        self.store
            .finish_workflow(gid, status, rollback_reason, result)
            .await
    }

    /// Validates that a transaction can be submitted by the lifecycle-managed
    /// recovery worker without performing branch I/O in the caller's request.
    pub async fn schedule_submit(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        let allowed = match tx.kind {
            TransactionKind::Tcc | TransactionKind::Xa => {
                &[TransactionStatus::Prepared, TransactionStatus::Succeeding][..]
            }
            TransactionKind::Saga => {
                &[TransactionStatus::Submitted, TransactionStatus::Succeeding][..]
            }
            TransactionKind::Workflow => &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
            ][..],
            TransactionKind::Message => &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
            ][..],
        };
        ensure_status(&tx, allowed)?;
        if tx.kind == TransactionKind::Workflow && tx.status == TransactionStatus::Prepared {
            tx.status = TransactionStatus::Submitted;
            self.store.update_transaction(tx.clone()).await?;
        }
        Ok(tx)
    }

    /// Persists an abort request so compensation is executed by the recovery
    /// worker. Message abort has no remote branch I/O and completes inline.
    pub async fn schedule_abort(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        if tx.kind == TransactionKind::Message {
            return self.abort_message(gid).await;
        }
        let allowed = match tx.kind {
            TransactionKind::Tcc => &[
                TransactionStatus::Submitted,
                TransactionStatus::Trying,
                TransactionStatus::Prepared,
                TransactionStatus::Aborting,
            ][..],
            TransactionKind::Saga => {
                &[
                    TransactionStatus::Submitted,
                    TransactionStatus::Succeeding,
                    TransactionStatus::Aborting,
                ][..]
            }
            TransactionKind::Workflow => &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
                TransactionStatus::Aborting,
            ][..],
            TransactionKind::Xa => &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Aborting,
            ][..],
            TransactionKind::Message => unreachable!("message abort is handled above"),
        };
        ensure_status(&tx, allowed)?;
        tx.status = TransactionStatus::Aborting;
        for branch in &mut tx.branches {
            branch.next_retry_millis = None;
        }
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn start_saga(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, TransactionKind::Saga)?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[TransactionStatus::Submitted, TransactionStatus::Succeeding],
        )?;
        let execution_options = tx.options.clone();
        let max_attempts = execution_options
            .retry_limit
            .map(|retries| retries.saturating_add(1))
            .unwrap_or(1);
        tx.status = TransactionStatus::Succeeding;
        self.store.update_transaction(tx.clone()).await?;
        for index in 0..tx.branches.len() {
            if tx.branches[index].status == BranchStatus::Succeeded {
                continue;
            }
            let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "action");
            if self.store.barrier(barrier.clone()).await? != BarrierDecision::Execute {
                return self
                    .store
                    .get_transaction(gid)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"));
            }
            let action_result = {
                let branch = &mut tx.branches[index];
                branch.status = BranchStatus::Running;
                branch.attempts = branch.attempts.saturating_add(1);
                let action = branch.action.clone();
                self.invoke_branch(&execution_options, branch, &action).await
            };
            match action_result {
                Ok(()) => {
                    let branch = &mut tx.branches[index];
                    branch.status = BranchStatus::Succeeded;
                    branch.next_retry_millis = None;
                }
                Err(_) => {
                    if tx.branches[index].attempts < max_attempts {
                        tx.branches[index].status = BranchStatus::Failed;
                        self.store.release_barrier(&barrier).await?;
                        self.store.update_transaction(tx.clone()).await?;
                        return Ok(tx);
                    }
                    tx.branches[index].status = BranchStatus::Compensating;
                    tx.status = TransactionStatus::Aborting;
                    for previous in &mut tx.branches {
                        if previous.status == BranchStatus::Succeeded {
                            previous.status = BranchStatus::Compensating;
                        }
                    }
                    self.store.update_transaction(tx.clone()).await?;
                    return self.abort_saga(gid).await;
                }
            }
            self.store.update_transaction(tx.clone()).await?;
        }
        tx.status = TransactionStatus::Succeeded;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn abort_saga(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, TransactionKind::Saga)?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Succeeding,
                TransactionStatus::Aborting,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Aborting;
        for branch in tx.branches.iter_mut().rev() {
            if branch.status == BranchStatus::Pending {
                branch.status = BranchStatus::Skipped;
                continue;
            }
            if !matches!(
                branch.status,
                BranchStatus::Succeeded | BranchStatus::Compensating
            ) {
                continue;
            }
            branch.status = BranchStatus::Compensating;
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "compensate");
            match self.store.barrier(barrier.clone()).await? {
                BarrierDecision::Execute => {
                    if let Some(compensate) = branch.compensate.clone() {
                        branch.attempts = branch.attempts.saturating_add(1);
                        if self
                            .invoke_url(&execution_options, branch, &compensate)
                            .await
                            .is_err()
                        {
                            branch.status = BranchStatus::Compensating;
                            self.store.release_barrier(&barrier).await?;
                            self.store.update_transaction(tx.clone()).await?;
                            return Ok(tx);
                        }
                    }
                    branch.status = BranchStatus::Skipped;
                    branch.next_retry_millis = None;
                }
                BarrierDecision::SkipDuplicate => {
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
                BarrierDecision::SkipNullCompensation => {
                    unreachable!("Saga compensation is not TCC cancel")
                }
                BarrierDecision::SkipCancelledTry => {
                    unreachable!("Saga compensation is not TCC try")
                }
            }
        }
        tx.status = TransactionStatus::Aborted;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn prepare_tcc(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, TransactionKind::Tcc)?;
        if tx.status == TransactionStatus::Prepared {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[TransactionStatus::Submitted, TransactionStatus::Trying],
        )?;
        let execution_options = tx.options.clone();
        let max_attempts = self.max_attempts(&execution_options);
        tx.status = TransactionStatus::Trying;
        for branch in &mut tx.branches {
            if branch.status == BranchStatus::Succeeded {
                continue;
            }
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "try");
            let decision = self.store.barrier(barrier.clone()).await?;
            match decision {
                BarrierDecision::Execute => {
                    branch.status = BranchStatus::Running;
                    branch.attempts = branch.attempts.saturating_add(1);
                    let action = branch.action.clone();
                    match self
                        .invoke_branch(&execution_options, branch, &action)
                        .await
                    {
                        Ok(()) => {
                            branch.status = BranchStatus::Succeeded;
                            branch.next_retry_millis = None;
                        }
                        Err(_) => {
                            branch.status = BranchStatus::Failed;
                            self.store.release_barrier(&barrier).await?;
                            if branch.attempts >= max_attempts {
                                tx.status = TransactionStatus::Aborting;
                            }
                            self.store.update_transaction(tx.clone()).await?;
                            return Ok(tx);
                        }
                    }
                }
                BarrierDecision::SkipCancelledTry => {
                    branch.status = BranchStatus::Skipped;
                    branch.next_retry_millis = None;
                    tx.status = TransactionStatus::Aborting;
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
                BarrierDecision::SkipDuplicate => {}
                BarrierDecision::SkipNullCompensation => {
                    unreachable!("TCC try is not a compensation operation")
                }
            }
        }
        tx.status = TransactionStatus::Prepared;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn confirm_tcc(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, TransactionKind::Tcc)?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[TransactionStatus::Prepared, TransactionStatus::Succeeding],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Succeeding;
        for branch in &mut tx.branches {
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "confirm");
            let decision = self.store.barrier(barrier.clone()).await?;
            if decision == BarrierDecision::Execute {
                let confirm = branch.confirm.clone().ok_or_else(|| {
                    anyhow::anyhow!("missing confirm action for branch {}", branch.id)
                })?;
                branch.attempts = branch.attempts.saturating_add(1);
                match self
                    .invoke_url(&execution_options, branch, &confirm)
                    .await
                {
                    Ok(()) => {
                        branch.status = BranchStatus::Succeeded;
                        branch.next_retry_millis = None;
                    }
                    Err(_) => {
                        branch.status = BranchStatus::Failed;
                        self.store.release_barrier(&barrier).await?;
                        self.store.update_transaction(tx.clone()).await?;
                        return Ok(tx);
                    }
                }
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn cancel_tcc(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, TransactionKind::Tcc)?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Trying,
                TransactionStatus::Prepared,
                TransactionStatus::Aborting,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Aborting;
        for branch in &mut tx.branches {
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "cancel");
            match self.store.barrier(barrier.clone()).await? {
                BarrierDecision::Execute => {
                    let cancel = branch
                        .cancel
                        .clone()
                        .or(branch.compensate.clone())
                        .ok_or_else(|| {
                            anyhow::anyhow!("missing cancel action for branch {}", branch.id)
                        })?;
                    branch.attempts = branch.attempts.saturating_add(1);
                    match self
                        .invoke_url(&execution_options, branch, &cancel)
                        .await
                    {
                        Ok(()) => {
                            branch.status = BranchStatus::Skipped;
                            branch.next_retry_millis = None;
                        }
                        Err(_) => {
                            branch.status = BranchStatus::Failed;
                            self.store.release_barrier(&barrier).await?;
                            self.store.update_transaction(tx.clone()).await?;
                            return Ok(tx);
                        }
                    }
                }
                BarrierDecision::SkipNullCompensation => {
                    branch.status = BranchStatus::Skipped;
                    branch.next_retry_millis = None;
                }
                BarrierDecision::SkipDuplicate => {}
                BarrierDecision::SkipCancelledTry => {
                    unreachable!("TCC cancel is not a try operation")
                }
            }
        }
        tx.status = TransactionStatus::Aborted;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn prepare_message(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Message)
            .await?;
        if tx.status == TransactionStatus::Prepared {
            return Ok(tx);
        }
        ensure_status(&tx, &[TransactionStatus::Submitted])?;
        tx.status = TransactionStatus::Prepared;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn prepare_workflow(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Workflow)
            .await?;
        if tx.status == TransactionStatus::Prepared || tx.status.is_terminal() {
            return Ok(tx);
        }
        ensure_status(&tx, &[TransactionStatus::Submitted])?;
        tx.status = TransactionStatus::Prepared;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn start_workflow(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Workflow)
            .await?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Succeeding;
        loop {
            let succeeded = tx
                .branches
                .iter()
                .filter(|branch| branch.status == BranchStatus::Succeeded)
                .map(|branch| branch.id.clone())
                .collect::<BTreeSet<_>>();
            let next = tx.branches.iter().position(|branch| {
                matches!(branch.status, BranchStatus::Pending | BranchStatus::Failed)
                    && branch
                        .dependencies
                        .iter()
                        .all(|dependency| succeeded.contains(dependency))
            });
            let Some(index) = next else {
                break;
            };
            let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "workflow");
            if self.store.barrier(barrier.clone()).await? != BarrierDecision::Execute {
                return self
                    .store
                    .get_transaction(gid)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"));
            }
            let action = tx.branches[index].action.clone();
            tx.branches[index].status = BranchStatus::Running;
            tx.branches[index].attempts = tx.branches[index].attempts.saturating_add(1);
            if self
                .invoke_branch(&execution_options, &mut tx.branches[index], &action)
                .await
                .is_err()
            {
                tx.branches[index].status = BranchStatus::Failed;
                tx.status = TransactionStatus::Aborting;
                self.store.release_barrier(&barrier).await?;
                self.store.update_transaction(tx.clone()).await?;
                return self.abort_workflow(gid).await;
            }
            tx.branches[index].status = BranchStatus::Succeeded;
            tx.branches[index].next_retry_millis = None;
            self.store.update_transaction(tx.clone()).await?;
        }
        if tx
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Succeeded)
        {
            tx.status = TransactionStatus::Succeeded;
            self.store.update_transaction(tx.clone()).await?;
            return Ok(tx);
        }
        anyhow::bail!("workflow {} has unresolved or cyclic dependencies", tx.gid)
    }

    pub async fn abort_workflow(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Workflow)
            .await?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
                TransactionStatus::Aborting,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Aborting;
        for branch in tx.branches.iter_mut().rev() {
            if branch.status != BranchStatus::Succeeded {
                if branch.status == BranchStatus::Pending {
                    branch.status = BranchStatus::Skipped;
                }
                continue;
            }
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "workflow_rollback");
            if self.store.barrier(barrier.clone()).await? == BarrierDecision::Execute {
                let compensate = branch.compensate.clone().ok_or_else(|| {
                    anyhow::anyhow!("missing workflow compensation URL for {}", branch.id)
                })?;
                branch.attempts = branch.attempts.saturating_add(1);
                if self
                    .invoke_url(&execution_options, branch, &compensate)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Compensating;
                    self.store.release_barrier(&barrier).await?;
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
            }
            branch.status = BranchStatus::Skipped;
            branch.next_retry_millis = None;
        }
        tx.status = TransactionStatus::Aborted;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn dispatch_message(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Message)
            .await?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Succeeding,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Succeeding;
        for branch in &mut tx.branches {
            if branch.status == BranchStatus::Succeeded {
                continue;
            }
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "message");
            if self.store.barrier(barrier.clone()).await? == BarrierDecision::Execute {
                branch.status = BranchStatus::Running;
                branch.attempts = branch.attempts.saturating_add(1);
                let action = branch.action.clone();
                if self
                    .invoke_branch(&execution_options, branch, &action)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Succeeded;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn abort_message(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Message)
            .await?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[TransactionStatus::Submitted, TransactionStatus::Prepared],
        )?;
        tx.status = TransactionStatus::Aborted;
        for branch in &mut tx.branches {
            branch.status = BranchStatus::Skipped;
            branch.next_retry_millis = None;
        }
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn prepare_xa(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self.transaction_of_kind(gid, TransactionKind::Xa).await?;
        if tx.status == TransactionStatus::Prepared {
            return Ok(tx);
        }
        ensure_status(&tx, &[TransactionStatus::Submitted])?;
        tx.status = TransactionStatus::Prepared;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn commit_xa(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self.transaction_of_kind(gid, TransactionKind::Xa).await?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[TransactionStatus::Prepared, TransactionStatus::Succeeding],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Succeeding;
        for branch in &mut tx.branches {
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "commit");
            if self.store.barrier(barrier.clone()).await? == BarrierDecision::Execute {
                let commit = branch
                    .confirm
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing XA commit URL for {}", branch.id))?;
                branch.attempts = branch.attempts.saturating_add(1);
                if self
                    .invoke_url(&execution_options, branch, &commit)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Succeeded;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn rollback_xa(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self.transaction_of_kind(gid, TransactionKind::Xa).await?;
        if tx.status == TransactionStatus::Aborted {
            return Ok(tx);
        }
        ensure_status(
            &tx,
            &[
                TransactionStatus::Submitted,
                TransactionStatus::Prepared,
                TransactionStatus::Aborting,
            ],
        )?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Aborting;
        for branch in &mut tx.branches {
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "rollback");
            if self.store.barrier(barrier.clone()).await? == BarrierDecision::Execute {
                let rollback = branch
                    .cancel
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing XA rollback URL for {}", branch.id))?;
                branch.attempts = branch.attempts.saturating_add(1);
                if self
                    .invoke_url(&execution_options, branch, &rollback)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.store.update_transaction(tx.clone()).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Skipped;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Aborted;
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    async fn transaction_of_kind(
        &self,
        gid: &str,
        kind: TransactionKind,
    ) -> anyhow::Result<Transaction> {
        let tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        ensure_kind(&tx, kind)?;
        Ok(tx)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<Transaction>> {
        self.store.list_transactions().await
    }

    /// Permanently stops automatic processing for a non-terminal transaction.
    pub async fn force_stop(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        anyhow::ensure!(!tx.status.is_terminal(), "transaction {gid} is already terminal");
        tx.status = TransactionStatus::Failed;
        for branch in &mut tx.branches {
            branch.next_retry_millis = None;
        }
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    /// Makes all retryable branches of one transaction immediately due.
    pub async fn reset_retry(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        anyhow::ensure!(!tx.status.is_terminal(), "transaction {gid} is terminal");
        let now = current_millis();
        for branch in &mut tx.branches {
            if matches!(branch.status, BranchStatus::Failed | BranchStatus::Compensating) {
                branch.next_retry_millis = Some(now);
            }
        }
        self.store.update_transaction(tx.clone()).await?;
        Ok(tx)
    }

    /// Resets up to `limit` non-terminal transactions for immediate recovery.
    pub async fn reset_retry_batch(&self, limit: usize) -> anyhow::Result<Vec<Transaction>> {
        let mut reset = Vec::new();
        for tx in self.store.list_transactions().await? {
            if reset.len() >= limit.max(1) {
                break;
            }
            if !tx.status.is_terminal() {
                reset.push(self.reset_retry(&tx.gid).await?);
            }
        }
        Ok(reset)
    }

    /// Forces one recoverable transaction through its next state transition.
    ///
    /// Terminal transactions are returned unchanged. In-flight states that
    /// cannot be replayed safely are rejected instead of re-invoking branches.
    pub async fn recover(&self, gid: &str) -> anyhow::Result<Transaction> {
        let tx = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        if tx.status.is_terminal() {
            return Ok(tx);
        }
        if is_expired(&tx, current_millis()) {
            return match tx.kind {
                TransactionKind::Tcc => self.cancel_tcc(gid).await,
                TransactionKind::Saga => self.abort_saga(gid).await,
                TransactionKind::Workflow if is_callback_workflow(&tx) => {
                    self.finish_callback_workflow(
                        gid,
                        TransactionStatus::Failed,
                        Some("workflow callback timed out".to_owned()),
                        None,
                    )
                    .await
                }
                TransactionKind::Workflow => self.abort_workflow(gid).await,
                TransactionKind::Message if tx.status == TransactionStatus::Succeeding => {
                    self.dispatch_message(gid).await
                }
                TransactionKind::Message => self.abort_message(gid).await,
                TransactionKind::Xa if tx.status == TransactionStatus::Succeeding => {
                    self.commit_xa(gid).await
                }
                TransactionKind::Xa => self.rollback_xa(gid).await,
            };
        }
        match (tx.kind, tx.status) {
            (TransactionKind::Tcc, TransactionStatus::Submitted | TransactionStatus::Trying) => {
                self.prepare_tcc(gid).await
            }
            (TransactionKind::Tcc, TransactionStatus::Prepared | TransactionStatus::Succeeding) => {
                self.confirm_tcc(gid).await
            }
            (TransactionKind::Tcc, TransactionStatus::Aborting) => self.cancel_tcc(gid).await,
            (
                TransactionKind::Saga,
                TransactionStatus::Submitted | TransactionStatus::Succeeding,
            ) => self.start_saga(gid).await,
            (TransactionKind::Saga, TransactionStatus::Aborting) => self.abort_saga(gid).await,
            (
                TransactionKind::Workflow,
                TransactionStatus::Submitted | TransactionStatus::Succeeding,
            ) => self.start_workflow(gid).await,
            (TransactionKind::Workflow, TransactionStatus::Aborting) => {
                self.abort_workflow(gid).await
            }
            (TransactionKind::Message, TransactionStatus::Submitted) => {
                self.prepare_message(gid).await
            }
            (
                TransactionKind::Message,
                TransactionStatus::Prepared | TransactionStatus::Succeeding,
            ) => self.dispatch_message(gid).await,
            (TransactionKind::Xa, TransactionStatus::Submitted) => self.prepare_xa(gid).await,
            (TransactionKind::Xa, TransactionStatus::Prepared | TransactionStatus::Succeeding) => {
                self.commit_xa(gid).await
            }
            (TransactionKind::Xa, TransactionStatus::Aborting) => self.rollback_xa(gid).await,
            (_, status) => anyhow::bail!("transaction {gid} is in non-replayable state {status:?}"),
        }
    }

    pub async fn tick_recover_once(&self) -> anyhow::Result<Vec<Transaction>> {
        let mut changed = Vec::new();
        let now = current_millis();
        for tx in self.store.list_transactions().await? {
            if tx.status.is_terminal() {
                continue;
            }
            if is_expired(&tx, now) {
                let next = match tx.kind {
                    TransactionKind::Tcc => self.cancel_tcc(&tx.gid).await?,
                    TransactionKind::Saga => self.abort_saga(&tx.gid).await?,
                    TransactionKind::Workflow if is_callback_workflow(&tx) => {
                        self.finish_callback_workflow(
                            &tx.gid,
                            TransactionStatus::Failed,
                            Some("workflow callback timed out".to_owned()),
                            None,
                        )
                        .await?
                    }
                    TransactionKind::Workflow => self.abort_workflow(&tx.gid).await?,
                    TransactionKind::Message if tx.status == TransactionStatus::Succeeding => {
                        self.dispatch_message(&tx.gid).await?
                    }
                    TransactionKind::Message => self.abort_message(&tx.gid).await?,
                    TransactionKind::Xa if tx.status == TransactionStatus::Succeeding => {
                        self.commit_xa(&tx.gid).await?
                    }
                    TransactionKind::Xa => self.rollback_xa(&tx.gid).await?,
                };
                changed.push(next);
                continue;
            }
            if !transaction_due(&tx, now) {
                continue;
            }
            let next = match (tx.kind, tx.status) {
                (
                    TransactionKind::Tcc,
                    TransactionStatus::Submitted | TransactionStatus::Trying,
                ) => self.prepare_tcc(&tx.gid).await?,
                (
                    TransactionKind::Tcc,
                    TransactionStatus::Prepared | TransactionStatus::Succeeding,
                ) => self.confirm_tcc(&tx.gid).await?,
                (TransactionKind::Tcc, TransactionStatus::Aborting) => {
                    self.cancel_tcc(&tx.gid).await?
                }
                (
                    TransactionKind::Saga,
                    TransactionStatus::Submitted | TransactionStatus::Succeeding,
                ) => {
                    self.start_saga(&tx.gid).await?
                }
                (TransactionKind::Saga, TransactionStatus::Aborting) => {
                    self.abort_saga(&tx.gid).await?
                }
                (
                    TransactionKind::Workflow,
                    TransactionStatus::Submitted | TransactionStatus::Succeeding,
                ) => self.start_workflow(&tx.gid).await?,
                (TransactionKind::Workflow, TransactionStatus::Aborting) => {
                    self.abort_workflow(&tx.gid).await?
                }
                (TransactionKind::Message, TransactionStatus::Submitted) => {
                    self.prepare_message(&tx.gid).await?
                }
                (
                    TransactionKind::Message,
                    TransactionStatus::Prepared | TransactionStatus::Succeeding,
                ) => self.dispatch_message(&tx.gid).await?,
                (TransactionKind::Xa, TransactionStatus::Submitted) => {
                    self.prepare_xa(&tx.gid).await?
                }
                (
                    TransactionKind::Xa,
                    TransactionStatus::Prepared | TransactionStatus::Succeeding,
                ) => self.commit_xa(&tx.gid).await?,
                (TransactionKind::Xa, TransactionStatus::Aborting) => {
                    self.rollback_xa(&tx.gid).await?
                }
                _ => continue,
            };
            changed.push(next);
        }
        Ok(changed)
    }

    pub async fn tick_recover_once_with_lease(
        &self,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<Vec<Transaction>> {
        if !self
            .store
            .try_acquire_recovery_lease("roze-dtm-recovery", owner, ttl_millis)
            .await?
        {
            return Ok(Vec::new());
        }
        self.tick_recover_once().await
    }

    fn max_attempts(&self, options: &TransactionOptions) -> u32 {
        options
            .retry_limit
            .map(|retries| retries.saturating_add(1))
            .unwrap_or(self.options.max_attempts)
    }

    async fn invoke_branch(
        &self,
        options: &TransactionOptions,
        branch: &mut Branch,
        url: &str,
    ) -> anyhow::Result<()> {
        match self
            .invoker
            .invoke_with_options(url, &branch.payload, options)
            .await
        {
            Ok(()) => Ok(()),
            Err(_) => {
                let retry_backoff_millis = options
                    .retry_interval_millis
                    .unwrap_or(self.options.retry_backoff_millis);
                record_branch_failure(
                    branch,
                    "branch_call_failed".to_string(),
                    retry_backoff_millis,
                    self.options
                        .max_retry_backoff_millis
                        .max(retry_backoff_millis),
                );
                Err(anyhow::anyhow!("branch call failed"))
            }
        }
    }

    async fn invoke_url(
        &self,
        options: &TransactionOptions,
        branch: &mut Branch,
        url: &str,
    ) -> anyhow::Result<()> {
        self.invoke_branch(options, branch, url).await
    }
}

fn append_dynamic_branch(tx: &mut Transaction, branch: Branch) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            tx.kind,
            TransactionKind::Tcc | TransactionKind::Xa | TransactionKind::Workflow
        ),
        "transaction {} does not support dynamic branches",
        tx.gid
    );
    anyhow::ensure!(
        matches!(
            tx.status,
            TransactionStatus::Submitted | TransactionStatus::Prepared
        ),
        "transaction {} is not accepting branches",
        tx.gid
    );
    if let Some(existing) = tx.branches.iter().find(|existing| existing.id == branch.id) {
        anyhow::ensure!(
            existing == &branch,
            "branch definition conflicts: {}",
            branch.id
        );
        return Ok(());
    }
    tx.branches.push(branch);
    tx.updated_at_millis = current_millis();
    Ok(())
}

fn append_workflow_progress(
    tx: &mut Transaction,
    progress: WorkflowProgress,
) -> anyhow::Result<()> {
    ensure_callback_workflow(tx)?;
    ensure_status(tx, &[TransactionStatus::Prepared])?;
    progress.validate()?;
    if let Some(existing) = tx.workflow_progresses.iter().find(|existing| {
        existing.branch_id == progress.branch_id && existing.operation == progress.operation
    }) {
        anyhow::ensure!(
            existing == &progress,
            "workflow progress conflicts: {}:{}",
            progress.branch_id,
            progress.operation
        );
        return Ok(());
    }
    anyhow::ensure!(
        tx.workflow_progresses.len() < 1_000,
        "workflow progress count exceeds 1000"
    );
    let total_bytes = tx
        .workflow_progresses
        .iter()
        .try_fold(progress.data.len(), |total, item| {
            total.checked_add(item.data.len())
        })
        .context("workflow progress data size overflow")?;
    anyhow::ensure!(
        total_bytes <= 2 * 1024 * 1024,
        "workflow progress data exceeds 2 MiB in total"
    );
    tx.workflow_progresses.push(progress);
    tx.updated_at_millis = current_millis();
    Ok(())
}

fn apply_workflow_completion(
    tx: &mut Transaction,
    status: TransactionStatus,
    rollback_reason: Option<String>,
    result: Option<String>,
) -> anyhow::Result<()> {
    ensure_callback_workflow(tx)?;
    anyhow::ensure!(
        matches!(status, TransactionStatus::Succeeded | TransactionStatus::Failed),
        "workflow completion status must be succeeded or failed"
    );
    let rollback_reason = rollback_reason.unwrap_or_default();
    let result = result.unwrap_or_default();
    anyhow::ensure!(
        rollback_reason.len() <= 4_096,
        "workflow rollback reason exceeds 4096 bytes"
    );
    anyhow::ensure!(
        result.len() <= 2 * 1024 * 1024,
        "workflow result exceeds 2 MiB"
    );
    if tx.status.is_terminal() {
        anyhow::ensure!(
            tx.status == status
                && tx
                    .metadata
                    .get("rollback_reason")
                    .map(String::as_str)
                    .unwrap_or_default()
                    == rollback_reason.as_str()
                && tx
                    .metadata
                    .get("dtm.workflow.result")
                    .map(String::as_str)
                    .unwrap_or_default()
                    == result.as_str(),
            "workflow completion conflicts with terminal transaction {}",
            tx.gid
        );
        return Ok(());
    }
    ensure_status(tx, &[TransactionStatus::Prepared])?;
    tx.status = status;
    if rollback_reason.is_empty() {
        tx.metadata.remove("rollback_reason");
    } else {
        tx.metadata
            .insert("rollback_reason".to_owned(), rollback_reason);
    }
    if result.is_empty() {
        tx.metadata.remove("dtm.workflow.result");
    } else {
        tx.metadata.insert("dtm.workflow.result".to_owned(), result);
    }
    tx.updated_at_millis = current_millis();
    Ok(())
}

fn ensure_callback_workflow(tx: &Transaction) -> anyhow::Result<()> {
    anyhow::ensure!(
        tx.kind == TransactionKind::Workflow,
        "transaction {} is not a workflow",
        tx.gid
    );
    anyhow::ensure!(
        is_callback_workflow(tx),
        "transaction {} is not a callback workflow",
        tx.gid
    );
    Ok(())
}

fn is_callback_workflow(tx: &Transaction) -> bool {
    tx.kind == TransactionKind::Workflow
        && tx.branches.is_empty()
        && tx
            .metadata
            .get("dtm.query_prepared")
            .is_some_and(|value| !value.is_empty())
}

fn ensure_kind(tx: &Transaction, expected: TransactionKind) -> anyhow::Result<()> {
    if tx.kind != expected {
        anyhow::bail!(
            "transaction {} is {:?}, expected {:?}",
            tx.gid,
            tx.kind,
            expected
        );
    }
    Ok(())
}

fn ensure_status(tx: &Transaction, allowed: &[TransactionStatus]) -> anyhow::Result<()> {
    anyhow::ensure!(
        allowed.contains(&tx.status),
        "transaction {} is in non-replayable state {:?}",
        tx.gid,
        tx.status
    );
    Ok(())
}

fn sqlite_kv_entry(row: &SqliteRow) -> anyhow::Result<KvEntry> {
    kv_entry_from_values(
        row.get("category"),
        row.get("entry_key"),
        row.get("entry_value"),
        row.get("version"),
        row.get("created_at_millis"),
        row.get("updated_at_millis"),
    )
}

fn postgres_kv_entry(row: &PgRow) -> anyhow::Result<KvEntry> {
    kv_entry_from_values(
        row.get("category"),
        row.get("entry_key"),
        row.get("entry_value"),
        row.get("version"),
        row.get("created_at_millis"),
        row.get("updated_at_millis"),
    )
}

fn mysql_kv_entry(row: &MySqlRow) -> anyhow::Result<KvEntry> {
    kv_entry_from_values(
        row.get("category"),
        row.get("entry_key"),
        row.get("entry_value"),
        row.get("version"),
        row.get("created_at_millis"),
        row.get("updated_at_millis"),
    )
}

fn kv_entry_from_values(
    category: String,
    key: String,
    value: String,
    version: i64,
    created_at_millis: i64,
    updated_at_millis: i64,
) -> anyhow::Result<KvEntry> {
    Ok(KvEntry {
        category,
        key,
        value,
        version: version.try_into()?,
        created_at_millis: created_at_millis.try_into()?,
        updated_at_millis: updated_at_millis.try_into()?,
    })
}

fn current_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_millis() as u64,
        Err(_) => 0,
    }
}

async fn insert_barrier(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    key: &str,
    barrier: &BranchBarrier,
) -> anyhow::Result<bool> {
    let inserted = sqlx::query(
        r#"
        INSERT OR IGNORE INTO roze_dtm_barriers
            (barrier_key, gid, branch_id, op, created_at_millis)
        VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(key)
    .bind(&barrier.gid)
    .bind(&barrier.branch_id)
    .bind(&barrier.op)
    .bind(current_millis() as i64)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok(inserted == 1)
}

fn record_branch_failure(
    branch: &mut Branch,
    error: String,
    backoff_millis: u64,
    max_backoff_millis: u64,
) {
    let shift = branch.attempts.saturating_sub(1).min(16);
    let factor = 1_u64 << shift;
    let backoff = backoff_millis
        .saturating_mul(factor)
        .min(max_backoff_millis.max(backoff_millis));
    branch.last_error = Some(error);
    branch.next_retry_millis = Some(current_millis().saturating_add(backoff));
}

fn transaction_due(tx: &Transaction, now: u64) -> bool {
    tx.branches
        .iter()
        .filter(|branch| {
            matches!(
                branch.status,
                BranchStatus::Failed | BranchStatus::Running | BranchStatus::Compensating
            )
        })
        .all(|branch| branch.next_retry_millis.is_none_or(|next| next <= now))
}

fn is_expired(tx: &Transaction, now: u64) -> bool {
    let Some(timeout) = tx.timeout_millis else {
        return false;
    };
    now.saturating_sub(tx.created_at_millis) >= timeout
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    #[derive(Clone)]
    struct FailingOnceInvoker {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct FailOnCallInvoker {
        calls: Arc<AtomicUsize>,
        fail_on: usize,
    }

    #[derive(Clone)]
    struct FailOnCallsInvoker {
        calls: Arc<AtomicUsize>,
        fail_on: Arc<BTreeSet<usize>>,
    }

    #[derive(Clone, Default)]
    struct RecordingOptionsInvoker {
        options: Arc<Mutex<Option<TransactionOptions>>>,
    }

    #[async_trait]
    impl BranchInvoker for FailingOnceInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst);
            if calls == 0 {
                anyhow::bail!("temporary failure");
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BranchInvoker for FailOnCallInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == self.fail_on {
                anyhow::bail!("injected branch failure");
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BranchInvoker for FailOnCallsInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_on.contains(&call) {
                anyhow::bail!("injected branch failure");
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BranchInvoker for RecordingOptionsInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }

        async fn invoke_with_options(
            &self,
            _url: &str,
            _payload: &serde_json::Value,
            options: &TransactionOptions,
        ) -> anyhow::Result<()> {
            *self.options.lock().expect("recording options lock") = Some(options.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn saga_can_submit_and_abort_with_compensation_barriers() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let tx = Transaction::saga(
            "gid-1",
            vec![Branch::saga(
                "b1",
                "http://inventory/reserve",
                "http://inventory/release",
                serde_json::json!({"sku": "A"}),
            )],
        );
        dtm.submit(tx).await.expect("submit");

        let aborted = dtm.abort_saga("gid-1").await.expect("abort");

        assert_eq!(aborted.status, TransactionStatus::Aborted);
        assert_eq!(aborted.branches[0].status, BranchStatus::Skipped);
    }

    #[tokio::test]
    async fn saga_uses_transaction_retry_limit_during_recovery() {
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            FailingOnceInvoker {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            DtmOptions {
                retry_backoff_millis: 100,
                ..DtmOptions::default()
            },
        );
        let mut transaction = Transaction::saga(
            "gid-saga-retry-limit",
            vec![Branch::saga(
                "b1",
                "action",
                "compensate",
                serde_json::json!({}),
            )],
        );
        transaction.options.retry_limit = Some(1);
        transaction.options.retry_interval_millis = Some(1);
        dtm.submit(transaction).await.expect("submit");

        let retrying = dtm
            .start_saga("gid-saga-retry-limit")
            .await
            .expect("first attempt");
        assert_eq!(retrying.status, TransactionStatus::Succeeding);
        assert_eq!(retrying.branches[0].status, BranchStatus::Failed);

        tokio::time::sleep(Duration::from_millis(2)).await;
        let recovered = dtm.tick_recover_once().await.expect("retry");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn branch_invoker_receives_transaction_options() {
        let invoker = RecordingOptionsInvoker::default();
        let recorded = Arc::clone(&invoker.options);
        let dtm = Dtm::with_invoker(InMemoryTransactionStore::new(), invoker);
        let mut transaction = Transaction::saga(
            "gid-options",
            vec![Branch::saga(
                "b1",
                "action",
                "compensate",
                serde_json::json!({}),
            )],
        );
        transaction.options.request_timeout_millis = Some(2_000);
        transaction.options.branch_headers.insert(
            "x-transaction".to_owned(),
            "transaction-a".to_owned(),
        );
        dtm.submit(transaction).await.expect("submit");
        dtm.start_saga("gid-options").await.expect("start");

        let options = recorded
            .lock()
            .expect("recording options lock")
            .clone()
            .expect("recorded options");
        assert_eq!(options.request_timeout_millis, Some(2_000));
        assert_eq!(options.branch_headers["x-transaction"], "transaction-a");
    }

    #[tokio::test]
    async fn saga_compensation_failure_stays_aborting_until_retry_succeeds() {
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            FailOnCallsInvoker {
                calls: Arc::new(AtomicUsize::new(0)),
                fail_on: Arc::new(BTreeSet::from([1, 2])),
            },
            DtmOptions {
                retry_backoff_millis: 1,
                max_retry_backoff_millis: 10,
                ..DtmOptions::default()
            },
        );
        dtm.submit(Transaction::saga(
            "gid-saga-compensate-retry",
            vec![
                Branch::saga("b1", "action-1", "compensate-1", serde_json::json!({})),
                Branch::saga("b2", "action-2", "compensate-2", serde_json::json!({})),
            ],
        ))
        .await
        .expect("submit");

        let aborting = dtm
            .start_saga("gid-saga-compensate-retry")
            .await
            .expect("start saga");
        assert_eq!(aborting.status, TransactionStatus::Aborting);
        assert!(aborting
            .branches
            .iter()
            .any(|branch| branch.status == BranchStatus::Compensating));

        tokio::time::sleep(Duration::from_millis(2)).await;
        let recovered = dtm.tick_recover_once().await.expect("compensation retry");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Aborted);
        assert!(recovered[0]
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Skipped));
    }

    #[tokio::test]
    async fn tcc_prepares_and_confirms() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let tx = Transaction::tcc(
            "gid-2",
            vec![Branch::tcc_try(
                "b1",
                "http://account/try",
                "http://account/confirm",
                "http://account/cancel",
                serde_json::json!({"amount": 100}),
            )],
        );
        dtm.submit(tx).await.expect("submit");

        let prepared = dtm.prepare_tcc("gid-2").await.expect("prepare");
        assert_eq!(prepared.status, TransactionStatus::Prepared);
        let confirmed = dtm.confirm_tcc("gid-2").await.expect("confirm");
        assert_eq!(confirmed.status, TransactionStatus::Succeeded);
        let duplicate = dtm.confirm_tcc("gid-2").await.expect("idempotent confirm");
        assert_eq!(duplicate.status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn tcc_rejects_confirm_before_prepare() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::tcc(
            "gid-early-confirm",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit");

        let error = dtm
            .confirm_tcc("gid-early-confirm")
            .await
            .expect_err("confirm must require prepared state");
        assert!(error.to_string().contains("non-replayable state"));
    }

    #[tokio::test]
    async fn default_transaction_kind_is_tcc() {
        assert_eq!(TransactionKind::default(), TransactionKind::Tcc);

        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let tx = dtm
            .submit_default_tcc(
                "gid-default",
                vec![Branch::tcc_try(
                    "b1",
                    "http://try",
                    "http://confirm",
                    "http://cancel",
                    serde_json::json!({}),
                )],
            )
            .await
            .expect("submit");

        assert_eq!(tx.kind, TransactionKind::Tcc);
    }

    #[tokio::test]
    async fn barrier_skips_null_compensation() {
        let store = InMemoryTransactionStore::new();
        let decision = store
            .barrier(BranchBarrier::new("gid", "branch", "cancel"))
            .await
            .expect("barrier");

        assert_eq!(decision, BarrierDecision::SkipNullCompensation);
        let late_try = store
            .barrier(BranchBarrier::new("gid", "branch", "try"))
            .await
            .expect("late try barrier");
        assert_eq!(late_try, BarrierDecision::SkipCancelledTry);
    }

    #[tokio::test]
    async fn failed_branch_gets_retry_schedule() {
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            FailingOnceInvoker {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            DtmOptions {
                max_attempts: 5,
                retry_backoff_millis: 1,
                max_retry_backoff_millis: 10,
                branch_call_timeout_millis: 5_000,
                transaction_timeout_millis: 60_000,
            },
        );
        let tx = Transaction::tcc(
            "gid-retry",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        );
        dtm.submit(tx).await.expect("submit");

        let prepared = dtm.prepare_tcc("gid-retry").await.expect("prepare");

        assert_eq!(prepared.status, TransactionStatus::Trying);
        assert_eq!(prepared.branches[0].status, BranchStatus::Failed);
        assert_eq!(
            prepared.branches[0].last_error.as_deref(),
            Some("branch_call_failed")
        );
        assert!(prepared.branches[0].next_retry_millis.is_some());

        tokio::time::sleep(Duration::from_millis(2)).await;
        let recovered = dtm.tick_recover_once().await.expect("recover retry");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Prepared);
    }

    #[tokio::test]
    async fn exhausted_try_clears_retry_schedule_after_null_cancel() {
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            FailingOnceInvoker {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            DtmOptions {
                max_attempts: 1,
                retry_backoff_millis: 1,
                max_retry_backoff_millis: 10,
                ..DtmOptions::default()
            },
        );
        dtm.submit(Transaction::tcc(
            "gid-exhausted",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit");

        let aborting = dtm.prepare_tcc("gid-exhausted").await.expect("prepare");
        assert_eq!(aborting.status, TransactionStatus::Aborting);
        tokio::time::sleep(Duration::from_millis(2)).await;
        let aborted = dtm.tick_recover_once().await.expect("cancel");
        assert_eq!(aborted[0].status, TransactionStatus::Aborted);
        assert_eq!(aborted[0].branches[0].next_retry_millis, None);
    }

    #[tokio::test]
    async fn failed_confirm_releases_barrier_for_recovery_retry() {
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            FailOnCallInvoker {
                calls: Arc::new(AtomicUsize::new(0)),
                fail_on: 1,
            },
            DtmOptions {
                retry_backoff_millis: 1,
                max_retry_backoff_millis: 10,
                ..DtmOptions::default()
            },
        );
        dtm.submit(Transaction::tcc(
            "gid-confirm-retry",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit");
        dtm.prepare_tcc("gid-confirm-retry").await.expect("prepare");

        let pending = dtm
            .confirm_tcc("gid-confirm-retry")
            .await
            .expect("failed confirm state");
        assert_eq!(pending.status, TransactionStatus::Succeeding);
        assert_eq!(pending.branches[0].status, BranchStatus::Failed);

        tokio::time::sleep(Duration::from_millis(2)).await;
        let recovered = dtm.tick_recover_once().await.expect("confirm retry");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn recovery_cancels_expired_tcc() {
        let store = InMemoryTransactionStore::new();
        let dtm = Dtm::with_options(
            store,
            NoopBranchInvoker,
            DtmOptions {
                max_attempts: 5,
                retry_backoff_millis: 1,
                max_retry_backoff_millis: 10,
                branch_call_timeout_millis: 5_000,
                transaction_timeout_millis: 0,
            },
        );
        let mut tx = Transaction::tcc(
            "gid-timeout",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        );
        tx.metadata = BTreeMap::new();
        dtm.submit(tx).await.expect("submit");

        let recovered = dtm.tick_recover_once().await.expect("recover");

        assert_eq!(recovered[0].status, TransactionStatus::Aborted);
    }

    #[tokio::test]
    async fn manual_recovery_advances_one_safe_transition() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::tcc(
            "gid-manual-recover",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit");

        let prepared = dtm.recover("gid-manual-recover").await.expect("prepare");
        assert_eq!(prepared.status, TransactionStatus::Prepared);
        let succeeded = dtm.recover("gid-manual-recover").await.expect("confirm");
        assert_eq!(succeeded.status, TransactionStatus::Succeeded);
        let unchanged = dtm
            .recover("gid-manual-recover")
            .await
            .expect("terminal transaction");
        assert_eq!(unchanged.status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn scheduled_workflow_submit_is_advanced_by_recovery() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::workflow("gid-workflow-async", Vec::new()))
            .await
            .expect("submit");
        dtm.prepare_workflow("gid-workflow-async")
            .await
            .expect("prepare");

        let scheduled = dtm
            .schedule_submit("gid-workflow-async")
            .await
            .expect("schedule submit");
        assert_eq!(scheduled.status, TransactionStatus::Submitted);

        let recovered = dtm.tick_recover_once().await.expect("recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn scheduled_abort_is_persisted_before_compensation() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::saga("gid-saga-async-abort", Vec::new()))
            .await
            .expect("submit");

        let scheduled = dtm
            .schedule_abort("gid-saga-async-abort")
            .await
            .expect("schedule abort");
        assert_eq!(scheduled.status, TransactionStatus::Aborting);

        let persisted = dtm
            .store()
            .get_transaction("gid-saga-async-abort")
            .await
            .expect("get transaction")
            .expect("transaction");
        assert_eq!(persisted.status, TransactionStatus::Aborting);

        let recovered = dtm.tick_recover_once().await.expect("recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, TransactionStatus::Aborted);
    }

    #[tokio::test]
    async fn scheduled_tcc_submit_requires_prepare() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::tcc("gid-tcc-not-prepared", Vec::new()))
            .await
            .expect("submit");

        assert!(dtm
            .schedule_submit("gid-tcc-not-prepared")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn callback_workflow_preserves_composite_progress_and_binary_data() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut workflow = Transaction::workflow("gid-callback-workflow", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        dtm.submit(workflow).await.expect("submit callback workflow");
        dtm.prepare_workflow("gid-callback-workflow")
            .await
            .expect("prepare callback workflow");

        let action = WorkflowProgress {
            branch_id: "01".to_owned(),
            operation: "action".to_owned(),
            status: WorkflowProgressStatus::Succeeded,
            data: vec![0, 159, 146, 150, 255],
        };
        dtm.record_workflow_progress("gid-callback-workflow", action.clone())
            .await
            .expect("record action");
        dtm.record_workflow_progress("gid-callback-workflow", action.clone())
            .await
            .expect("idempotent action");
        dtm.record_workflow_progress(
            "gid-callback-workflow",
            WorkflowProgress {
                branch_id: "01".to_owned(),
                operation: "commit".to_owned(),
                status: WorkflowProgressStatus::Succeeded,
                data: vec![1, 2, 3],
            },
        )
        .await
        .expect("same branch with another operation");

        let prepared = dtm
            .prepare_workflow("gid-callback-workflow")
            .await
            .expect("query progress");
        assert_eq!(prepared.workflow_progresses.len(), 2);
        assert_eq!(prepared.workflow_progresses[0], action);
        assert!(dtm
            .record_workflow_progress(
                "gid-callback-workflow",
                WorkflowProgress {
                    branch_id: "01".to_owned(),
                    operation: "action".to_owned(),
                    status: WorkflowProgressStatus::Failed,
                    data: b"conflict".to_vec(),
                },
            )
            .await
            .is_err());
    }

    #[test]
    fn workflow_progress_json_preserves_binary_data_as_base64() {
        let progress = WorkflowProgress {
            branch_id: "01".to_owned(),
            operation: "action".to_owned(),
            status: WorkflowProgressStatus::Succeeded,
            data: vec![0, 255, 1],
        };

        let encoded = serde_json::to_value(&progress).expect("serialize progress");
        assert_eq!(encoded["data"], "AP8B");
        let decoded: WorkflowProgress =
            serde_json::from_value(encoded).expect("deserialize progress");
        assert_eq!(decoded, progress);
    }

    #[tokio::test]
    async fn callback_workflow_uses_only_explicit_timeout_and_fails_closed() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut workflow = Transaction::workflow("gid-callback-timeout", Vec::new());
        workflow.created_at_millis = 0;
        workflow.updated_at_millis = 0;
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        let submitted = dtm
            .submit(workflow)
            .await
            .expect("submit callback workflow");
        assert_eq!(submitted.timeout_millis, None);

        let mut expiring = Transaction::workflow("gid-callback-expiring", Vec::new());
        expiring.created_at_millis = 0;
        expiring.updated_at_millis = 0;
        expiring.timeout_millis = Some(1);
        expiring.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        dtm.submit(expiring)
            .await
            .expect("submit expiring callback workflow");
        dtm.prepare_workflow("gid-callback-expiring")
            .await
            .expect("prepare expiring callback workflow");
        let failed = dtm
            .recover("gid-callback-expiring")
            .await
            .expect("expire callback workflow");
        assert_eq!(failed.status, TransactionStatus::Failed);
        assert_eq!(
            failed.metadata["rollback_reason"],
            "workflow callback timed out"
        );
    }

    #[tokio::test]
    async fn callback_workflow_completion_is_terminal_and_idempotent() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut workflow = Transaction::workflow("gid-callback-complete", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        dtm.submit(workflow).await.expect("submit callback workflow");
        dtm.prepare_workflow("gid-callback-complete")
            .await
            .expect("prepare callback workflow");

        let completed = dtm
            .finish_callback_workflow(
                "gid-callback-complete",
                TransactionStatus::Succeeded,
                None,
                Some("cmVzdWx0".to_owned()),
            )
            .await
            .expect("finish callback workflow");
        assert_eq!(completed.status, TransactionStatus::Succeeded);
        assert_eq!(
            completed.metadata["dtm.workflow.result"],
            "cmVzdWx0"
        );

        let queried = dtm
            .prepare_workflow("gid-callback-complete")
            .await
            .expect("query terminal workflow");
        assert_eq!(queried.status, TransactionStatus::Succeeded);
        dtm.finish_callback_workflow(
            "gid-callback-complete",
            TransactionStatus::Succeeded,
            None,
            Some("cmVzdWx0".to_owned()),
        )
        .await
        .expect("idempotent completion");
        assert!(dtm
            .finish_callback_workflow(
                "gid-callback-complete",
                TransactionStatus::Failed,
                Some("late failure".to_owned()),
                None,
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn recovery_lease_allows_one_owner_until_expired() {
        let store = InMemoryTransactionStore::new();

        assert!(store
            .try_acquire_recovery_lease("recovery", "worker-a", 10_000)
            .await
            .expect("lease"));
        assert!(!store
            .try_acquire_recovery_lease("recovery", "worker-b", 10_000)
            .await
            .expect("lease"));
        assert!(store
            .try_acquire_recovery_lease("recovery", "worker-a", 10_000)
            .await
            .expect("renew"));
    }

    #[tokio::test]
    async fn sqlite_store_persists_transactions_and_barriers() {
        let store = SqliteTransactionStore::connect("sqlite::memory:")
            .await
            .expect("connect");
        let tx = Transaction::tcc(
            "gid-sqlite",
            vec![Branch::tcc_try(
                "b1",
                "try",
                "confirm",
                "cancel",
                serde_json::json!({}),
            )],
        );

        store.insert_transaction(tx.clone()).await.expect("insert");
        assert_eq!(
            store
                .get_transaction("gid-sqlite")
                .await
                .expect("get")
                .unwrap()
                .gid,
            tx.gid
        );
        assert_eq!(
            store
                .barrier(BranchBarrier::new("gid-sqlite", "b1", "try"))
                .await
                .expect("barrier"),
            BarrierDecision::Execute
        );
        assert_eq!(
            store
                .barrier(BranchBarrier::new("gid-sqlite", "b1", "try"))
                .await
                .expect("barrier"),
            BarrierDecision::SkipDuplicate
        );
    }

    #[tokio::test]
    async fn sqlite_barrier_has_exactly_one_concurrent_winner() {
        let store = SqliteTransactionStore::connect("sqlite::memory:")
            .await
            .expect("connect");
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .barrier(BranchBarrier::new("gid-race", "branch", "confirm"))
                    .await
                    .expect("barrier")
            }));
        }

        let mut execute = 0;
        let mut duplicate = 0;
        for task in tasks {
            match task.await.expect("join") {
                BarrierDecision::Execute => execute += 1,
                BarrierDecision::SkipDuplicate => duplicate += 1,
                BarrierDecision::SkipNullCompensation => panic!("unexpected null compensation"),
                BarrierDecision::SkipCancelledTry => panic!("unexpected cancelled try"),
            }
        }
        assert_eq!(execute, 1);
        assert_eq!(duplicate, 31);
    }

    #[tokio::test]
    async fn sqlite_recovery_lease_has_exactly_one_concurrent_owner() {
        let store = SqliteTransactionStore::connect("sqlite::memory:")
            .await
            .expect("connect");
        let mut tasks = Vec::new();
        for index in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .try_acquire_recovery_lease("recovery-race", &format!("worker-{index}"), 10_000)
                    .await
                    .expect("lease")
            }));
        }

        let mut acquired = 0;
        for task in tasks {
            acquired += usize::from(task.await.expect("join"));
        }
        assert_eq!(acquired, 1);
    }

    #[tokio::test]
    async fn concurrent_dynamic_branch_registration_does_not_lose_updates() {
        let store = InMemoryTransactionStore::new();
        store
            .insert_transaction(Transaction::new(
                "gid-register-race",
                TransactionKind::Tcc,
                Vec::new(),
            ))
            .await
            .expect("insert transaction");

        let mut tasks = Vec::new();
        for index in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .register_branch(
                        "gid-register-race",
                        Branch::tcc_try(
                            format!("branch-{index}"),
                            format!("http://branch-{index}/try"),
                            format!("http://branch-{index}/confirm"),
                            format!("http://branch-{index}/cancel"),
                            serde_json::json!({}),
                        ),
                    )
                    .await
                    .expect("register branch");
            }));
        }
        for task in tasks {
            task.await.expect("join");
        }

        let tx = store
            .get_transaction("gid-register-race")
            .await
            .expect("read transaction")
            .expect("transaction exists");
        assert_eq!(tx.branches.len(), 32);
    }

    #[tokio::test]
    async fn sqlite_null_compensation_blocks_a_late_try() {
        let store = SqliteTransactionStore::connect("sqlite::memory:")
            .await
            .expect("connect");
        let cancel = store
            .barrier(BranchBarrier::new("gid-null", "branch", "cancel"))
            .await
            .expect("cancel barrier");
        assert_eq!(cancel, BarrierDecision::SkipNullCompensation);

        let late_try = store
            .barrier(BranchBarrier::new("gid-null", "branch", "try"))
            .await
            .expect("try barrier");
        assert_eq!(late_try, BarrierDecision::SkipCancelledTry);
    }

    #[tokio::test]
    async fn prepared_message_dispatches_exactly_once() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::message(
            "gid-message",
            vec![Branch::message(
                "publish",
                "http://events/publish",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit message");
        assert_eq!(
            dtm.prepare_message("gid-message")
                .await
                .expect("prepare")
                .status,
            TransactionStatus::Prepared
        );
        assert_eq!(
            dtm.dispatch_message("gid-message")
                .await
                .expect("dispatch")
                .status,
            TransactionStatus::Succeeded
        );
        assert_eq!(
            dtm.dispatch_message("gid-message")
                .await
                .expect("idempotent dispatch")
                .status,
            TransactionStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn topic_subscriptions_expand_message_branches() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.subscribe_topic("orders", "http://billing/orders", "billing")
            .await
            .expect("subscribe billing");
        dtm.subscribe_topic("orders", "http://warehouse/orders", "warehouse")
            .await
            .expect("subscribe warehouse");

        let transaction = dtm
            .submit(Transaction::message(
                "gid-topic-message",
                vec![Branch::message(
                    "publish",
                    "topic://orders",
                    serde_json::json!({"order_id": "42"}),
                )],
            ))
            .await
            .expect("submit topic message");

        assert_eq!(transaction.branches.len(), 2);
        assert_eq!(transaction.branches[0].id, "publish-01");
        assert_eq!(transaction.branches[0].action, "http://billing/orders");
        assert_eq!(transaction.branches[1].id, "publish-02");
        assert_eq!(
            transaction.branches[1].action,
            "http://warehouse/orders"
        );
    }

    #[tokio::test]
    async fn topic_subscription_updates_are_versioned() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let first = dtm
            .subscribe_topic("events", "http://one/events", "one")
            .await
            .expect("first subscription");
        assert_eq!(first.version, 1);
        let second = dtm
            .subscribe_topic("events", "http://two/events", "two")
            .await
            .expect("second subscription");
        assert_eq!(second.version, 2);
        assert!(dtm
            .subscribe_topic("events", "http://two/events", "duplicate")
            .await
            .is_err());
        let remaining = dtm
            .unsubscribe_topic("events", "http://one/events")
            .await
            .expect("unsubscribe");
        assert_eq!(remaining.version, 3);
        assert_eq!(remaining.subscribers.len(), 1);
        assert!(dtm.delete_topic("events").await.expect("delete topic"));
        assert!(dtm.get_topic("events").await.expect("read topic").is_none());
    }

    #[tokio::test]
    async fn concurrent_topic_subscriptions_do_not_lose_updates() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut tasks = Vec::new();
        for index in 0..32 {
            let dtm = dtm.clone();
            tasks.push(tokio::spawn(async move {
                dtm.subscribe_topic(
                    "fanout",
                    &format!("http://subscriber-{index}/events"),
                    &format!("subscriber {index}"),
                )
                .await
                .expect("subscribe");
            }));
        }
        for task in tasks {
            task.await.expect("join");
        }

        let topic = dtm
            .get_topic("fanout")
            .await
            .expect("read topic")
            .expect("topic exists");
        assert_eq!(topic.version, 32);
        assert_eq!(topic.subscribers.len(), 32);
    }

    #[tokio::test]
    async fn xa_prepares_and_commits_registered_branches() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::xa(
            "gid-xa",
            vec![Branch::xa(
                "account",
                "http://account/commit",
                "http://account/rollback",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit XA");
        assert_eq!(
            dtm.prepare_xa("gid-xa").await.expect("prepare").status,
            TransactionStatus::Prepared
        );
        assert_eq!(
            dtm.commit_xa("gid-xa").await.expect("commit").status,
            TransactionStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn workflow_respects_dependencies() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::workflow(
            "gid-workflow",
            vec![
                Branch::workflow(
                    "reserve",
                    "http://inventory/reserve",
                    "http://inventory/release",
                    Vec::new(),
                    serde_json::json!({}),
                ),
                Branch::workflow(
                    "charge",
                    "http://account/charge",
                    "http://account/refund",
                    vec!["reserve".to_string()],
                    serde_json::json!({}),
                ),
            ],
        ))
        .await
        .expect("submit workflow");
        let completed = dtm.start_workflow("gid-workflow").await.expect("workflow");
        assert_eq!(completed.status, TransactionStatus::Succeeded);
        assert!(completed
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Succeeded));
    }

    #[test]
    fn branch_url_policy_requires_an_exact_origin() {
        let policy = BranchUrlPolicy::from_allowed_origins([
            "https://inventory.example.com",
            "http://account:8080",
        ])
        .expect("policy");

        policy
            .validate("https://inventory.example.com/v1/reserve?mode=sync")
            .expect("same origin");
        policy
            .validate("http://account:8080/confirm")
            .expect("same host and port");
        assert!(policy
            .validate("http://inventory.example.com/v1/reserve")
            .is_err());
        assert!(policy.validate("http://account/confirm").is_err());
        assert!(policy.validate("https://metadata.internal/latest").is_err());
        assert!(policy
            .validate("https://inventory.example.com/reserve#ignored")
            .is_err());
        assert!(policy
            .validate("https://user@inventory.example.com/reserve")
            .is_err());
    }

    #[test]
    fn branch_url_policy_rejects_non_origin_configuration() {
        assert!(
            BranchUrlPolicy::from_allowed_origins(["https://inventory.example.com/api"]).is_err()
        );
        assert!(BranchUrlPolicy::from_allowed_origins(["file:///tmp/action"]).is_err());
        assert!(BranchUrlPolicy::from_allowed_origins(["https://user@example.com"]).is_err());
    }
}
