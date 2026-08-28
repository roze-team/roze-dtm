use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, SqlitePool};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionKind {
    Saga,
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
        }
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
            metadata: BTreeMap::new(),
        }
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
    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>>;
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

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        (**self).list_transactions().await
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
        self.url_policy.validate(url)?;
        let response = self.client.post(url).json(payload).send().await?;
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

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        Ok(self.txs.read().await.values().cloned().collect())
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

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let rows =
            sqlx::query("SELECT payload FROM roze_dtm_transactions ORDER BY updated_at_millis ASC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(row.get::<&str, _>("payload")).map_err(Into::into))
            .collect()
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
        let mut tx = self.pool.begin().await?;
        let current: Option<(String, i64)> = sqlx::query_as(
            "SELECT owner, expires_at_millis FROM roze_dtm_recovery_leases WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((current_owner, current_expires_at)) = current {
            if current_owner != owner && current_expires_at as u64 > now {
                tx.commit().await?;
                return Ok(false);
            }
        }
        sqlx::query(
            r#"
            INSERT INTO roze_dtm_recovery_leases (name, owner, expires_at_millis)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                owner = excluded.owner,
                expires_at_millis = excluded.expires_at_millis
            "#,
        )
        .bind(name)
        .bind(owner)
        .bind(expires_at as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
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

    pub async fn submit(&self, tx: Transaction) -> anyhow::Result<Transaction> {
        let mut tx = tx;
        tx.timeout_millis
            .get_or_insert(self.options.transaction_timeout_millis);
        self.store.insert_transaction(tx.clone()).await?;
        Ok(tx)
    }

    pub async fn submit_default_tcc(
        &self,
        gid: impl Into<String>,
        branches: Vec<Branch>,
    ) -> anyhow::Result<Transaction> {
        self.submit(Transaction::default_tcc(gid, branches)).await
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
        ensure_status(&tx, &[TransactionStatus::Submitted])?;
        tx.status = TransactionStatus::Succeeding;
        self.store.update_transaction(tx.clone()).await?;
        for index in 0..tx.branches.len() {
            let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "action");
            if self.store.barrier(barrier).await? != BarrierDecision::Execute {
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
                self.invoke_branch(branch, &action).await
            };
            match action_result {
                Ok(()) => {
                    let branch = &mut tx.branches[index];
                    branch.status = BranchStatus::Succeeded;
                    branch.next_retry_millis = None;
                }
                Err(_) => {
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
            &[TransactionStatus::Submitted, TransactionStatus::Aborting],
        )?;
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
                        if self.invoke_url(branch, &compensate).await.is_err() {
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
                    match self.invoke_branch(branch, &action).await {
                        Ok(()) => {
                            branch.status = BranchStatus::Succeeded;
                            branch.next_retry_millis = None;
                        }
                        Err(_) => {
                            branch.status = BranchStatus::Failed;
                            self.store.release_barrier(&barrier).await?;
                            if branch.attempts >= self.options.max_attempts {
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
        tx.status = TransactionStatus::Succeeding;
        for branch in &mut tx.branches {
            let barrier = BranchBarrier::new(&tx.gid, &branch.id, "confirm");
            let decision = self.store.barrier(barrier.clone()).await?;
            if decision == BarrierDecision::Execute {
                let confirm = branch.confirm.clone().ok_or_else(|| {
                    anyhow::anyhow!("missing confirm action for branch {}", branch.id)
                })?;
                branch.attempts = branch.attempts.saturating_add(1);
                match self.invoke_url(branch, &confirm).await {
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
                    match self.invoke_url(branch, &cancel).await {
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

    pub async fn list(&self) -> anyhow::Result<Vec<Transaction>> {
        self.store.list_transactions().await
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
            (TransactionKind::Saga, TransactionStatus::Submitted) => self.start_saga(gid).await,
            (TransactionKind::Saga, TransactionStatus::Aborting) => self.abort_saga(gid).await,
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
                (TransactionKind::Saga, TransactionStatus::Submitted) => {
                    self.start_saga(&tx.gid).await?
                }
                (TransactionKind::Saga, TransactionStatus::Aborting) => {
                    self.abort_saga(&tx.gid).await?
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

    async fn invoke_branch(&self, branch: &mut Branch, url: &str) -> anyhow::Result<()> {
        match self.invoker.invoke(url, &branch.payload).await {
            Ok(()) => Ok(()),
            Err(_) => {
                record_branch_failure(
                    branch,
                    "branch_call_failed".to_string(),
                    self.options.retry_backoff_millis,
                    self.options.max_retry_backoff_millis,
                );
                Err(anyhow::anyhow!("branch call failed"))
            }
        }
    }

    async fn invoke_url(&self, branch: &mut Branch, url: &str) -> anyhow::Result<()> {
        self.invoke_branch(branch, url).await
    }
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
            Arc,
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
