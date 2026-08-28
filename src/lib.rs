use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use async_trait::async_trait;
use base64::Engine as _;
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
pub mod redis_store;
pub mod xa;

pub use kv::{KvEntry, Topic, TopicSubscriber, TOPICS_CATEGORY};
pub use redis_store::{
    validate_redis_namespace, RedisTransactionStore, DEFAULT_REDIS_OPERATION_TIMEOUT,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowCallbackProtocol {
    Http,
    JsonRpc,
    Grpc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCallbackRequest {
    pub gid: String,
    pub url: String,
    pub operation: String,
    pub data: Vec<u8>,
    pub protocol: WorkflowCallbackProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowCallbackResult {
    Completed,
    Failed { reason: Option<String> },
    Ongoing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRecoveryDelay {
    pub attempted_at_millis: u64,
    pub retry_interval_millis: u64,
    pub max_retry_interval_millis: u64,
    pub backoff: bool,
    pub outcome: String,
}

impl WorkflowRecoveryDelay {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.retry_interval_millis > 0
                && self.max_retry_interval_millis >= self.retry_interval_millis
                && self.max_retry_interval_millis <= 86_400_000,
            "invalid workflow callback retry interval"
        );
        anyhow::ensure!(
            !self.outcome.is_empty()
                && self.outcome.len() <= 64
                && self
                    .outcome
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "invalid workflow callback outcome"
        );
        Ok(())
    }
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

    pub fn saga_with_dependencies(
        id: impl Into<String>,
        action: impl Into<String>,
        compensate: impl Into<String>,
        dependencies: Vec<String>,
        payload: serde_json::Value,
    ) -> Self {
        let mut branch = Self::saga(id, action, compensate, payload);
        branch.dependencies = dependencies;
        branch
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
    #[serde(default)]
    pub revision: u64,
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

    pub fn concurrent_saga(gid: impl Into<String>, branches: Vec<Branch>) -> Self {
        let mut transaction = Self::saga(gid, branches);
        transaction.options.concurrent = true;
        transaction
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
            revision: 1,
            timeout_millis: None,
            options: TransactionOptions::default(),
            workflow_progresses: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn callback_workflow_request(&self) -> anyhow::Result<WorkflowCallbackRequest> {
        workflow_callback_request(self)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionOptions {
    #[serde(default)]
    pub wait_result: bool,
    /// Enables concurrent execution for Saga or Message branches.
    ///
    /// Concurrent Saga branches use their dependency DAG. Concurrent Message
    /// branches are independent and are delivered in one batch.
    #[serde(default)]
    pub concurrent: bool,
    /// Defers Message branch delivery from the transaction creation time.
    ///
    /// This is the native millisecond form of dtm-labs Message
    /// `custom_data.delay`, whose compatibility wire unit is seconds.
    #[serde(default)]
    pub delay_millis: Option<u64>,
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
        if let Some(value) = self.delay_millis {
            anyhow::ensure!(
                (1..=31_536_000_000).contains(&value),
                "message delay must be between 1 and 31536000000 milliseconds"
            );
        }
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

/// Identifies one recovery-lease epoch.
///
/// Stores with native fencing issue a new epoch whenever an expired lease is
/// acquired, even when the owner string is unchanged, and validate all three
/// fields atomically with the protected write. Other stores retain their
/// existing lease behavior through the default trait methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryLeaseFence {
    pub name: String,
    pub owner: String,
    pub epoch: u64,
}

#[async_trait]
pub trait TransactionStore: Send + Sync + 'static {
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()>;
    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>>;
    async fn update_transaction(&self, tx: Transaction) -> anyhow::Result<()>;
    async fn update_transaction_fenced(
        &self,
        tx: Transaction,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        self.update_transaction(tx).await
    }
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
    async fn record_workflow_progress_fenced(
        &self,
        gid: &str,
        progress: WorkflowProgress,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        self.record_workflow_progress(gid, progress).await
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
    async fn finish_workflow_fenced(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        self.finish_workflow(gid, status, rollback_reason, result)
            .await
    }
    async fn defer_workflow_recovery(
        &self,
        _gid: &str,
        _delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        anyhow::bail!("workflow recovery persistence is not supported by this store")
    }
    async fn defer_workflow_recovery_fenced(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        self.defer_workflow_recovery(gid, delay).await
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
    async fn barrier_fenced(
        &self,
        barrier: BranchBarrier,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<BarrierDecision> {
        self.barrier(barrier).await
    }
    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()>;
    async fn release_barrier_fenced(
        &self,
        barrier: &BranchBarrier,
        _fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        self.release_barrier(barrier).await
    }
    async fn try_acquire_recovery_lease(
        &self,
        _name: &str,
        _owner: &str,
        _ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
    async fn acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<Option<RecoveryLeaseFence>> {
        Ok(self
            .try_acquire_recovery_lease(name, owner, ttl_millis)
            .await?
            .then(|| RecoveryLeaseFence {
                name: name.to_owned(),
                owner: owner.to_owned(),
                epoch: 1,
            }))
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

    async fn update_transaction_fenced(
        &self,
        tx: Transaction,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        (**self).update_transaction_fenced(tx, fence).await
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

    async fn record_workflow_progress_fenced(
        &self,
        gid: &str,
        progress: WorkflowProgress,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        (**self)
            .record_workflow_progress_fenced(gid, progress, fence)
            .await
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

    async fn finish_workflow_fenced(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        (**self)
            .finish_workflow_fenced(gid, status, rollback_reason, result, fence)
            .await
    }

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        (**self).defer_workflow_recovery(gid, delay).await
    }

    async fn defer_workflow_recovery_fenced(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        (**self)
            .defer_workflow_recovery_fenced(gid, delay, fence)
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

    async fn barrier_fenced(
        &self,
        barrier: BranchBarrier,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<BarrierDecision> {
        (**self).barrier_fenced(barrier, fence).await
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        (**self).release_barrier(barrier).await
    }

    async fn release_barrier_fenced(
        &self,
        barrier: &BranchBarrier,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        (**self).release_barrier_fenced(barrier, fence).await
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

    async fn acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<Option<RecoveryLeaseFence>> {
        (**self)
            .acquire_recovery_lease(name, owner, ttl_millis)
            .await
    }
}

#[derive(Debug, Clone)]
struct RecoveryFencedStore<S> {
    inner: S,
    fence: RecoveryLeaseFence,
}

impl<S> RecoveryFencedStore<S> {
    fn new(inner: S, fence: RecoveryLeaseFence) -> Self {
        Self { inner, fence }
    }
}

#[async_trait]
impl<S> TransactionStore for RecoveryFencedStore<S>
where
    S: TransactionStore,
{
    async fn insert_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        self.inner.insert_transaction(tx).await
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        self.inner.get_transaction(gid).await
    }

    async fn update_transaction(&self, tx: Transaction) -> anyhow::Result<()> {
        self.inner.update_transaction_fenced(tx, &self.fence).await
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        self.inner.register_branch(gid, branch).await
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        self.inner
            .record_workflow_progress_fenced(gid, progress, &self.fence)
            .await
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        self.inner
            .finish_workflow_fenced(gid, status, rollback_reason, result, &self.fence)
            .await
    }

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        self.inner
            .defer_workflow_recovery_fenced(gid, delay, &self.fence)
            .await
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        self.inner.list_transactions().await
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        self.inner.get_kv(category, key).await
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        self.inner.list_kv(category, key, offset, limit).await
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        self.inner.create_kv(entry).await
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        self.inner.update_kv(entry, expected_version).await
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        self.inner.delete_kv(category, key).await
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        self.inner.barrier_fenced(barrier, &self.fence).await
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        self.inner
            .release_barrier_fenced(barrier, &self.fence)
            .await
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        self.inner
            .try_acquire_recovery_lease(name, owner, ttl_millis)
            .await
    }

    async fn acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<Option<RecoveryLeaseFence>> {
        self.inner
            .acquire_recovery_lease(name, owner, ttl_millis)
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

    async fn query_workflow_callback(
        &self,
        _request: &WorkflowCallbackRequest,
        _options: &TransactionOptions,
    ) -> anyhow::Result<WorkflowCallbackResult> {
        anyhow::bail!("workflow callback invocation is not supported by this invoker")
    }

    async fn notify_branch_failure(&self, _alert: &BranchFailureAlert) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchFailureAlert {
    pub gid: String,
    pub status: String,
    pub branch: String,
    pub error: String,
    pub retry_count: u32,
}

#[derive(Clone, PartialEq, Eq)]
pub struct AlertWebhookConfig {
    pub url: String,
    pub retry_limit: u32,
    pub timeout: Duration,
}

impl std::fmt::Debug for AlertWebhookConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlertWebhookConfig")
            .field("url", &"[REDACTED]")
            .field("retry_limit", &self.retry_limit)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl AlertWebhookConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        parse_branch_url(&self.url).context("invalid alert webhook URL")?;
        anyhow::ensure!(
            (1..=10_000).contains(&self.retry_limit),
            "alert webhook retry limit must be between 1 and 10000"
        );
        anyhow::ensure!(
            (Duration::from_millis(1)..=Duration::from_secs(120)).contains(&self.timeout),
            "alert webhook timeout must be between 1 and 120000 milliseconds"
        );
        Ok(())
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

#[derive(Clone)]
pub struct HttpBranchInvoker {
    client: reqwest::Client,
    url_policy: BranchUrlPolicy,
    default_timeout: Option<Duration>,
    alert_webhook: Option<AlertWebhook>,
}

#[derive(Clone)]
struct AlertWebhook {
    client: reqwest::Client,
    url: String,
    retry_limit: u32,
}

impl std::fmt::Debug for HttpBranchInvoker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpBranchInvoker")
            .field("url_policy", &self.url_policy)
            .field("default_timeout", &self.default_timeout)
            .field("alert_webhook_configured", &self.alert_webhook.is_some())
            .finish_non_exhaustive()
    }
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

    pub fn validate_callback(&self, value: &str) -> anyhow::Result<()> {
        if value.starts_with("http://") || value.starts_with("https://") {
            return self.validate(value);
        }
        let target = parse_grpc_callback_target(value)?;
        self.validate(&target.endpoint)
    }
}

impl HttpBranchInvoker {
    pub fn new() -> Self {
        Self {
            client: branch_http_client(None).expect("default HTTP client configuration is valid"),
            url_policy: BranchUrlPolicy::allow_all(),
            default_timeout: None,
            alert_webhook: None,
        }
    }

    pub fn with_timeout(timeout: Duration) -> anyhow::Result<Self> {
        Self::with_timeout_and_policy(timeout, BranchUrlPolicy::allow_all())
    }

    pub fn with_timeout_and_policy(
        timeout: Duration,
        url_policy: BranchUrlPolicy,
    ) -> anyhow::Result<Self> {
        Self::with_timeout_policy_and_alert(timeout, url_policy, None)
    }

    pub fn with_timeout_policy_and_alert(
        timeout: Duration,
        url_policy: BranchUrlPolicy,
        alert_webhook: Option<AlertWebhookConfig>,
    ) -> anyhow::Result<Self> {
        let alert_webhook = alert_webhook
            .map(|config| {
                config.validate()?;
                Ok::<AlertWebhook, anyhow::Error>(AlertWebhook {
                    client: branch_http_client(Some(config.timeout))?,
                    url: config.url,
                    retry_limit: config.retry_limit,
                })
            })
            .transpose()?;
        Ok(Self {
            client: branch_http_client(Some(timeout))?,
            url_policy,
            default_timeout: Some(timeout),
            alert_webhook,
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

    async fn query_workflow_callback(
        &self,
        request: &WorkflowCallbackRequest,
        options: &TransactionOptions,
    ) -> anyhow::Result<WorkflowCallbackResult> {
        options.validate()?;
        match request.protocol {
            WorkflowCallbackProtocol::Http => {
                self.query_http_workflow_callback(request, options, false)
                    .await
            }
            WorkflowCallbackProtocol::JsonRpc => {
                self.query_http_workflow_callback(request, options, true)
                    .await
            }
            WorkflowCallbackProtocol::Grpc => {
                self.query_grpc_workflow_callback(request, options).await
            }
        }
    }

    async fn notify_branch_failure(&self, alert: &BranchFailureAlert) -> anyhow::Result<()> {
        let Some(webhook) = &self.alert_webhook else {
            return Ok(());
        };
        if !branch_alert_due(alert.retry_count, webhook.retry_limit) {
            return Ok(());
        }
        let response = webhook.client.post(&webhook.url).json(alert).send().await?;
        anyhow::ensure!(
            response.status().is_success(),
            "alert webhook rejected notification"
        );
        Ok(())
    }
}

const fn branch_alert_due(retry_count: u32, retry_limit: u32) -> bool {
    retry_count >= retry_limit
}

impl HttpBranchInvoker {
    async fn query_http_workflow_callback(
        &self,
        callback: &WorkflowCallbackRequest,
        options: &TransactionOptions,
        json_rpc: bool,
    ) -> anyhow::Result<WorkflowCallbackResult> {
        self.url_policy.validate(&callback.url)?;
        let mut url = reqwest::Url::parse(&callback.url)?;
        let mut request = if json_rpc {
            let method = url
                .query_pairs()
                .find_map(|(name, value)| (name == "method").then(|| value.into_owned()))
                .filter(|method| !method.is_empty())
                .context("JSON-RPC callback URL requires a method query parameter")?;
            let mut params = if callback.data.is_empty() {
                serde_json::Map::new()
            } else {
                serde_json::from_slice::<serde_json::Value>(&callback.data)?
                    .as_object()
                    .cloned()
                    .context("JSON-RPC callback data must be a JSON object")?
            };
            add_workflow_callback_params(&mut params, callback);
            self.client.post(url).json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": &callback.gid,
                "method": method,
                "params": params,
            }))
        } else {
            url.query_pairs_mut()
                .append_pair("gid", &callback.gid)
                .append_pair("trans_type", "workflow")
                .append_pair("branch_id", "00")
                .append_pair("op", &callback.operation);
            if callback.data.is_empty() {
                self.client.get(url)
            } else {
                self.client
                    .post(url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(callback.data.clone())
            }
        };
        request = apply_http_transaction_options(request, options);
        let response = request.send().await?;
        match response.status().as_u16() {
            200 if json_rpc => decode_json_rpc_callback_response(response).await,
            200 => Ok(WorkflowCallbackResult::Completed),
            409 => Ok(WorkflowCallbackResult::Failed {
                reason: limited_response_text(response, 4_096).await?,
            }),
            425 => Ok(WorkflowCallbackResult::Ongoing),
            status => anyhow::bail!("workflow callback returned unexpected HTTP status {status}"),
        }
    }

    async fn query_grpc_workflow_callback(
        &self,
        callback: &WorkflowCallbackRequest,
        options: &TransactionOptions,
    ) -> anyhow::Result<WorkflowCallbackResult> {
        let target = parse_grpc_callback_target(&callback.url)?;
        self.url_policy.validate(&target.endpoint)?;
        let timeout = options
            .request_timeout_millis
            .map(Duration::from_millis)
            .or(self.default_timeout);
        let mut endpoint = tonic::transport::Endpoint::from_shared(target.endpoint)?;
        if let Some(timeout) = timeout {
            endpoint = endpoint.connect_timeout(timeout).timeout(timeout);
        }
        let channel = endpoint.connect().await?;
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready()
            .await
            .map_err(|error| anyhow::anyhow!("workflow gRPC callback is not ready: {error}"))?;
        let mut request = tonic::Request::new(CallbackWorkflowData {
            data: callback.data.clone(),
        });
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
        }
        insert_grpc_metadata(&mut request, "dtm-gid", &callback.gid)?;
        insert_grpc_metadata(&mut request, "dtm-trans_type", "workflow")?;
        insert_grpc_metadata(&mut request, "dtm-branch_id", "00")?;
        insert_grpc_metadata(&mut request, "dtm-op", &callback.operation)?;
        insert_grpc_metadata(&mut request, "dtm-dtm", "")?;
        for (name, value) in &options.branch_headers {
            insert_grpc_metadata(&mut request, &name.to_ascii_lowercase(), value)?;
        }
        let path = http::uri::PathAndQuery::from_maybe_shared(target.method)?;
        let codec = tonic_prost::ProstCodec::<CallbackWorkflowData, CallbackEmpty>::default();
        let result: Result<tonic::Response<CallbackEmpty>, tonic::Status> =
            grpc.unary(request, path, codec).await;
        match result {
            Ok(_) => Ok(WorkflowCallbackResult::Completed),
            Err(status) if status.code() == tonic::Code::Aborted => {
                Ok(WorkflowCallbackResult::Failed {
                    reason: bounded_reason(status.message()),
                })
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                Ok(WorkflowCallbackResult::Ongoing)
            }
            Err(status) => Err(status.into()),
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct CallbackWorkflowData {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CallbackEmpty {}

struct GrpcCallbackTarget {
    endpoint: String,
    method: String,
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

fn apply_http_transaction_options(
    mut request: reqwest::RequestBuilder,
    options: &TransactionOptions,
) -> reqwest::RequestBuilder {
    if let Some(timeout) = options.request_timeout_millis {
        request = request.timeout(Duration::from_millis(timeout));
    }
    for (name, value) in &options.branch_headers {
        request = request.header(name.as_str(), value.as_str());
    }
    request
}

fn add_workflow_callback_params(
    params: &mut serde_json::Map<String, serde_json::Value>,
    callback: &WorkflowCallbackRequest,
) {
    params.insert("gid".to_owned(), callback.gid.clone().into());
    params.insert("trans_type".to_owned(), "workflow".into());
    params.insert("branch_id".to_owned(), "00".into());
    params.insert("op".to_owned(), callback.operation.clone().into());
}

async fn decode_json_rpc_callback_response(
    response: reqwest::Response,
) -> anyhow::Result<WorkflowCallbackResult> {
    let body = limited_response_bytes(response, 64 * 1024).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    let Some(error) = value.get("error").filter(|error| !error.is_null()) else {
        return Ok(WorkflowCallbackResult::Completed);
    };
    let code = error
        .get("code")
        .and_then(serde_json::Value::as_i64)
        .context("JSON-RPC callback error requires a numeric code")?;
    match code {
        -32901 => Ok(WorkflowCallbackResult::Failed {
            reason: error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .and_then(bounded_reason),
        }),
        -32902 => Ok(WorkflowCallbackResult::Ongoing),
        _ => anyhow::bail!("workflow JSON-RPC callback returned unexpected error code {code}"),
    }
}

async fn limited_response_bytes(
    mut response: reqwest::Response,
    limit: usize,
) -> anyhow::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        anyhow::bail!("workflow callback response exceeds {limit} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            body.len().saturating_add(chunk.len()) <= limit,
            "workflow callback response exceeds {limit} bytes"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn limited_response_text(
    response: reqwest::Response,
    limit: usize,
) -> anyhow::Result<Option<String>> {
    let body = limited_response_bytes(response, limit).await?;
    Ok(bounded_reason(&String::from_utf8_lossy(&body)))
}

fn bounded_reason(value: &str) -> Option<String> {
    let mut value = value.trim().to_owned();
    if value.is_empty() {
        return None;
    }
    if value.len() > 4_096 {
        let mut boundary = 4_096;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        value.truncate(boundary);
    }
    Some(value)
}

fn insert_grpc_metadata<T>(
    request: &mut tonic::Request<T>,
    name: &str,
    value: &str,
) -> anyhow::Result<()> {
    let name = name.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()?;
    let value = value.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()?;
    request.metadata_mut().insert(name, value);
    Ok(())
}

fn parse_grpc_callback_target(value: &str) -> anyhow::Result<GrpcCallbackTarget> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 2_048,
        "invalid workflow gRPC callback"
    );
    let (scheme, remainder) = if let Some(remainder) = value
        .strip_prefix("grpc://")
        .or_else(|| value.strip_prefix("http://"))
    {
        ("http", remainder)
    } else if let Some(remainder) = value
        .strip_prefix("grpcs://")
        .or_else(|| value.strip_prefix("https://"))
    {
        ("https", remainder)
    } else {
        ("http", value)
    };
    let (authority, method) = remainder
        .split_once('/')
        .context("workflow gRPC callback requires server and method")?;
    anyhow::ensure!(
        !authority.is_empty()
            && !method.is_empty()
            && !method.contains('?')
            && !method.contains('#')
            && method.split('/').all(|segment| !segment.is_empty()),
        "invalid workflow gRPC callback"
    );
    let endpoint = format!("{scheme}://{authority}");
    let parsed = parse_branch_url(&endpoint)?;
    anyhow::ensure!(
        parsed.path() == "/" && parsed.query().is_none(),
        "invalid workflow gRPC callback"
    );
    Ok(GrpcCallbackTarget {
        endpoint,
        method: format!("/{method}"),
    })
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
        bump_transaction_revision(&mut tx);
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

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        let mut txs = self.txs.write().await;
        let tx = txs
            .get_mut(gid)
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        defer_workflow_recovery(tx, delay)?;
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
        bump_transaction_revision(&mut tx);
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
            apply_workflow_completion(&mut tx, status, rollback_reason.clone(), result.clone())?;
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

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        for _ in 0..16 {
            let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ?")
                .bind(gid)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let previous_payload = row.get::<&str, _>("payload").to_owned();
            let mut tx: Transaction = serde_json::from_str(&previous_payload)?;
            defer_workflow_recovery(&mut tx, delay.clone())?;
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
        anyhow::bail!("transaction {gid} workflow recovery update is contended")
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
        Ok(
            sqlx::query("DELETE FROM roze_dtm_kv WHERE category = ? AND entry_key = ?")
                .bind(category)
                .bind(key)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
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
        bump_transaction_revision(&mut tx);
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
        let row =
            sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE")
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
        let row =
            sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE")
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
        let row =
            sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE")
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

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row =
            sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = $1 FOR UPDATE")
                .bind(gid)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        defer_workflow_recovery(&mut tx, delay)?;
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
        Ok(
            sqlx::query("DELETE FROM roze_dtm_kv WHERE category = $1 AND entry_key = $2")
                .bind(category)
                .bind(key)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
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
        bump_transaction_revision(&mut tx);
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
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE")
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
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE")
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
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE")
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

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT payload FROM roze_dtm_transactions WHERE gid = ? FOR UPDATE")
            .bind(gid)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        let mut tx: Transaction = serde_json::from_str(row.get::<&str, _>("payload"))?;
        defer_workflow_recovery(&mut tx, delay)?;
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
        Ok(
            sqlx::query("DELETE FROM roze_dtm_kv WHERE category = ? AND entry_key = ?")
                .bind(category)
                .bind(key)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
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

    async fn persist_transaction(&self, transaction: &mut Transaction) -> anyhow::Result<()> {
        self.store.update_transaction(transaction.clone()).await?;
        transaction.revision = transaction.revision.saturating_add(1);
        Ok(())
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
        // The coordinator owns the persisted revision sequence. Do not allow a
        // caller-supplied value to skip or saturate later compare-and-set writes.
        tx.revision = 1;
        tx.options.validate()?;
        anyhow::ensure!(
            tx.kind == TransactionKind::Message || tx.options.delay_millis.is_none(),
            "delay_millis is only supported by Message transactions"
        );
        validate_transaction_dependencies(&tx)?;
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

    pub async fn recover_callback_workflow(&self, gid: &str) -> anyhow::Result<Transaction> {
        let tx = self
            .transaction_of_kind(gid, TransactionKind::Workflow)
            .await?;
        if tx.status.is_terminal() {
            return Ok(tx);
        }
        ensure_callback_workflow(&tx)?;
        ensure_status(&tx, &[TransactionStatus::Prepared])?;
        let attempted_at_millis = current_millis();
        let callback = match workflow_callback_request(&tx) {
            Ok(callback) => callback,
            Err(_) => {
                tracing::error!(
                    event = "dtm.workflow.callback.invalid",
                    gid = %tx.gid,
                    error_kind = "invalid_callback_contract",
                    "Workflow callback contract is invalid"
                );
                return self
                    .defer_callback_workflow(&tx, attempted_at_millis, true, "invalid_callback")
                    .await;
            }
        };
        let result = self
            .invoker
            .query_workflow_callback(&callback, &tx.options)
            .await;
        let latest = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        if latest.status.is_terminal() {
            tracing::info!(
                event = "dtm.workflow.callback.completed",
                gid = %latest.gid,
                status = ?latest.status,
                "Workflow callback reached a terminal state"
            );
            return Ok(latest);
        }
        ensure_callback_workflow(&latest)?;
        ensure_status(&latest, &[TransactionStatus::Prepared])?;
        match result {
            Ok(WorkflowCallbackResult::Failed { reason }) => {
                tracing::warn!(
                    event = "dtm.workflow.callback.failed",
                    gid = %latest.gid,
                    outcome = "business_failure",
                    "Workflow callback reported failure"
                );
                match self
                    .finish_callback_workflow(
                        gid,
                        TransactionStatus::Failed,
                        reason.or_else(|| Some("workflow callback reported failure".to_owned())),
                        None,
                    )
                    .await
                {
                    Ok(transaction) => Ok(transaction),
                    Err(error) => self.terminal_after_conflict(gid, error).await,
                }
            }
            Ok(WorkflowCallbackResult::Ongoing) => {
                tracing::info!(
                    event = "dtm.workflow.callback.deferred",
                    gid = %latest.gid,
                    outcome = "ongoing",
                    "Workflow callback remains ongoing"
                );
                self.defer_callback_workflow(&latest, attempted_at_millis, false, "ongoing")
                    .await
            }
            Ok(WorkflowCallbackResult::Completed) => {
                tracing::warn!(
                    event = "dtm.workflow.callback.deferred",
                    gid = %latest.gid,
                    outcome = "completed_without_terminal",
                    "Workflow callback returned success without submitting a terminal state"
                );
                self.defer_callback_workflow(
                    &latest,
                    attempted_at_millis,
                    true,
                    "completed_without_terminal",
                )
                .await
            }
            Err(_) => {
                tracing::warn!(
                    event = "dtm.workflow.callback.deferred",
                    gid = %latest.gid,
                    outcome = "transport_error",
                    error_kind = "callback_transport",
                    "Workflow callback transport failed"
                );
                self.defer_callback_workflow(&latest, attempted_at_millis, true, "transport_error")
                    .await
            }
        }
    }

    async fn defer_callback_workflow(
        &self,
        tx: &Transaction,
        attempted_at_millis: u64,
        backoff: bool,
        outcome: &str,
    ) -> anyhow::Result<Transaction> {
        let retry_interval_millis = tx
            .options
            .retry_interval_millis
            .unwrap_or(self.options.retry_backoff_millis)
            .max(1);
        let delay = WorkflowRecoveryDelay {
            attempted_at_millis,
            retry_interval_millis,
            max_retry_interval_millis: self
                .options
                .max_retry_backoff_millis
                .max(retry_interval_millis),
            backoff,
            outcome: outcome.to_owned(),
        };
        match self.store.defer_workflow_recovery(&tx.gid, delay).await {
            Ok(transaction) => Ok(transaction),
            Err(error) => self.terminal_after_conflict(&tx.gid, error).await,
        }
    }

    async fn terminal_after_conflict(
        &self,
        gid: &str,
        error: anyhow::Error,
    ) -> anyhow::Result<Transaction> {
        let latest = self
            .store
            .get_transaction(gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
        if latest.status.is_terminal() {
            Ok(latest)
        } else {
            Err(error)
        }
    }

    async fn expire_callback_workflow(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<Transaction> {
        let transaction = if transaction.status == TransactionStatus::Submitted {
            self.prepare_workflow(&transaction.gid).await?
        } else {
            transaction.clone()
        };
        ensure_status(&transaction, &[TransactionStatus::Prepared])?;
        match self
            .finish_callback_workflow(
                &transaction.gid,
                TransactionStatus::Failed,
                Some("workflow callback timed out".to_owned()),
                None,
            )
            .await
        {
            Ok(transaction) => Ok(transaction),
            Err(error) => self.terminal_after_conflict(&transaction.gid, error).await,
        }
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
        anyhow::ensure!(
            !is_callback_workflow(&tx),
            "callback workflow must submit a succeed or failed completion"
        );
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
        if (matches!(tx.kind, TransactionKind::Tcc | TransactionKind::Xa)
            && tx.status == TransactionStatus::Prepared)
            || (tx.kind == TransactionKind::Message
                && matches!(
                    tx.status,
                    TransactionStatus::Submitted | TransactionStatus::Prepared
                ))
        {
            tx.status = TransactionStatus::Succeeding;
            self.persist_transaction(&mut tx).await?;
        } else if tx.kind == TransactionKind::Workflow && tx.status == TransactionStatus::Prepared {
            tx.status = TransactionStatus::Submitted;
            self.persist_transaction(&mut tx).await?;
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
            TransactionKind::Saga => &[
                TransactionStatus::Submitted,
                TransactionStatus::Succeeding,
                TransactionStatus::Aborting,
            ][..],
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
        self.persist_transaction(&mut tx).await?;
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
        if tx.options.concurrent {
            return self.start_concurrent_saga(tx).await;
        }
        let execution_options = tx.options.clone();
        let max_attempts = execution_options
            .retry_limit
            .map(|retries| retries.saturating_add(1))
            .unwrap_or(1);
        tx.status = TransactionStatus::Succeeding;
        self.persist_transaction(&mut tx).await?;
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
                self.invoke_branch(&execution_options, &tx.gid, tx.status, branch, &action)
                    .await
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
                        self.persist_transaction(&mut tx).await?;
                        return Ok(tx);
                    }
                    tx.branches[index].status = BranchStatus::Compensating;
                    tx.status = TransactionStatus::Aborting;
                    for previous in &mut tx.branches {
                        if previous.status == BranchStatus::Succeeded {
                            previous.status = BranchStatus::Compensating;
                        }
                    }
                    self.persist_transaction(&mut tx).await?;
                    return self.abort_saga(gid).await;
                }
            }
            self.persist_transaction(&mut tx).await?;
        }
        tx.status = TransactionStatus::Succeeded;
        self.persist_transaction(&mut tx).await?;
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
        if tx.options.concurrent {
            return self.abort_concurrent_saga(tx).await;
        }
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
                            .invoke_url(&execution_options, &tx.gid, tx.status, branch, &compensate)
                            .await
                            .is_err()
                        {
                            branch.status = BranchStatus::Compensating;
                            self.store.release_barrier(&barrier).await?;
                            self.persist_transaction(&mut tx).await?;
                            return Ok(tx);
                        }
                    }
                    branch.status = BranchStatus::Skipped;
                    branch.next_retry_millis = None;
                }
                BarrierDecision::SkipDuplicate => {
                    self.persist_transaction(&mut tx).await?;
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
        self.persist_transaction(&mut tx).await?;
        Ok(tx)
    }

    async fn start_concurrent_saga(&self, mut tx: Transaction) -> anyhow::Result<Transaction> {
        validate_transaction_dependencies(&tx)?;
        let execution_options = tx.options.clone();
        let max_attempts = execution_options
            .retry_limit
            .map(|retries| retries.saturating_add(1))
            .unwrap_or(1);
        tx.status = TransactionStatus::Succeeding;
        self.persist_transaction(&mut tx).await?;

        loop {
            if tx
                .branches
                .iter()
                .all(|branch| branch.status == BranchStatus::Succeeded)
            {
                tx.status = TransactionStatus::Succeeded;
                self.persist_transaction(&mut tx).await?;
                return Ok(tx);
            }
            let succeeded = tx
                .branches
                .iter()
                .filter(|branch| branch.status == BranchStatus::Succeeded)
                .map(|branch| branch.id.as_str())
                .collect::<BTreeSet<_>>();
            let ready = tx
                .branches
                .iter()
                .enumerate()
                .filter(|(_, branch)| {
                    matches!(branch.status, BranchStatus::Pending | BranchStatus::Failed)
                        && branch
                            .dependencies
                            .iter()
                            .all(|dependency| succeeded.contains(dependency.as_str()))
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if ready.is_empty() {
                return Ok(tx);
            }

            let mut claimed = Vec::with_capacity(ready.len());
            for &index in &ready {
                let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "action");
                if self.store.barrier(barrier.clone()).await? != BarrierDecision::Execute {
                    for barrier in claimed {
                        self.store.release_barrier(&barrier).await?;
                    }
                    return self
                        .store
                        .get_transaction(&tx.gid)
                        .await?
                        .with_context(|| format!("transaction not found: {}", tx.gid));
                }
                claimed.push(barrier);
            }
            for &index in &ready {
                tx.branches[index].status = BranchStatus::Running;
                tx.branches[index].attempts = tx.branches[index].attempts.saturating_add(1);
            }
            self.persist_transaction(&mut tx).await?;

            let mut tasks = tokio::task::JoinSet::new();
            for index in ready {
                let invoker = self.invoker.clone();
                let gid = tx.gid.clone();
                let options = execution_options.clone();
                let mut branch = tx.branches[index].clone();
                let action = branch.action.clone();
                let retry_backoff_millis = options
                    .retry_interval_millis
                    .unwrap_or(self.options.retry_backoff_millis);
                let max_retry_backoff_millis = self
                    .options
                    .max_retry_backoff_millis
                    .max(retry_backoff_millis);
                tasks.spawn(async move {
                    let succeeded = invoker
                        .invoke_with_options(&action, &branch.payload, &options)
                        .await
                        .is_ok();
                    if succeeded {
                        branch.status = BranchStatus::Succeeded;
                        branch.next_retry_millis = None;
                    } else {
                        branch.status = BranchStatus::Failed;
                        record_branch_failure(
                            &mut branch,
                            "branch_call_failed".to_owned(),
                            retry_backoff_millis,
                            max_retry_backoff_millis,
                        );
                        notify_branch_failure(
                            &invoker,
                            &gid,
                            TransactionStatus::Succeeding,
                            &branch,
                            &action,
                        )
                        .await;
                    }
                    (index, branch, succeeded)
                });
            }

            let mut retryable_failure = false;
            let mut exhausted_failure = false;
            while let Some(result) = tasks.join_next().await {
                let (index, branch, succeeded) =
                    result.context("concurrent Saga action task failed")?;
                tx.branches[index] = branch;
                if !succeeded {
                    let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "action");
                    self.store.release_barrier(&barrier).await?;
                    if tx.branches[index].attempts < max_attempts {
                        retryable_failure = true;
                    } else {
                        exhausted_failure = true;
                    }
                }
            }

            if exhausted_failure {
                tx.status = TransactionStatus::Aborting;
                for branch in &mut tx.branches {
                    branch.status = if branch.status == BranchStatus::Succeeded {
                        BranchStatus::Compensating
                    } else {
                        BranchStatus::Skipped
                    };
                    if branch.status == BranchStatus::Skipped {
                        branch.next_retry_millis = None;
                    }
                }
                self.persist_transaction(&mut tx).await?;
                return self.abort_concurrent_saga(tx).await;
            }
            self.persist_transaction(&mut tx).await?;
            if retryable_failure {
                return Ok(tx);
            }
        }
    }

    async fn abort_concurrent_saga(&self, mut tx: Transaction) -> anyhow::Result<Transaction> {
        validate_transaction_dependencies(&tx)?;
        let execution_options = tx.options.clone();
        tx.status = TransactionStatus::Aborting;
        for branch in &mut tx.branches {
            branch.status = match branch.status {
                BranchStatus::Succeeded => BranchStatus::Compensating,
                BranchStatus::Pending | BranchStatus::Running | BranchStatus::Failed => {
                    branch.next_retry_millis = None;
                    BranchStatus::Skipped
                }
                status => status,
            };
        }
        self.persist_transaction(&mut tx).await?;

        loop {
            if tx
                .branches
                .iter()
                .all(|branch| branch.status != BranchStatus::Compensating)
            {
                tx.status = TransactionStatus::Aborted;
                self.persist_transaction(&mut tx).await?;
                return Ok(tx);
            }
            let ready = tx
                .branches
                .iter()
                .enumerate()
                .filter(|(_, branch)| branch.status == BranchStatus::Compensating)
                .filter(|(_, branch)| {
                    !tx.branches.iter().any(|dependent| {
                        dependent.dependencies.contains(&branch.id)
                            && dependent.status == BranchStatus::Compensating
                    })
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                !ready.is_empty(),
                "concurrent Saga {} has unresolved compensation dependencies",
                tx.gid
            );

            let mut claimed = Vec::with_capacity(ready.len());
            for &index in &ready {
                let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "compensate");
                if self.store.barrier(barrier.clone()).await? != BarrierDecision::Execute {
                    for barrier in claimed {
                        self.store.release_barrier(&barrier).await?;
                    }
                    return self
                        .store
                        .get_transaction(&tx.gid)
                        .await?
                        .with_context(|| format!("transaction not found: {}", tx.gid));
                }
                claimed.push(barrier);
                tx.branches[index].attempts = tx.branches[index].attempts.saturating_add(1);
            }
            self.persist_transaction(&mut tx).await?;

            let mut tasks = tokio::task::JoinSet::new();
            for index in ready {
                let invoker = self.invoker.clone();
                let gid = tx.gid.clone();
                let options = execution_options.clone();
                let mut branch = tx.branches[index].clone();
                let compensate = branch
                    .compensate
                    .clone()
                    .with_context(|| format!("missing Saga compensation URL for {}", branch.id))?;
                let retry_backoff_millis = options
                    .retry_interval_millis
                    .unwrap_or(self.options.retry_backoff_millis);
                let max_retry_backoff_millis = self
                    .options
                    .max_retry_backoff_millis
                    .max(retry_backoff_millis);
                tasks.spawn(async move {
                    let succeeded = invoker
                        .invoke_with_options(&compensate, &branch.payload, &options)
                        .await
                        .is_ok();
                    if succeeded {
                        branch.status = BranchStatus::Skipped;
                        branch.next_retry_millis = None;
                    } else {
                        branch.status = BranchStatus::Compensating;
                        record_branch_failure(
                            &mut branch,
                            "branch_call_failed".to_owned(),
                            retry_backoff_millis,
                            max_retry_backoff_millis,
                        );
                        notify_branch_failure(
                            &invoker,
                            &gid,
                            TransactionStatus::Aborting,
                            &branch,
                            &compensate,
                        )
                        .await;
                    }
                    (index, branch, succeeded)
                });
            }

            let mut failed = false;
            while let Some(result) = tasks.join_next().await {
                let (index, branch, succeeded) =
                    result.context("concurrent Saga compensation task failed")?;
                tx.branches[index] = branch;
                if !succeeded {
                    failed = true;
                    let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "compensate");
                    self.store.release_barrier(&barrier).await?;
                }
            }
            self.persist_transaction(&mut tx).await?;
            if failed {
                return Ok(tx);
            }
        }
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
                        .invoke_branch(&execution_options, &tx.gid, tx.status, branch, &action)
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
                            self.persist_transaction(&mut tx).await?;
                            return Ok(tx);
                        }
                    }
                }
                BarrierDecision::SkipCancelledTry => {
                    branch.status = BranchStatus::Skipped;
                    branch.next_retry_millis = None;
                    tx.status = TransactionStatus::Aborting;
                    self.persist_transaction(&mut tx).await?;
                    return Ok(tx);
                }
                BarrierDecision::SkipDuplicate => {}
                BarrierDecision::SkipNullCompensation => {
                    unreachable!("TCC try is not a compensation operation")
                }
            }
        }
        tx.status = TransactionStatus::Prepared;
        self.persist_transaction(&mut tx).await?;
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
                    .invoke_url(&execution_options, &tx.gid, tx.status, branch, &confirm)
                    .await
                {
                    Ok(()) => {
                        branch.status = BranchStatus::Succeeded;
                        branch.next_retry_millis = None;
                    }
                    Err(_) => {
                        branch.status = BranchStatus::Failed;
                        self.store.release_barrier(&barrier).await?;
                        self.persist_transaction(&mut tx).await?;
                        return Ok(tx);
                    }
                }
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.persist_transaction(&mut tx).await?;
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
                        .invoke_url(&execution_options, &tx.gid, tx.status, branch, &cancel)
                        .await
                    {
                        Ok(()) => {
                            branch.status = BranchStatus::Skipped;
                            branch.next_retry_millis = None;
                        }
                        Err(_) => {
                            branch.status = BranchStatus::Failed;
                            self.store.release_barrier(&barrier).await?;
                            self.persist_transaction(&mut tx).await?;
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
        self.persist_transaction(&mut tx).await?;
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
        self.persist_transaction(&mut tx).await?;
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
        self.persist_transaction(&mut tx).await?;
        Ok(tx)
    }

    pub async fn start_workflow(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self
            .transaction_of_kind(gid, TransactionKind::Workflow)
            .await?;
        if tx.status == TransactionStatus::Succeeded {
            return Ok(tx);
        }
        anyhow::ensure!(
            !is_callback_workflow(&tx),
            "callback workflow is driven through its QueryPrepared callback"
        );
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
                .invoke_branch(
                    &execution_options,
                    &tx.gid,
                    tx.status,
                    &mut tx.branches[index],
                    &action,
                )
                .await
                .is_err()
            {
                tx.branches[index].status = BranchStatus::Failed;
                tx.status = TransactionStatus::Aborting;
                self.store.release_barrier(&barrier).await?;
                self.persist_transaction(&mut tx).await?;
                return self.abort_workflow(gid).await;
            }
            tx.branches[index].status = BranchStatus::Succeeded;
            tx.branches[index].next_retry_millis = None;
            self.persist_transaction(&mut tx).await?;
        }
        if tx
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Succeeded)
        {
            tx.status = TransactionStatus::Succeeded;
            self.persist_transaction(&mut tx).await?;
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
                    .invoke_url(&execution_options, &tx.gid, tx.status, branch, &compensate)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Compensating;
                    self.store.release_barrier(&barrier).await?;
                    self.persist_transaction(&mut tx).await?;
                    return Ok(tx);
                }
            }
            branch.status = BranchStatus::Skipped;
            branch.next_retry_millis = None;
        }
        tx.status = TransactionStatus::Aborted;
        self.persist_transaction(&mut tx).await?;
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
        if tx.status != TransactionStatus::Succeeding {
            // Persist the submit decision before waiting or performing branch
            // I/O, so recovery never has to guess whether a Prepared Message
            // was committed by its caller.
            tx.status = TransactionStatus::Succeeding;
            self.persist_transaction(&mut tx).await?;
        }
        if !message_dispatch_due(&tx, current_millis()) {
            return Ok(tx);
        }
        let execution_options = tx.options.clone();
        if execution_options.concurrent {
            let ready = tx
                .branches
                .iter()
                .enumerate()
                .filter(|(_, branch)| branch.status != BranchStatus::Succeeded)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if ready.is_empty() {
                tx.status = TransactionStatus::Succeeded;
                self.persist_transaction(&mut tx).await?;
                return Ok(tx);
            }

            let mut claimed = Vec::with_capacity(ready.len());
            for &index in &ready {
                let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "message");
                if self.store.barrier(barrier.clone()).await? != BarrierDecision::Execute {
                    for barrier in claimed {
                        self.store.release_barrier(&barrier).await?;
                    }
                    return self
                        .store
                        .get_transaction(&tx.gid)
                        .await?
                        .with_context(|| format!("transaction not found: {}", tx.gid));
                }
                claimed.push(barrier);
            }
            for &index in &ready {
                tx.branches[index].status = BranchStatus::Running;
                tx.branches[index].attempts = tx.branches[index].attempts.saturating_add(1);
            }
            self.persist_transaction(&mut tx).await?;

            let retry_backoff_millis = execution_options
                .retry_interval_millis
                .unwrap_or(self.options.retry_backoff_millis);
            let max_retry_backoff_millis = self
                .options
                .max_retry_backoff_millis
                .max(retry_backoff_millis);
            let mut tasks = tokio::task::JoinSet::new();
            for index in ready {
                let invoker = self.invoker.clone();
                let gid = tx.gid.clone();
                let options = execution_options.clone();
                let mut branch = tx.branches[index].clone();
                let action = branch.action.clone();
                tasks.spawn(async move {
                    let succeeded = invoker
                        .invoke_with_options(&action, &branch.payload, &options)
                        .await
                        .is_ok();
                    if succeeded {
                        branch.status = BranchStatus::Succeeded;
                        branch.next_retry_millis = None;
                    } else {
                        branch.status = BranchStatus::Failed;
                        record_branch_failure(
                            &mut branch,
                            "branch_call_failed".to_owned(),
                            retry_backoff_millis,
                            max_retry_backoff_millis,
                        );
                        notify_branch_failure(
                            &invoker,
                            &gid,
                            TransactionStatus::Succeeding,
                            &branch,
                            &action,
                        )
                        .await;
                    }
                    (index, branch, succeeded)
                });
            }

            let mut failed = false;
            while let Some(result) = tasks.join_next().await {
                let (index, branch, succeeded) =
                    result.context("concurrent Message branch task failed")?;
                tx.branches[index] = branch;
                if !succeeded {
                    failed = true;
                    let barrier = BranchBarrier::new(&tx.gid, &tx.branches[index].id, "message");
                    self.store.release_barrier(&barrier).await?;
                }
            }
            tx.status = if failed {
                TransactionStatus::Succeeding
            } else {
                TransactionStatus::Succeeded
            };
            self.persist_transaction(&mut tx).await?;
            return Ok(tx);
        }
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
                    .invoke_branch(&execution_options, &tx.gid, tx.status, branch, &action)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.persist_transaction(&mut tx).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Succeeded;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.persist_transaction(&mut tx).await?;
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
        self.persist_transaction(&mut tx).await?;
        Ok(tx)
    }

    pub async fn prepare_xa(&self, gid: &str) -> anyhow::Result<Transaction> {
        let mut tx = self.transaction_of_kind(gid, TransactionKind::Xa).await?;
        if tx.status == TransactionStatus::Prepared {
            return Ok(tx);
        }
        ensure_status(&tx, &[TransactionStatus::Submitted])?;
        tx.status = TransactionStatus::Prepared;
        self.persist_transaction(&mut tx).await?;
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
                let commit = xa_phase2_callback_url(&commit, &tx.gid, &branch.id, "commit")?;
                branch.attempts = branch.attempts.saturating_add(1);
                if self
                    .invoke_url(&execution_options, &tx.gid, tx.status, branch, &commit)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.persist_transaction(&mut tx).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Succeeded;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Succeeded;
        self.persist_transaction(&mut tx).await?;
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
                let rollback = xa_phase2_callback_url(&rollback, &tx.gid, &branch.id, "rollback")?;
                branch.attempts = branch.attempts.saturating_add(1);
                if self
                    .invoke_url(&execution_options, &tx.gid, tx.status, branch, &rollback)
                    .await
                    .is_err()
                {
                    branch.status = BranchStatus::Failed;
                    self.store.release_barrier(&barrier).await?;
                    self.persist_transaction(&mut tx).await?;
                    return Ok(tx);
                }
                branch.status = BranchStatus::Skipped;
                branch.next_retry_millis = None;
            }
        }
        tx.status = TransactionStatus::Aborted;
        self.persist_transaction(&mut tx).await?;
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
        anyhow::ensure!(
            !tx.status.is_terminal(),
            "transaction {gid} is already terminal"
        );
        tx.status = TransactionStatus::Failed;
        for branch in &mut tx.branches {
            branch.next_retry_millis = None;
        }
        self.persist_transaction(&mut tx).await?;
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
            if matches!(
                branch.status,
                BranchStatus::Failed | BranchStatus::Running | BranchStatus::Compensating
            ) {
                branch.next_retry_millis = Some(now);
            }
        }
        if is_callback_workflow(&tx) {
            tx.metadata
                .insert("dtm.callback.next_retry_millis".to_owned(), now.to_string());
            tx.metadata.insert(
                "dtm.callback.last_outcome".to_owned(),
                "manual_reset".to_owned(),
            );
        }
        self.persist_transaction(&mut tx).await?;
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

    /// Resets transactions whose earliest retry is scheduled beyond `after_millis`.
    ///
    /// This matches DTM's `resetCronTime` recovery operation without exposing a
    /// backend-specific cron index. The extra boolean is true only when more
    /// matching transactions remain after the bounded batch.
    pub async fn reset_retry_batch_after(
        &self,
        after_millis: u64,
        limit: usize,
    ) -> anyhow::Result<(Vec<Transaction>, bool)> {
        let limit = limit.max(1);
        let cutoff = current_millis().saturating_add(after_millis);
        let mut candidates = self
            .store
            .list_transactions()
            .await?
            .into_iter()
            .filter_map(|transaction| {
                transaction_next_recovery_millis(&transaction)
                    .filter(|next| *next > cutoff)
                    .map(|next| (next, transaction.gid))
            })
            .collect::<Vec<_>>();
        candidates.sort();
        let has_remaining = candidates.len() > limit;
        let mut reset = Vec::with_capacity(limit.min(candidates.len()));
        for (_, gid) in candidates.into_iter().take(limit) {
            reset.push(self.reset_retry(&gid).await?);
        }
        Ok((reset, has_remaining))
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
        if tx.kind == TransactionKind::Message
            && tx.status == TransactionStatus::Succeeding
            && !message_dispatch_due(&tx, current_millis())
        {
            return Ok(tx);
        }
        if is_expired(&tx, current_millis()) {
            return match tx.kind {
                TransactionKind::Tcc => self.cancel_tcc(gid).await,
                TransactionKind::Saga => self.abort_saga(gid).await,
                TransactionKind::Workflow if is_callback_workflow(&tx) => {
                    self.expire_callback_workflow(&tx).await
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
        if is_callback_workflow(&tx) {
            return match tx.status {
                TransactionStatus::Submitted => self.prepare_workflow(gid).await,
                TransactionStatus::Prepared => self.recover_callback_workflow(gid).await,
                status => {
                    anyhow::bail!("callback workflow {gid} is in non-replayable state {status:?}")
                }
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
            if tx.kind == TransactionKind::Message
                && tx.status == TransactionStatus::Succeeding
                && !message_dispatch_due(&tx, now)
            {
                continue;
            }
            if is_expired(&tx, now) {
                let next = match tx.kind {
                    TransactionKind::Tcc => self.cancel_tcc(&tx.gid).await?,
                    TransactionKind::Saga => self.abort_saga(&tx.gid).await?,
                    TransactionKind::Workflow if is_callback_workflow(&tx) => {
                        self.expire_callback_workflow(&tx).await?
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
            if is_callback_workflow(&tx) {
                let next = match tx.status {
                    TransactionStatus::Submitted => self.prepare_workflow(&tx.gid).await?,
                    TransactionStatus::Prepared if callback_workflow_due(&tx, now) => {
                        self.recover_callback_workflow(&tx.gid).await?
                    }
                    TransactionStatus::Prepared => continue,
                    _ => continue,
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
                (TransactionKind::Tcc, TransactionStatus::Prepared) => continue,
                (TransactionKind::Tcc, TransactionStatus::Succeeding) => {
                    self.confirm_tcc(&tx.gid).await?
                }
                (TransactionKind::Tcc, TransactionStatus::Aborting) => {
                    self.cancel_tcc(&tx.gid).await?
                }
                (
                    TransactionKind::Saga,
                    TransactionStatus::Submitted | TransactionStatus::Succeeding,
                ) => self.start_saga(&tx.gid).await?,
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
                (TransactionKind::Message, TransactionStatus::Prepared) => continue,
                (TransactionKind::Message, TransactionStatus::Succeeding) => {
                    self.dispatch_message(&tx.gid).await?
                }
                (TransactionKind::Xa, TransactionStatus::Submitted) => {
                    self.prepare_xa(&tx.gid).await?
                }
                (TransactionKind::Xa, TransactionStatus::Prepared) => continue,
                (TransactionKind::Xa, TransactionStatus::Succeeding) => {
                    self.commit_xa(&tx.gid).await?
                }
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
    ) -> anyhow::Result<Vec<Transaction>>
    where
        S: Clone,
    {
        let Some(fence) = self
            .store
            .acquire_recovery_lease("roze-dtm-recovery", owner, ttl_millis)
            .await?
        else {
            return Ok(Vec::new());
        };
        let recovering = Dtm {
            store: RecoveryFencedStore::new(self.store.clone(), fence),
            invoker: self.invoker.clone(),
            options: self.options,
        };
        recovering.tick_recover_once().await
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
        gid: &str,
        transaction_status: TransactionStatus,
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
                self.notify_branch_failure(gid, transaction_status, branch, url)
                    .await;
                Err(anyhow::anyhow!("branch call failed"))
            }
        }
    }

    async fn invoke_url(
        &self,
        options: &TransactionOptions,
        gid: &str,
        transaction_status: TransactionStatus,
        branch: &mut Branch,
        url: &str,
    ) -> anyhow::Result<()> {
        self.invoke_branch(options, gid, transaction_status, branch, url)
            .await
    }

    async fn notify_branch_failure(
        &self,
        gid: &str,
        transaction_status: TransactionStatus,
        branch: &Branch,
        url: &str,
    ) {
        notify_branch_failure(&self.invoker, gid, transaction_status, branch, url).await;
    }
}

async fn notify_branch_failure<I: BranchInvoker>(
    invoker: &I,
    gid: &str,
    transaction_status: TransactionStatus,
    branch: &Branch,
    url: &str,
) {
    let alert = BranchFailureAlert {
        gid: gid.to_owned(),
        status: transaction_status_name(transaction_status).to_owned(),
        branch: alert_branch_url(url),
        error: "branch_call_failed".to_owned(),
        retry_count: branch.attempts,
    };
    if invoker.notify_branch_failure(&alert).await.is_err() {
        tracing::warn!(
            event = "dtm.branch.alert.failed",
            error_kind = "alert_webhook_failed",
            transaction_status = alert.status,
            retry_count = alert.retry_count,
            "DTM branch failure alert could not be delivered"
        );
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
    bump_transaction_revision(tx);
    Ok(())
}

/// Validates execution options and dependency DAGs used by Saga and Workflow.
pub fn validate_transaction_dependencies(transaction: &Transaction) -> anyhow::Result<()> {
    if transaction.kind != TransactionKind::Saga {
        anyhow::ensure!(
            transaction.kind == TransactionKind::Message || !transaction.options.concurrent,
            "options.concurrent is only supported for Saga and Message transactions"
        );
        if transaction.kind == TransactionKind::Workflow {
            return validate_dependency_graph(transaction, "Workflow");
        }
        anyhow::ensure!(
            transaction
                .branches
                .iter()
                .all(|branch| branch.dependencies.is_empty()),
            "branch dependencies are only supported for Saga and Workflow transactions"
        );
        return Ok(());
    }
    if !transaction.options.concurrent {
        anyhow::ensure!(
            transaction
                .branches
                .iter()
                .all(|branch| branch.dependencies.is_empty()),
            "Saga dependencies require options.concurrent=true"
        );
        return Ok(());
    }

    validate_dependency_graph(transaction, "concurrent Saga")
}

/// Backward-compatible validation entry point retained for callers that used
/// the original Saga-only helper before Workflow dependencies were supported.
pub fn validate_saga_dependencies(transaction: &Transaction) -> anyhow::Result<()> {
    validate_transaction_dependencies(transaction)
}

fn validate_dependency_graph(
    transaction: &Transaction,
    graph_name: &'static str,
) -> anyhow::Result<()> {
    let positions = transaction
        .branches
        .iter()
        .enumerate()
        .map(|(index, branch)| (branch.id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    anyhow::ensure!(
        positions.len() == transaction.branches.len(),
        "{graph_name} branch ids must be unique"
    );
    for branch in &transaction.branches {
        let mut unique = BTreeSet::new();
        for dependency in &branch.dependencies {
            anyhow::ensure!(
                dependency != &branch.id,
                "{graph_name} branch {} cannot depend on itself",
                branch.id
            );
            anyhow::ensure!(
                positions.contains_key(dependency.as_str()),
                "{graph_name} branch {} has unknown dependency {}",
                branch.id,
                dependency
            );
            anyhow::ensure!(
                unique.insert(dependency),
                "{graph_name} branch {} repeats dependency {}",
                branch.id,
                dependency
            );
        }
    }

    fn visit(
        index: usize,
        branches: &[Branch],
        positions: &BTreeMap<&str, usize>,
        visiting: &mut BTreeSet<usize>,
        visited: &mut BTreeSet<usize>,
        graph_name: &'static str,
    ) -> anyhow::Result<()> {
        if visited.contains(&index) {
            return Ok(());
        }
        anyhow::ensure!(
            visiting.insert(index),
            "{graph_name} dependencies contain a cycle"
        );
        for dependency in &branches[index].dependencies {
            let dependency_index = *positions
                .get(dependency.as_str())
                .expect("validated transaction dependency");
            visit(
                dependency_index,
                branches,
                positions,
                visiting,
                visited,
                graph_name,
            )?;
        }
        visiting.remove(&index);
        visited.insert(index);
        Ok(())
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for index in 0..transaction.branches.len() {
        visit(
            index,
            &transaction.branches,
            &positions,
            &mut visiting,
            &mut visited,
            graph_name,
        )?;
    }
    Ok(())
}

fn xa_phase2_callback_url(
    value: &str,
    gid: &str,
    branch_id: &str,
    operation: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        matches!(operation, "commit" | "rollback"),
        "invalid XA phase-2 operation"
    );
    let mut url = parse_branch_url(value)?;
    let retained = url
        .query_pairs()
        .filter(|(name, _)| !matches!(name.as_ref(), "gid" | "trans_type" | "branch_id" | "op"))
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in retained {
            query.append_pair(&name, &value);
        }
        query
            .append_pair("gid", gid)
            .append_pair("trans_type", "xa")
            .append_pair("branch_id", branch_id)
            .append_pair("op", operation);
    }
    Ok(url.into())
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
    bump_transaction_revision(tx);
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
        matches!(
            status,
            TransactionStatus::Succeeded | TransactionStatus::Failed
        ),
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
    bump_transaction_revision(tx);
    Ok(())
}

fn defer_workflow_recovery(
    tx: &mut Transaction,
    delay: WorkflowRecoveryDelay,
) -> anyhow::Result<()> {
    ensure_callback_workflow(tx)?;
    ensure_status(tx, &[TransactionStatus::Prepared])?;
    delay.validate()?;
    let attempts = tx
        .metadata
        .get("dtm.callback.attempts")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default()
        .saturating_add(1);
    let shift = if delay.backoff {
        attempts.saturating_sub(1).min(16)
    } else {
        0
    };
    let retry_after = delay
        .retry_interval_millis
        .saturating_mul(1_u64 << shift)
        .min(delay.max_retry_interval_millis);
    tx.metadata
        .insert("dtm.callback.attempts".to_owned(), attempts.to_string());
    tx.metadata.insert(
        "dtm.callback.last_attempt_millis".to_owned(),
        delay.attempted_at_millis.to_string(),
    );
    tx.metadata.insert(
        "dtm.callback.next_retry_millis".to_owned(),
        delay
            .attempted_at_millis
            .saturating_add(retry_after)
            .to_string(),
    );
    tx.metadata
        .insert("dtm.callback.last_outcome".to_owned(), delay.outcome);
    bump_transaction_revision(tx);
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

fn workflow_callback_request(tx: &Transaction) -> anyhow::Result<WorkflowCallbackRequest> {
    ensure_callback_workflow(tx)?;
    let url = tx
        .metadata
        .get("dtm.query_prepared")
        .cloned()
        .context("callback workflow query URL is missing")?;
    let custom_data = tx
        .metadata
        .get("dtm.custom_data")
        .context("callback workflow custom data is missing")?;
    anyhow::ensure!(
        custom_data.len() <= 3 * 1024 * 1024,
        "callback workflow custom data exceeds 3 MiB"
    );
    let custom_data: serde_json::Value = serde_json::from_str(custom_data)?;
    let custom_data = custom_data
        .as_object()
        .context("callback workflow custom data must be a JSON object")?;
    let operation = custom_data
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty() && name.len() <= 128)
        .context("callback workflow name must contain 1 to 128 bytes")?
        .to_owned();
    let data = match custom_data.get("data") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::String(data)) => base64::engine::general_purpose::STANDARD
            .decode(data)
            .context("callback workflow data is not valid base64")?,
        Some(serde_json::Value::Array(data)) => data
            .iter()
            .map(|byte| {
                byte.as_u64()
                    .filter(|byte| *byte <= u64::from(u8::MAX))
                    .map(|byte| byte as u8)
                    .context("callback workflow data array contains an invalid byte")
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        Some(_) => anyhow::bail!("callback workflow data must be base64 or a byte array"),
    };
    anyhow::ensure!(
        data.len() <= 2 * 1024 * 1024,
        "callback workflow data exceeds 2 MiB"
    );
    let configured_protocol = tx.metadata.get("dtm.protocol").map(String::as_str);
    let protocol = if configured_protocol == Some("grpc") {
        WorkflowCallbackProtocol::Grpc
    } else if url.starts_with("http://") || url.starts_with("https://") {
        let legacy_json_rpc = configured_protocol.is_none()
            && reqwest::Url::parse(&url)?
                .query_pairs()
                .any(|(name, value)| name == "method" && !value.is_empty());
        if configured_protocol == Some("json-rpc") || legacy_json_rpc {
            WorkflowCallbackProtocol::JsonRpc
        } else {
            WorkflowCallbackProtocol::Http
        }
    } else {
        WorkflowCallbackProtocol::Grpc
    };
    Ok(WorkflowCallbackRequest {
        gid: tx.gid.clone(),
        url,
        operation,
        data,
        protocol,
    })
}

fn is_callback_workflow(tx: &Transaction) -> bool {
    tx.kind == TransactionKind::Workflow
        && tx.branches.is_empty()
        && tx
            .metadata
            .get("dtm.query_prepared")
            .is_some_and(|value| !value.is_empty())
}

fn callback_workflow_due(tx: &Transaction, now: u64) -> bool {
    tx.metadata
        .get("dtm.callback.next_retry_millis")
        .and_then(|value| value.parse::<u64>().ok())
        .is_none_or(|next| next <= now)
}

fn transaction_next_recovery_millis(tx: &Transaction) -> Option<u64> {
    if tx.status.is_terminal() {
        return None;
    }
    if is_callback_workflow(tx) {
        return tx
            .metadata
            .get("dtm.callback.next_retry_millis")
            .and_then(|value| value.parse::<u64>().ok());
    }

    let mut next = if tx.kind == TransactionKind::Message
        && tx.status == TransactionStatus::Succeeding
        && tx.branches.iter().all(|branch| branch.attempts == 0)
    {
        message_dispatch_at_millis(tx)
    } else {
        None
    };
    for branch in tx.branches.iter().filter(|branch| {
        matches!(
            branch.status,
            BranchStatus::Failed | BranchStatus::Running | BranchStatus::Compensating
        )
    }) {
        let branch_next = branch.next_retry_millis?;
        next = Some(next.map_or(branch_next, |current: u64| current.min(branch_next)));
    }
    next
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

fn bump_transaction_revision(transaction: &mut Transaction) {
    transaction.revision = transaction.revision.saturating_add(1);
    transaction.updated_at_millis = current_millis();
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

fn alert_branch_url(value: &str) -> String {
    let Ok(mut url) = parse_branch_url(value) else {
        return "redacted".to_owned();
    };
    // Query strings commonly carry signatures or credentials. The alert keeps
    // the upstream `branch` field while removing that sensitive component.
    url.set_query(None);
    url.into()
}

const fn transaction_status_name(status: TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Submitted => "submitted",
        TransactionStatus::Trying => "trying",
        TransactionStatus::Prepared => "prepared",
        TransactionStatus::Succeeding => "submitted",
        TransactionStatus::Succeeded => "succeed",
        TransactionStatus::Aborting => "aborting",
        TransactionStatus::Aborted | TransactionStatus::Failed => "failed",
    }
}

fn transaction_due(tx: &Transaction, now: u64) -> bool {
    if tx.kind == TransactionKind::Message
        && tx.status == TransactionStatus::Succeeding
        && !message_dispatch_due(tx, now)
    {
        return false;
    }
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

fn message_dispatch_at_millis(tx: &Transaction) -> Option<u64> {
    tx.options
        .delay_millis
        .map(|delay| tx.created_at_millis.saturating_add(delay))
}

fn message_dispatch_due(tx: &Transaction, now: u64) -> bool {
    message_dispatch_at_millis(tx).is_none_or(|dispatch_at| dispatch_at <= now)
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

    #[test]
    fn transaction_revision_is_monotonic_and_legacy_records_start_at_zero() {
        let transaction = Transaction::tcc("revision", Vec::new());
        assert_eq!(transaction.revision, 1);
        let mut legacy = serde_json::to_value(&transaction).expect("serialize transaction");
        legacy
            .as_object_mut()
            .expect("transaction object")
            .remove("revision");
        let mut legacy: Transaction =
            serde_json::from_value(legacy).expect("deserialize legacy transaction");
        assert_eq!(legacy.revision, 0);
        bump_transaction_revision(&mut legacy);
        assert_eq!(legacy.revision, 1);
    }

    #[tokio::test]
    async fn submit_normalizes_caller_supplied_revision() {
        let store = InMemoryTransactionStore::new();
        let dtm = Dtm::new(store.clone());
        let mut transaction = Transaction::tcc("revision-submit", Vec::new());
        transaction.revision = u64::MAX;

        let submitted = dtm.submit(transaction).await.expect("submit transaction");
        let stored = store
            .get_transaction("revision-submit")
            .await
            .expect("read transaction")
            .expect("stored transaction");

        assert_eq!(submitted.revision, 1);
        assert_eq!(stored.revision, 1);
    }

    #[derive(Clone)]
    struct FailingOnceInvoker {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CountingInvoker {
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

    #[derive(Clone)]
    struct StaticCallbackInvoker {
        result: WorkflowCallbackResult,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone, Default)]
    struct RecordingFailingAlertInvoker {
        alerts: Arc<Mutex<Vec<BranchFailureAlert>>>,
    }

    #[derive(Clone, Default)]
    struct ConcurrentSagaInvoker {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        events: Arc<Mutex<Vec<String>>>,
        alerts: Arc<Mutex<Vec<BranchFailureAlert>>>,
        fail_url: Option<String>,
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
    impl BranchInvoker for CountingInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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

    #[async_trait]
    impl BranchInvoker for StaticCallbackInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }

        async fn query_workflow_callback(
            &self,
            _request: &WorkflowCallbackRequest,
            _options: &TransactionOptions,
        ) -> anyhow::Result<WorkflowCallbackResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.result.clone())
        }
    }

    #[async_trait]
    impl BranchInvoker for RecordingFailingAlertInvoker {
        async fn invoke(&self, _url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            anyhow::bail!("injected branch failure")
        }

        async fn notify_branch_failure(&self, alert: &BranchFailureAlert) -> anyhow::Result<()> {
            self.alerts
                .lock()
                .expect("recording alert lock")
                .push(alert.clone());
            anyhow::bail!("injected alert delivery failure")
        }
    }

    #[async_trait]
    impl BranchInvoker for ConcurrentSagaInvoker {
        async fn invoke(&self, url: &str, _payload: &serde_json::Value) -> anyhow::Result<()> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            self.events
                .lock()
                .expect("concurrent Saga events lock")
                .push(format!("start:{url}"));
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.events
                .lock()
                .expect("concurrent Saga events lock")
                .push(format!("finish:{url}"));
            self.active.fetch_sub(1, Ordering::SeqCst);
            if self.fail_url.as_deref() == Some(url) {
                anyhow::bail!("injected concurrent Saga failure");
            }
            Ok(())
        }

        async fn notify_branch_failure(&self, alert: &BranchFailureAlert) -> anyhow::Result<()> {
            self.alerts
                .lock()
                .expect("concurrent Saga alerts lock")
                .push(alert.clone());
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

    #[test]
    fn concurrent_saga_dependencies_must_be_known_and_acyclic() {
        let mut transaction = Transaction::saga(
            "gid-saga-graph",
            vec![
                Branch::saga("a", "action-a", "compensate-a", serde_json::json!({})),
                Branch::saga("b", "action-b", "compensate-b", serde_json::json!({})),
            ],
        );
        transaction.branches[1].dependencies = vec!["a".to_owned()];
        assert!(validate_saga_dependencies(&transaction).is_err());

        transaction.options.concurrent = true;
        validate_saga_dependencies(&transaction).expect("valid concurrent Saga graph");
        transaction.branches[0].dependencies = vec!["b".to_owned()];
        assert!(validate_saga_dependencies(&transaction).is_err());
        transaction.branches[0].dependencies = vec!["missing".to_owned()];
        assert!(validate_saga_dependencies(&transaction).is_err());
    }

    #[test]
    fn workflow_dependencies_must_be_known_unique_and_acyclic() {
        let mut transaction = Transaction::workflow(
            "gid-workflow-graph",
            vec![
                Branch::workflow(
                    "a",
                    "action-a",
                    "compensate-a",
                    Vec::new(),
                    serde_json::json!({}),
                ),
                Branch::workflow(
                    "b",
                    "action-b",
                    "compensate-b",
                    vec!["a".to_owned()],
                    serde_json::json!({}),
                ),
            ],
        );
        validate_transaction_dependencies(&transaction).expect("valid Workflow graph");

        transaction.branches[0].dependencies = vec!["b".to_owned()];
        assert!(validate_transaction_dependencies(&transaction).is_err());
        transaction.branches[0].dependencies = vec!["missing".to_owned()];
        assert!(validate_transaction_dependencies(&transaction).is_err());
        transaction.branches[0].dependencies = vec!["a".to_owned()];
        assert!(validate_transaction_dependencies(&transaction).is_err());
        transaction.branches[0].dependencies = Vec::new();
        transaction.branches[1].dependencies = vec!["a".to_owned(), "a".to_owned()];
        assert!(validate_transaction_dependencies(&transaction).is_err());
    }

    #[tokio::test]
    async fn concurrent_saga_runs_ready_layers_and_compensates_reverse_dependencies() {
        let invoker = ConcurrentSagaInvoker {
            fail_url: Some("action-c".to_owned()),
            ..ConcurrentSagaInvoker::default()
        };
        let max_active = Arc::clone(&invoker.max_active);
        let events = Arc::clone(&invoker.events);
        let alerts = Arc::clone(&invoker.alerts);
        let dtm = Dtm::with_invoker(InMemoryTransactionStore::new(), invoker);
        let mut transaction = Transaction::saga(
            "gid-saga-concurrent",
            vec![
                Branch::saga("a", "action-a", "compensate-a", serde_json::json!({})),
                Branch::saga("b", "action-b", "compensate-b", serde_json::json!({})),
                Branch::saga("c", "action-c", "compensate-c", serde_json::json!({})),
            ],
        );
        transaction.options.concurrent = true;
        transaction.branches[2].dependencies = vec!["a".to_owned(), "b".to_owned()];
        dtm.submit(transaction)
            .await
            .expect("submit concurrent Saga");

        let aborted = dtm
            .start_saga("gid-saga-concurrent")
            .await
            .expect("run concurrent Saga");
        assert_eq!(aborted.status, TransactionStatus::Aborted);
        assert!(aborted
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Skipped));
        assert_eq!(max_active.load(Ordering::SeqCst), 2);

        let events = events.lock().expect("concurrent Saga events lock");
        let start_c = events
            .iter()
            .position(|event| event == "start:action-c")
            .expect("start action c");
        let finish_a = events
            .iter()
            .position(|event| event == "finish:action-a")
            .expect("finish action a");
        let finish_b = events
            .iter()
            .position(|event| event == "finish:action-b")
            .expect("finish action b");
        assert!(start_c > finish_a && start_c > finish_b);
        assert!(events.iter().any(|event| event == "start:compensate-a"));
        assert!(events.iter().any(|event| event == "start:compensate-b"));
        assert!(!events.iter().any(|event| event == "start:compensate-c"));
        drop(events);
        let alerts = alerts.lock().expect("concurrent Saga alerts lock");
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].gid, "gid-saga-concurrent");
        assert_eq!(alerts[0].status, "submitted");
        assert_eq!(alerts[0].branch, "redacted");
        assert_eq!(alerts[0].error, "branch_call_failed");
        assert_eq!(alerts[0].retry_count, 1);
    }

    #[test]
    fn alert_webhook_config_is_bounded_and_debug_redacts_url() {
        let config = AlertWebhookConfig {
            url: "https://alerts.example.com/dtm?token=secret".to_owned(),
            retry_limit: 3,
            timeout: Duration::from_secs(3),
        };
        config.validate().expect("valid alert webhook config");
        let config_debug = format!("{config:?}");
        assert!(config_debug.contains("[REDACTED]"));
        assert!(!config_debug.contains("alerts.example.com"));
        assert!(!config_debug.contains("secret"));
        let invoker = HttpBranchInvoker::with_timeout_policy_and_alert(
            Duration::from_secs(1),
            BranchUrlPolicy::allow_all(),
            Some(config),
        )
        .expect("alerting HTTP invoker");
        let debug = format!("{invoker:?}");
        assert!(debug.contains("alert_webhook_configured: true"));
        assert!(!debug.contains("alerts.example.com"));
        assert!(!debug.contains("secret"));

        assert_eq!(
            alert_branch_url("https://branch.example.com/action?signature=secret"),
            "https://branch.example.com/action"
        );
        assert!(AlertWebhookConfig {
            url: "file:///tmp/alert".to_owned(),
            retry_limit: 3,
            timeout: Duration::from_secs(3),
        }
        .validate()
        .is_err());
        assert!(!branch_alert_due(2, 3));
        assert!(branch_alert_due(3, 3));
        assert!(branch_alert_due(4, 3));
    }

    #[tokio::test]
    async fn alert_delivery_failure_does_not_change_branch_recovery_state() {
        let invoker = RecordingFailingAlertInvoker::default();
        let alerts = Arc::clone(&invoker.alerts);
        let dtm = Dtm::with_invoker(InMemoryTransactionStore::new(), invoker);
        dtm.submit(Transaction::tcc(
            "gid-alert-failure",
            vec![Branch::tcc_try(
                "inventory",
                "https://inventory.example.com/try?signature=secret",
                "https://inventory.example.com/confirm",
                "https://inventory.example.com/cancel",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit alert test transaction");

        let transaction = dtm
            .prepare_tcc("gid-alert-failure")
            .await
            .expect("branch failure remains a transaction result");

        assert_eq!(transaction.status, TransactionStatus::Trying);
        assert_eq!(transaction.branches[0].status, BranchStatus::Failed);
        let alerts = alerts.lock().expect("recorded alert lock");
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].branch, "https://inventory.example.com/try");
        assert_eq!(alerts[0].retry_count, 1);
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
        transaction
            .options
            .branch_headers
            .insert("x-transaction".to_owned(), "transaction-a".to_owned());
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

        assert!(dtm.schedule_submit("gid-tcc-not-prepared").await.is_err());
    }

    #[tokio::test]
    async fn prepared_xa_waits_for_an_explicit_global_decision() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::xa(
            "gid-xa-decision",
            vec![Branch::xa(
                "account",
                "https://account.example.com/xa",
                "https://account.example.com/xa",
                serde_json::json!({}),
            )],
        ))
        .await
        .expect("submit XA");
        dtm.prepare_xa("gid-xa-decision").await.expect("prepare XA");

        assert!(dtm
            .tick_recover_once()
            .await
            .expect("idle recovery")
            .is_empty());
        let prepared = dtm
            .store()
            .get_transaction("gid-xa-decision")
            .await
            .expect("read XA")
            .expect("XA exists");
        assert_eq!(prepared.status, TransactionStatus::Prepared);

        let scheduled = dtm
            .schedule_submit("gid-xa-decision")
            .await
            .expect("persist commit decision");
        assert_eq!(scheduled.status, TransactionStatus::Succeeding);
        let committed = dtm.tick_recover_once().await.expect("commit XA");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].status, TransactionStatus::Succeeded);
    }

    #[tokio::test]
    async fn prepared_tcc_and_message_wait_for_submit() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        dtm.submit(Transaction::tcc("gid-tcc-decision", Vec::new()))
            .await
            .expect("submit TCC");
        dtm.prepare_tcc("gid-tcc-decision")
            .await
            .expect("prepare TCC");
        dtm.submit(Transaction::message("gid-message-decision", Vec::new()))
            .await
            .expect("submit message");
        dtm.prepare_message("gid-message-decision")
            .await
            .expect("prepare message");

        assert!(dtm
            .tick_recover_once()
            .await
            .expect("idle recovery")
            .is_empty());
        assert_eq!(
            dtm.store()
                .get_transaction("gid-tcc-decision")
                .await
                .expect("read TCC")
                .expect("TCC exists")
                .status,
            TransactionStatus::Prepared
        );
        assert_eq!(
            dtm.store()
                .get_transaction("gid-message-decision")
                .await
                .expect("read message")
                .expect("message exists")
                .status,
            TransactionStatus::Prepared
        );

        assert_eq!(
            dtm.schedule_submit("gid-tcc-decision")
                .await
                .expect("persist TCC submit decision")
                .status,
            TransactionStatus::Succeeding
        );
        assert_eq!(
            dtm.schedule_submit("gid-message-decision")
                .await
                .expect("persist message submit decision")
                .status,
            TransactionStatus::Succeeding
        );
        let completed = dtm
            .tick_recover_once()
            .await
            .expect("apply submit decisions");
        assert_eq!(completed.len(), 2);
        assert!(completed
            .iter()
            .all(|transaction| transaction.status == TransactionStatus::Succeeded));
    }

    #[tokio::test]
    async fn callback_workflow_preserves_composite_progress_and_binary_data() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut workflow = Transaction::workflow("gid-callback-workflow", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        dtm.submit(workflow)
            .await
            .expect("submit callback workflow");
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

    #[test]
    fn callback_workflow_request_decodes_upstream_custom_data_and_grpc_target() {
        let mut workflow = Transaction::workflow("gid-callback-request", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "grpc://127.0.0.1:50051/workflow.Workflow/Execute".to_owned(),
        );
        workflow.metadata.insert(
            "dtm.custom_data".to_owned(),
            serde_json::json!({"name": "order", "data": "AP8B"}).to_string(),
        );
        workflow
            .metadata
            .insert("dtm.protocol".to_owned(), "grpc".to_owned());

        let callback = workflow
            .callback_workflow_request()
            .expect("callback request");
        assert_eq!(callback.gid, "gid-callback-request");
        assert_eq!(callback.operation, "order");
        assert_eq!(callback.data, vec![0, 255, 1]);
        assert_eq!(callback.protocol, WorkflowCallbackProtocol::Grpc);
        let target = parse_grpc_callback_target(&callback.url).expect("gRPC target");
        assert_eq!(target.endpoint, "http://127.0.0.1:50051");
        assert_eq!(target.method, "/workflow.Workflow/Execute");

        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "https://workflow.example.com/workflow.Workflow/Execute".to_owned(),
        );
        let callback = workflow
            .callback_workflow_request()
            .expect("HTTPS gRPC callback request");
        assert_eq!(callback.protocol, WorkflowCallbackProtocol::Grpc);
        let target = parse_grpc_callback_target(&callback.url).expect("HTTPS gRPC target");
        assert_eq!(target.endpoint, "https://workflow.example.com");
        assert_eq!(target.method, "/workflow.Workflow/Execute");
    }

    #[test]
    fn xa_phase2_callback_replaces_reserved_query_parameters() {
        let url = xa_phase2_callback_url(
            "https://business.example.com/xa?tenant=roze&gid=untrusted&op=action",
            "order-2026",
            "01",
            "commit",
        )
        .expect("XA phase-2 URL");
        let url = reqwest::Url::parse(&url).expect("parse XA phase-2 URL");
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        assert_eq!(query["tenant"], "roze");
        assert_eq!(query["gid"], "order-2026");
        assert_eq!(query["trans_type"], "xa");
        assert_eq!(query["branch_id"], "01");
        assert_eq!(query["op"], "commit");
    }

    #[test]
    fn callback_workflow_recovery_delay_persists_bounded_backoff() {
        let mut workflow = Transaction::workflow("gid-callback-backoff", Vec::new());
        workflow.status = TransactionStatus::Prepared;
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );

        defer_workflow_recovery(
            &mut workflow,
            WorkflowRecoveryDelay {
                attempted_at_millis: 1_000,
                retry_interval_millis: 100,
                max_retry_interval_millis: 800,
                backoff: true,
                outcome: "transport_error".to_owned(),
            },
        )
        .expect("first callback defer");
        assert_eq!(workflow.metadata["dtm.callback.attempts"], "1");
        assert_eq!(workflow.metadata["dtm.callback.next_retry_millis"], "1100");

        defer_workflow_recovery(
            &mut workflow,
            WorkflowRecoveryDelay {
                attempted_at_millis: 1_100,
                retry_interval_millis: 100,
                max_retry_interval_millis: 800,
                backoff: true,
                outcome: "transport_error".to_owned(),
            },
        )
        .expect("second callback defer");
        assert_eq!(workflow.metadata["dtm.callback.attempts"], "2");
        assert_eq!(workflow.metadata["dtm.callback.next_retry_millis"], "1300");
    }

    #[tokio::test]
    async fn callback_workflow_ongoing_result_is_persistently_deferred() {
        let calls = Arc::new(AtomicUsize::new(0));
        let dtm = Dtm::with_options(
            InMemoryTransactionStore::new(),
            StaticCallbackInvoker {
                result: WorkflowCallbackResult::Ongoing,
                calls: Arc::clone(&calls),
            },
            DtmOptions {
                retry_backoff_millis: 100,
                max_retry_backoff_millis: 800,
                ..DtmOptions::default()
            },
        );
        let mut workflow = Transaction::workflow("gid-callback-ongoing", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        workflow.metadata.insert(
            "dtm.custom_data".to_owned(),
            serde_json::json!({"name": "order", "data": ""}).to_string(),
        );
        dtm.submit(workflow)
            .await
            .expect("submit callback workflow");
        dtm.prepare_workflow("gid-callback-ongoing")
            .await
            .expect("prepare callback workflow");

        let deferred = dtm
            .recover_callback_workflow("gid-callback-ongoing")
            .await
            .expect("defer ongoing callback");
        assert_eq!(deferred.status, TransactionStatus::Prepared);
        assert_eq!(deferred.metadata["dtm.callback.attempts"], "1");
        assert_eq!(deferred.metadata["dtm.callback.last_outcome"], "ongoing");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(dtm.tick_recover_once().await.expect("not due").is_empty());
    }

    #[tokio::test]
    async fn callback_workflow_failure_result_becomes_terminal() {
        let dtm = Dtm::with_invoker(
            InMemoryTransactionStore::new(),
            StaticCallbackInvoker {
                result: WorkflowCallbackResult::Failed {
                    reason: Some("business failure".to_owned()),
                },
                calls: Arc::new(AtomicUsize::new(0)),
            },
        );
        let mut workflow = Transaction::workflow("gid-callback-recovered-failure", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        workflow.metadata.insert(
            "dtm.custom_data".to_owned(),
            serde_json::json!({"name": "order", "data": ""}).to_string(),
        );
        dtm.submit(workflow)
            .await
            .expect("submit callback workflow");
        dtm.prepare_workflow("gid-callback-recovered-failure")
            .await
            .expect("prepare callback workflow");

        let failed = dtm
            .recover_callback_workflow("gid-callback-recovered-failure")
            .await
            .expect("recover failed callback");
        assert_eq!(failed.status, TransactionStatus::Failed);
        assert_eq!(failed.metadata["rollback_reason"], "business failure");
    }

    #[tokio::test]
    async fn callback_workflow_completion_is_terminal_and_idempotent() {
        let dtm = Dtm::new(InMemoryTransactionStore::new());
        let mut workflow = Transaction::workflow("gid-callback-complete", Vec::new());
        workflow.metadata.insert(
            "dtm.query_prepared".to_owned(),
            "http://workflow/callback".to_owned(),
        );
        dtm.submit(workflow)
            .await
            .expect("submit callback workflow");
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
        assert_eq!(completed.metadata["dtm.workflow.result"], "cmVzdWx0");

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

        let fence = store
            .acquire_recovery_lease("recovery", "worker-a", 10_000)
            .await
            .expect("lease")
            .expect("lease acquired");
        assert_eq!(fence.name, "recovery");
        assert_eq!(fence.owner, "worker-a");
        assert_eq!(fence.epoch, 1);
        assert!(store
            .acquire_recovery_lease("recovery", "worker-b", 10_000)
            .await
            .expect("lease")
            .is_none());
        assert!(store
            .try_acquire_recovery_lease("recovery", "worker-a", 10_000)
            .await
            .expect("renew"));
    }

    #[tokio::test]
    async fn reset_retry_batch_after_honors_cutoff_limit_and_remaining_flag() {
        let store = InMemoryTransactionStore::new();
        let dtm = Dtm::new(store.clone());
        let now = current_millis();
        for (gid, status, next_retry_millis) in [
            ("retry-near", BranchStatus::Failed, now.saturating_add(500)),
            (
                "retry-far-1",
                BranchStatus::Running,
                now.saturating_add(10_000),
            ),
            (
                "retry-far-2",
                BranchStatus::Failed,
                now.saturating_add(20_000),
            ),
        ] {
            let mut branch = Branch::saga(
                "01",
                "http://inventory/action",
                "http://inventory/compensate",
                serde_json::json!({}),
            );
            branch.status = status;
            branch.next_retry_millis = Some(next_retry_millis);
            dtm.submit(Transaction::saga(gid, vec![branch]))
                .await
                .expect("submit scheduled retry");
        }

        let (first, has_remaining) = dtm
            .reset_retry_batch_after(1_000, 1)
            .await
            .expect("first reset batch");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].gid, "retry-far-1");
        assert!(has_remaining);

        let (second, has_remaining) = dtm
            .reset_retry_batch_after(1_000, 10)
            .await
            .expect("second reset batch");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].gid, "retry-far-2");
        assert!(!has_remaining);

        let near = store
            .get_transaction("retry-near")
            .await
            .expect("read near retry")
            .expect("near retry exists");
        assert_eq!(
            near.branches[0].next_retry_millis,
            Some(now.saturating_add(500))
        );
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
    async fn concurrent_message_delivers_all_branches_in_parallel() {
        let invoker = ConcurrentSagaInvoker::default();
        let max_active = Arc::clone(&invoker.max_active);
        let dtm = Dtm::with_invoker(InMemoryTransactionStore::new(), invoker);
        let mut message = Transaction::message(
            "gid-message-concurrent",
            vec![
                Branch::message("first", "http://events/first", serde_json::json!({})),
                Branch::message("second", "http://events/second", serde_json::json!({})),
            ],
        );
        message.options.concurrent = true;
        dtm.submit(message)
            .await
            .expect("submit concurrent message");

        let dispatched = dtm
            .dispatch_message("gid-message-concurrent")
            .await
            .expect("dispatch concurrent message");

        assert_eq!(dispatched.status, TransactionStatus::Succeeded);
        assert!(dispatched
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Succeeded));
        assert!(max_active.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn concurrent_message_retries_only_the_failed_branch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let dtm = Dtm::with_invoker(
            InMemoryTransactionStore::new(),
            FailingOnceInvoker {
                calls: Arc::clone(&calls),
            },
        );
        let mut message = Transaction::message(
            "gid-message-concurrent-retry",
            vec![
                Branch::message("first", "http://events/first", serde_json::json!({})),
                Branch::message("second", "http://events/second", serde_json::json!({})),
            ],
        );
        message.options.concurrent = true;
        dtm.submit(message)
            .await
            .expect("submit concurrent message");

        let failed = dtm
            .dispatch_message("gid-message-concurrent-retry")
            .await
            .expect("dispatch with one failure");
        assert_eq!(failed.status, TransactionStatus::Succeeding);
        assert_eq!(
            failed
                .branches
                .iter()
                .filter(|branch| branch.status == BranchStatus::Failed)
                .count(),
            1
        );
        assert_eq!(
            failed
                .branches
                .iter()
                .filter(|branch| branch.status == BranchStatus::Succeeded)
                .count(),
            1
        );

        let recovered = dtm
            .dispatch_message("gid-message-concurrent-retry")
            .await
            .expect("retry failed concurrent branch");
        assert_eq!(recovered.status, TransactionStatus::Succeeded);
        assert!(recovered
            .branches
            .iter()
            .all(|branch| branch.status == BranchStatus::Succeeded));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn delayed_message_persists_submit_decision_without_early_delivery() {
        let calls = Arc::new(AtomicUsize::new(0));
        let store = InMemoryTransactionStore::new();
        let dtm = Dtm::with_invoker(
            store.clone(),
            CountingInvoker {
                calls: Arc::clone(&calls),
            },
        );
        let mut message = Transaction::message(
            "gid-message-delay",
            vec![
                Branch::message(
                    "publish-primary",
                    "http://events/publish-primary",
                    serde_json::json!({}),
                ),
                Branch::message(
                    "publish-secondary",
                    "http://events/publish-secondary",
                    serde_json::json!({}),
                ),
            ],
        );
        message.options.concurrent = true;
        message.options.delay_millis = Some(60_000);
        let dispatch_at = message.created_at_millis + 60_000;
        dtm.submit(message).await.expect("submit delayed message");

        let delayed = dtm
            .dispatch_message("gid-message-delay")
            .await
            .expect("persist delayed dispatch decision");

        assert_eq!(delayed.status, TransactionStatus::Succeeding);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(dtm
            .tick_recover_once()
            .await
            .expect("skip early delayed recovery")
            .is_empty());
        assert_eq!(
            transaction_next_recovery_millis(&delayed),
            Some(dispatch_at)
        );
        assert!(!transaction_due(&delayed, dispatch_at - 1));
        assert!(transaction_due(&delayed, dispatch_at));
        assert_eq!(
            store
                .get_transaction("gid-message-delay")
                .await
                .expect("read delayed message")
                .expect("delayed message exists")
                .status,
            TransactionStatus::Succeeding
        );
    }

    #[tokio::test]
    async fn elapsed_message_delay_dispatches_normally() {
        let calls = Arc::new(AtomicUsize::new(0));
        let dtm = Dtm::with_invoker(
            InMemoryTransactionStore::new(),
            CountingInvoker {
                calls: Arc::clone(&calls),
            },
        );
        let mut message = Transaction::message(
            "gid-message-delay-elapsed",
            vec![Branch::message(
                "publish",
                "http://events/publish",
                serde_json::json!({}),
            )],
        );
        message.created_at_millis = 0;
        message.options.delay_millis = Some(1);
        dtm.submit(message)
            .await
            .expect("submit elapsed delayed message");

        let dispatched = dtm
            .dispatch_message("gid-message-delay-elapsed")
            .await
            .expect("dispatch elapsed delayed message");

        assert_eq!(dispatched.status, TransactionStatus::Succeeded);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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
        assert_eq!(transaction.branches[1].action, "http://warehouse/orders");
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
    fn branch_url_policy_applies_to_grpc_callback_targets() {
        let policy = BranchUrlPolicy::from_allowed_origins([
            "http://workflow:50051",
            "https://secure-workflow:443",
        ])
        .expect("policy");

        policy
            .validate_callback("workflow:50051/workflow.Workflow/Execute")
            .expect("allowed plaintext gRPC target");
        policy
            .validate_callback("grpcs://secure-workflow:443/workflow.Workflow/Execute")
            .expect("allowed TLS gRPC target");
        assert!(policy
            .validate_callback("other:50051/workflow.Workflow/Execute")
            .is_err());
        assert!(policy
            .validate_callback("workflow:50051/workflow.Workflow/Execute?token=secret")
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
