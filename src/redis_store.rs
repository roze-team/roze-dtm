use std::{collections::BTreeMap, fmt, future::Future, time::Duration};

use anyhow::Context as _;
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use roze_redis::{redis, RedisClient, RedisConnection};

use crate::{
    append_dynamic_branch, append_workflow_progress, apply_workflow_completion,
    bump_transaction_revision, defer_workflow_recovery, BarrierDecision, Branch, BranchBarrier,
    KvEntry, RecoveryLeaseFence, Transaction, TransactionStatus, TransactionStore,
    WorkflowProgress, WorkflowRecoveryDelay,
};

const TRANSACTION_MUTATION_ATTEMPTS: usize = 16;
const REDIS_SCAN_COUNT: usize = 256;
const MAX_REDIS_SCAN_ENTRIES: usize = 1_000_000;
pub const DEFAULT_REDIS_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

const CAS_HASH_FIELD: &str = r#"
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
  return -1
end
if current ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"#;

const DELETE_TRANSACTION_IF_UNCHANGED: &str = r#"
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
  return -1
end
if current ~= ARGV[2] then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
local fields = redis.call('SMEMBERS', KEYS[3])
if table.getn(fields) > 0 then
  redis.call('HDEL', KEYS[2], unpack(fields))
end
for index = 3, table.getn(ARGV) do
  redis.call('HDEL', KEYS[2], ARGV[index])
end
redis.call('DEL', KEYS[3])
return 1
"#;

const BARRIER_DECISION: &str = r#"
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  return 'duplicate'
end
if ARGV[4] == 'try' and redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
  return 'cancelled_try'
end
if ARGV[4] == 'cancel' and redis.call('HEXISTS', KEYS[1], ARGV[3]) == 0 then
  redis.call('HSET', KEYS[1], ARGV[1], '1')
  redis.call('SADD', KEYS[2], ARGV[1])
  return 'null_compensation'
end
redis.call('HSET', KEYS[1], ARGV[1], '1')
redis.call('SADD', KEYS[2], ARGV[1])
return 'execute'
"#;

const ACQUIRE_RECOVERY_LEASE: &str = r#"
local server_time = redis.call('TIME')
local now_millis = server_time[1] * 1000 + math.floor(server_time[2] / 1000)
local ttl_millis = tonumber(ARGV[5])
local current_owner = redis.call('HGET', KEYS[1], ARGV[1])
local current_expiry = tonumber(redis.call('HGET', KEYS[1], ARGV[2]) or '0')
local current_epoch = tonumber(redis.call('HGET', KEYS[1], ARGV[3]) or '0')
if current_owner and current_expiry > now_millis then
  if current_owner ~= ARGV[4] then
    return {0, current_epoch}
  end
  if current_epoch < 1 then
    current_epoch = 1
    redis.call('HSET', KEYS[1], ARGV[3], current_epoch)
  end
  redis.call('HSET', KEYS[1], ARGV[2], now_millis + ttl_millis)
  return {1, current_epoch}
end
local next_epoch = current_epoch + 1
redis.call(
  'HSET', KEYS[1],
  ARGV[1], ARGV[4],
  ARGV[2], now_millis + ttl_millis,
  ARGV[3], next_epoch
)
return {1, next_epoch}
"#;

const FENCED_CAS_HASH_FIELD: &str = r#"
local server_time = redis.call('TIME')
local now_millis = server_time[1] * 1000 + math.floor(server_time[2] / 1000)
local owner = redis.call('HGET', KEYS[2], ARGV[4])
local expiry = tonumber(redis.call('HGET', KEYS[2], ARGV[5]) or '0')
local epoch = tonumber(redis.call('HGET', KEYS[2], ARGV[6]) or '0')
if owner ~= ARGV[7] or expiry <= now_millis or epoch ~= tonumber(ARGV[8]) then
  return -2
end
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
  return -1
end
if current ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"#;

const FENCED_BARRIER_DECISION: &str = r#"
local server_time = redis.call('TIME')
local now_millis = server_time[1] * 1000 + math.floor(server_time[2] / 1000)
local owner = redis.call('HGET', KEYS[2], ARGV[5])
local expiry = tonumber(redis.call('HGET', KEYS[2], ARGV[6]) or '0')
local epoch = tonumber(redis.call('HGET', KEYS[2], ARGV[7]) or '0')
if owner ~= ARGV[8] or expiry <= now_millis or epoch ~= tonumber(ARGV[9]) then
  return 'fence_lost'
end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  return 'duplicate'
end
if ARGV[4] == 'try' and redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
  return 'cancelled_try'
end
if ARGV[4] == 'cancel' and redis.call('HEXISTS', KEYS[1], ARGV[3]) == 0 then
  redis.call('HSET', KEYS[1], ARGV[1], '1')
  redis.call('SADD', KEYS[3], ARGV[1])
  return 'null_compensation'
end
redis.call('HSET', KEYS[1], ARGV[1], '1')
redis.call('SADD', KEYS[3], ARGV[1])
return 'execute'
"#;

const FENCED_DELETE_HASH_FIELD: &str = r#"
local server_time = redis.call('TIME')
local now_millis = server_time[1] * 1000 + math.floor(server_time[2] / 1000)
local owner = redis.call('HGET', KEYS[2], ARGV[2])
local expiry = tonumber(redis.call('HGET', KEYS[2], ARGV[3]) or '0')
local epoch = tonumber(redis.call('HGET', KEYS[2], ARGV[4]) or '0')
if owner ~= ARGV[5] or expiry <= now_millis or epoch ~= tonumber(ARGV[6]) then
  return -2
end
local deleted = redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[1])
return deleted
"#;

#[derive(Clone)]
pub struct RedisTransactionStore {
    client: RedisClient,
    keys: RedisStoreKeys,
    operation_timeout: Duration,
}

#[derive(Debug, Clone)]
struct RedisStoreKeys {
    transactions: String,
    kv: String,
    barriers: String,
    barrier_index_prefix: String,
    leases: String,
}

impl fmt::Debug for RedisTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisTransactionStore")
            .field("client", &self.client)
            .field("keys", &self.keys)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl RedisTransactionStore {
    pub fn open(url: &str, namespace: &str) -> anyhow::Result<Self> {
        Self::open_with_timeout(url, namespace, DEFAULT_REDIS_OPERATION_TIMEOUT)
    }

    pub fn open_with_timeout(
        url: &str,
        namespace: &str,
        operation_timeout: Duration,
    ) -> anyhow::Result<Self> {
        Self::from_client_with_timeout(RedisClient::open(url)?, namespace, operation_timeout)
    }

    pub fn open_topology(
        url: &str,
        cluster_urls: &[String],
        namespace: &str,
    ) -> anyhow::Result<Self> {
        Self::open_topology_with_timeout(
            url,
            cluster_urls,
            namespace,
            DEFAULT_REDIS_OPERATION_TIMEOUT,
        )
    }

    pub fn open_topology_with_timeout(
        url: &str,
        cluster_urls: &[String],
        namespace: &str,
        operation_timeout: Duration,
    ) -> anyhow::Result<Self> {
        Self::from_client_with_timeout(
            RedisClient::open_topology(url, cluster_urls)?,
            namespace,
            operation_timeout,
        )
    }

    pub fn from_client(client: RedisClient, namespace: &str) -> anyhow::Result<Self> {
        Self::from_client_with_timeout(client, namespace, DEFAULT_REDIS_OPERATION_TIMEOUT)
    }

    pub fn from_client_with_timeout(
        client: RedisClient,
        namespace: &str,
        operation_timeout: Duration,
    ) -> anyhow::Result<Self> {
        validate_redis_namespace(namespace)?;
        anyhow::ensure!(
            !operation_timeout.is_zero(),
            "Redis DTM operation timeout must be greater than zero"
        );
        let slot = format!("{{{namespace}}}");
        let prefix = format!("roze-dtm:{slot}");
        Ok(Self {
            client,
            keys: RedisStoreKeys {
                transactions: format!("{prefix}:transactions"),
                kv: format!("{prefix}:kv"),
                barriers: format!("{prefix}:barriers"),
                barrier_index_prefix: format!("{prefix}:barrier-index:"),
                leases: format!("{prefix}:leases"),
            },
            operation_timeout,
        })
    }

    pub async fn health_check(&self) -> anyhow::Result<()> {
        let mut connection = self.connection().await?;
        let response: String = self
            .redis_call(
                "health check",
                redis::cmd("PING").query_async(&mut connection),
            )
            .await?;
        anyhow::ensure!(response == "PONG", "unexpected Redis PING response");
        Ok(())
    }

    async fn connection(&self) -> anyhow::Result<RedisConnection> {
        tokio::time::timeout(self.operation_timeout, self.client.connection())
            .await
            .with_context(|| {
                format!(
                    "Redis connection timed out after {} ms",
                    self.operation_timeout.as_millis()
                )
            })?
    }

    fn barrier_index_key(&self, gid: &str) -> anyhow::Result<String> {
        Ok(format!(
            "{}{}",
            self.keys.barrier_index_prefix,
            serde_json::to_string(gid).context("encode Redis barrier index key")?
        ))
    }

    async fn redis_call<T, F>(&self, operation: &str, future: F) -> anyhow::Result<T>
    where
        F: Future<Output = redis::RedisResult<T>>,
    {
        tokio::time::timeout(self.operation_timeout, future)
            .await
            .with_context(|| {
                format!(
                    "Redis {operation} timed out after {} ms",
                    self.operation_timeout.as_millis()
                )
            })?
            .map_err(Into::into)
    }

    async fn transaction_payload(&self, gid: &str) -> anyhow::Result<Option<String>> {
        let mut connection = self.connection().await?;
        self.redis_call(
            "transaction lookup",
            redis::cmd("HGET")
                .arg(&self.keys.transactions)
                .arg(gid)
                .query_async(&mut connection),
        )
        .await
    }

    async fn hash_values(&self, hash: &str) -> anyhow::Result<Vec<String>> {
        let mut cursor = 0_u64;
        let mut values = BTreeMap::new();
        let mut connection = self.connection().await?;
        loop {
            let (next_cursor, entries): (u64, Vec<(String, String)>) = self
                .redis_call(
                    "hash scan",
                    redis::cmd("HSCAN")
                        .arg(hash)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(REDIS_SCAN_COUNT)
                        .query_async(&mut connection),
                )
                .await?;
            values.extend(entries);
            anyhow::ensure!(
                values.len() <= MAX_REDIS_SCAN_ENTRIES,
                "Redis DTM scan exceeds {MAX_REDIS_SCAN_ENTRIES} entries"
            );
            cursor = next_cursor;
            if cursor == 0 {
                return Ok(values.into_values().collect());
            }
        }
    }

    async fn mutate_transaction<F>(
        &self,
        gid: &str,
        operation: &str,
        mut mutate: F,
    ) -> anyhow::Result<Transaction>
    where
        F: FnMut(&mut Transaction) -> anyhow::Result<()>,
    {
        for _ in 0..TRANSACTION_MUTATION_ATTEMPTS {
            let previous_payload = self
                .transaction_payload(gid)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let mut transaction: Transaction = serde_json::from_str(&previous_payload)?;
            mutate(&mut transaction)?;
            let payload = serde_json::to_string(&transaction)?;
            let mut connection = self.connection().await?;
            let changed: i64 = self
                .redis_call(
                    "transaction compare-and-swap",
                    redis::Script::new(CAS_HASH_FIELD)
                        .key(&self.keys.transactions)
                        .arg(gid)
                        .arg(&previous_payload)
                        .arg(payload)
                        .invoke_async(&mut connection),
                )
                .await?;
            match changed {
                1 => return Ok(transaction),
                0 => continue,
                -1 => anyhow::bail!("transaction not found: {gid}"),
                other => anyhow::bail!("unknown Redis transaction CAS result: {other}"),
            }
        }
        anyhow::bail!("transaction {gid} {operation} is contended")
    }

    async fn mutate_transaction_fenced<F>(
        &self,
        gid: &str,
        operation: &str,
        fence: &RecoveryLeaseFence,
        mut mutate: F,
    ) -> anyhow::Result<Transaction>
    where
        F: FnMut(&mut Transaction) -> anyhow::Result<()>,
    {
        validate_recovery_fence(fence)?;
        let (owner_field, expiry_field, epoch_field) = lease_fields(&fence.name);
        for _ in 0..TRANSACTION_MUTATION_ATTEMPTS {
            let previous_payload = self
                .transaction_payload(gid)
                .await?
                .ok_or_else(|| anyhow::anyhow!("transaction not found: {gid}"))?;
            let mut transaction: Transaction = serde_json::from_str(&previous_payload)?;
            mutate(&mut transaction)?;
            let payload = serde_json::to_string(&transaction)?;
            let mut connection = self.connection().await?;
            let changed: i64 = self
                .redis_call(
                    "fenced transaction compare-and-swap",
                    redis::Script::new(FENCED_CAS_HASH_FIELD)
                        .key(&self.keys.transactions)
                        .key(&self.keys.leases)
                        .arg(gid)
                        .arg(&previous_payload)
                        .arg(payload)
                        .arg(&owner_field)
                        .arg(&expiry_field)
                        .arg(&epoch_field)
                        .arg(&fence.owner)
                        .arg(fence.epoch)
                        .invoke_async(&mut connection),
                )
                .await?;
            match changed {
                1 => return Ok(transaction),
                0 => continue,
                -1 => anyhow::bail!("transaction not found: {gid}"),
                -2 => anyhow::bail!("Redis recovery lease fence is no longer valid"),
                other => anyhow::bail!("unknown Redis fenced transaction CAS result: {other}"),
            }
        }
        anyhow::bail!("transaction {gid} {operation} is contended")
    }

    async fn update_transaction_fenced_inner(
        &self,
        mut transaction: Transaction,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        validate_recovery_fence(fence)?;
        let expected_revision = transaction.revision;
        let previous_payload = self
            .transaction_payload(&transaction.gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {}", transaction.gid))?;
        let current: Transaction = serde_json::from_str(&previous_payload)?;
        anyhow::ensure!(
            current.revision == expected_revision,
            "Redis transaction revision conflict"
        );
        bump_transaction_revision(&mut transaction);
        let payload = serde_json::to_string(&transaction)?;
        let (owner_field, expiry_field, epoch_field) = lease_fields(&fence.name);
        let mut connection = self.connection().await?;
        let changed: i64 = self
            .redis_call(
                "fenced transaction update",
                redis::Script::new(FENCED_CAS_HASH_FIELD)
                    .key(&self.keys.transactions)
                    .key(&self.keys.leases)
                    .arg(&transaction.gid)
                    .arg(previous_payload)
                    .arg(payload)
                    .arg(owner_field)
                    .arg(expiry_field)
                    .arg(epoch_field)
                    .arg(&fence.owner)
                    .arg(fence.epoch)
                    .invoke_async(&mut connection),
            )
            .await?;
        match changed {
            1 => Ok(()),
            0 => anyhow::bail!("Redis transaction revision conflict"),
            -1 => anyhow::bail!("transaction not found: {}", transaction.gid),
            -2 => anyhow::bail!("Redis recovery lease fence is no longer valid"),
            other => anyhow::bail!("unknown Redis fenced transaction update result: {other}"),
        }
    }
}

#[async_trait]
impl TransactionStore for RedisTransactionStore {
    async fn insert_transaction(&self, transaction: Transaction) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&transaction)?;
        let mut connection = self.connection().await?;
        let inserted: i64 = self
            .redis_call(
                "transaction insert",
                redis::cmd("HSETNX")
                    .arg(&self.keys.transactions)
                    .arg(&transaction.gid)
                    .arg(payload)
                    .query_async(&mut connection),
            )
            .await?;
        anyhow::ensure!(
            inserted == 1,
            "transaction already exists: {}",
            transaction.gid
        );
        Ok(())
    }

    async fn get_transaction(&self, gid: &str) -> anyhow::Result<Option<Transaction>> {
        self.transaction_payload(gid)
            .await?
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .transpose()
    }

    async fn update_transaction(&self, mut transaction: Transaction) -> anyhow::Result<()> {
        let expected_revision = transaction.revision;
        let previous_payload = self
            .transaction_payload(&transaction.gid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transaction not found: {}", transaction.gid))?;
        let current: Transaction = serde_json::from_str(&previous_payload)?;
        anyhow::ensure!(
            current.revision == expected_revision,
            "Redis transaction revision conflict"
        );
        bump_transaction_revision(&mut transaction);
        let payload = serde_json::to_string(&transaction)?;
        let mut connection = self.connection().await?;
        let changed: i64 = self
            .redis_call(
                "transaction update",
                redis::Script::new(CAS_HASH_FIELD)
                    .key(&self.keys.transactions)
                    .arg(&transaction.gid)
                    .arg(previous_payload)
                    .arg(payload)
                    .invoke_async(&mut connection),
            )
            .await?;
        match changed {
            1 => Ok(()),
            0 => anyhow::bail!("Redis transaction revision conflict"),
            -1 => anyhow::bail!("transaction not found: {}", transaction.gid),
            other => anyhow::bail!("unknown Redis transaction update result: {other}"),
        }
    }

    async fn update_transaction_fenced(
        &self,
        transaction: Transaction,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        self.update_transaction_fenced_inner(transaction, fence)
            .await
    }

    async fn register_branch(&self, gid: &str, branch: Branch) -> anyhow::Result<Transaction> {
        self.mutate_transaction(gid, "branch registration", |transaction| {
            append_dynamic_branch(transaction, branch.clone())
        })
        .await
    }

    async fn record_workflow_progress(
        &self,
        gid: &str,
        progress: WorkflowProgress,
    ) -> anyhow::Result<Transaction> {
        self.mutate_transaction(gid, "workflow progress update", |transaction| {
            append_workflow_progress(transaction, progress.clone())
        })
        .await
    }

    async fn record_workflow_progress_fenced(
        &self,
        gid: &str,
        progress: WorkflowProgress,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        self.mutate_transaction_fenced(gid, "workflow progress update", fence, |transaction| {
            append_workflow_progress(transaction, progress.clone())
        })
        .await
    }

    async fn finish_workflow(
        &self,
        gid: &str,
        status: TransactionStatus,
        rollback_reason: Option<String>,
        result: Option<String>,
    ) -> anyhow::Result<Transaction> {
        self.mutate_transaction(gid, "workflow completion", |transaction| {
            apply_workflow_completion(transaction, status, rollback_reason.clone(), result.clone())
        })
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
        self.mutate_transaction_fenced(gid, "workflow completion", fence, |transaction| {
            apply_workflow_completion(transaction, status, rollback_reason.clone(), result.clone())
        })
        .await
    }

    async fn defer_workflow_recovery(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
    ) -> anyhow::Result<Transaction> {
        self.mutate_transaction(gid, "workflow recovery update", |transaction| {
            defer_workflow_recovery(transaction, delay.clone())
        })
        .await
    }

    async fn defer_workflow_recovery_fenced(
        &self,
        gid: &str,
        delay: WorkflowRecoveryDelay,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<Transaction> {
        self.mutate_transaction_fenced(gid, "workflow recovery update", fence, |transaction| {
            defer_workflow_recovery(transaction, delay.clone())
        })
        .await
    }

    async fn list_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let payloads = self.hash_values(&self.keys.transactions).await?;
        let mut transactions = payloads
            .into_iter()
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .collect::<anyhow::Result<Vec<Transaction>>>()?;
        transactions.sort_by(|left, right| {
            left.updated_at_millis
                .cmp(&right.updated_at_millis)
                .then_with(|| left.gid.cmp(&right.gid))
        });
        Ok(transactions)
    }

    async fn delete_transaction_if_unchanged(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<bool> {
        let Some(payload) = self.transaction_payload(&transaction.gid).await? else {
            return Ok(false);
        };
        let current: Transaction = serde_json::from_str(&payload)?;
        if &current != transaction {
            return Ok(false);
        }
        let mut connection = self.connection().await?;
        let deleted: i64 = self
            .redis_call(
                "transaction retention delete",
                redis::Script::new(DELETE_TRANSACTION_IF_UNCHANGED)
                    .key(&self.keys.transactions)
                    .key(&self.keys.barriers)
                    .key(self.barrier_index_key(&transaction.gid)?)
                    .arg(&transaction.gid)
                    .arg(payload)
                    .arg(transaction_barrier_fields(transaction)?)
                    .invoke_async(&mut connection),
            )
            .await?;
        match deleted {
            1 => Ok(true),
            0 | -1 => Ok(false),
            other => anyhow::bail!("unknown Redis transaction retention result: {other}"),
        }
    }

    async fn get_kv(&self, category: &str, key: &str) -> anyhow::Result<Option<KvEntry>> {
        let field = kv_field(category, key);
        let mut connection = self.connection().await?;
        let payload: Option<String> = self
            .redis_call(
                "KV lookup",
                redis::cmd("HGET")
                    .arg(&self.keys.kv)
                    .arg(field)
                    .query_async(&mut connection),
            )
            .await?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .transpose()
    }

    async fn list_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let payloads = self.hash_values(&self.keys.kv).await?;
        let mut entries = payloads
            .into_iter()
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .collect::<anyhow::Result<Vec<KvEntry>>>()?;
        entries.retain(|entry| {
            category.is_none_or(|category| entry.category == category)
                && key.is_none_or(|key| entry.key == key)
        });
        entries.sort_by(|left, right| {
            left.category
                .cmp(&right.category)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(entries.into_iter().skip(offset).take(limit).collect())
    }

    async fn create_kv(&self, entry: KvEntry) -> anyhow::Result<bool> {
        let field = kv_field(&entry.category, &entry.key);
        let payload = serde_json::to_string(&entry)?;
        let mut connection = self.connection().await?;
        let inserted: i64 = self
            .redis_call(
                "KV insert",
                redis::cmd("HSETNX")
                    .arg(&self.keys.kv)
                    .arg(field)
                    .arg(payload)
                    .query_async(&mut connection),
            )
            .await?;
        Ok(inserted == 1)
    }

    async fn update_kv(&self, entry: KvEntry, expected_version: u64) -> anyhow::Result<bool> {
        anyhow::ensure!(
            entry.version == expected_version.saturating_add(1),
            "invalid KV version transition"
        );
        let field = kv_field(&entry.category, &entry.key);
        let mut connection = self.connection().await?;
        let previous_payload: Option<String> = self
            .redis_call(
                "KV version lookup",
                redis::cmd("HGET")
                    .arg(&self.keys.kv)
                    .arg(&field)
                    .query_async(&mut connection),
            )
            .await?;
        let Some(previous_payload) = previous_payload else {
            return Ok(false);
        };
        let previous: KvEntry = serde_json::from_str(&previous_payload)?;
        if previous.version != expected_version {
            return Ok(false);
        }
        let payload = serde_json::to_string(&entry)?;
        let changed: i64 = self
            .redis_call(
                "KV compare-and-swap",
                redis::Script::new(CAS_HASH_FIELD)
                    .key(&self.keys.kv)
                    .arg(field)
                    .arg(previous_payload)
                    .arg(payload)
                    .invoke_async(&mut connection),
            )
            .await?;
        match changed {
            1 => Ok(true),
            0 | -1 => Ok(false),
            other => anyhow::bail!("unknown Redis KV CAS result: {other}"),
        }
    }

    async fn delete_kv(&self, category: &str, key: &str) -> anyhow::Result<bool> {
        let mut connection = self.connection().await?;
        let deleted: i64 = self
            .redis_call(
                "KV delete",
                redis::cmd("HDEL")
                    .arg(&self.keys.kv)
                    .arg(kv_field(category, key))
                    .query_async(&mut connection),
            )
            .await?;
        Ok(deleted == 1)
    }

    async fn barrier(&self, barrier: BranchBarrier) -> anyhow::Result<BarrierDecision> {
        let field = barrier_field(&barrier.gid, &barrier.branch_id, &barrier.op)?;
        let cancel_field = barrier_field(&barrier.gid, &barrier.branch_id, "cancel")?;
        let try_field = barrier_field(&barrier.gid, &barrier.branch_id, "try")?;
        let mut connection = self.connection().await?;
        let decision: String = self
            .redis_call(
                "barrier decision",
                redis::Script::new(BARRIER_DECISION)
                    .key(&self.keys.barriers)
                    .key(self.barrier_index_key(&barrier.gid)?)
                    .arg(field)
                    .arg(cancel_field)
                    .arg(try_field)
                    .arg(&barrier.op)
                    .invoke_async(&mut connection),
            )
            .await?;
        match decision.as_str() {
            "execute" => Ok(BarrierDecision::Execute),
            "duplicate" => Ok(BarrierDecision::SkipDuplicate),
            "null_compensation" => Ok(BarrierDecision::SkipNullCompensation),
            "cancelled_try" => Ok(BarrierDecision::SkipCancelledTry),
            other => anyhow::bail!("unknown Redis barrier decision: {other}"),
        }
    }

    async fn barrier_fenced(
        &self,
        barrier: BranchBarrier,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<BarrierDecision> {
        validate_recovery_fence(fence)?;
        let field = barrier_field(&barrier.gid, &barrier.branch_id, &barrier.op)?;
        let cancel_field = barrier_field(&barrier.gid, &barrier.branch_id, "cancel")?;
        let try_field = barrier_field(&barrier.gid, &barrier.branch_id, "try")?;
        let (owner_field, expiry_field, epoch_field) = lease_fields(&fence.name);
        let mut connection = self.connection().await?;
        let decision: String = self
            .redis_call(
                "fenced barrier decision",
                redis::Script::new(FENCED_BARRIER_DECISION)
                    .key(&self.keys.barriers)
                    .key(&self.keys.leases)
                    .key(self.barrier_index_key(&barrier.gid)?)
                    .arg(field)
                    .arg(cancel_field)
                    .arg(try_field)
                    .arg(&barrier.op)
                    .arg(owner_field)
                    .arg(expiry_field)
                    .arg(epoch_field)
                    .arg(&fence.owner)
                    .arg(fence.epoch)
                    .invoke_async(&mut connection),
            )
            .await?;
        match decision.as_str() {
            "execute" => Ok(BarrierDecision::Execute),
            "duplicate" => Ok(BarrierDecision::SkipDuplicate),
            "null_compensation" => Ok(BarrierDecision::SkipNullCompensation),
            "cancelled_try" => Ok(BarrierDecision::SkipCancelledTry),
            "fence_lost" => anyhow::bail!("Redis recovery lease fence is no longer valid"),
            other => anyhow::bail!("unknown Redis fenced barrier decision: {other}"),
        }
    }

    async fn release_barrier(&self, barrier: &BranchBarrier) -> anyhow::Result<()> {
        let field = barrier_field(&barrier.gid, &barrier.branch_id, &barrier.op)?;
        let mut connection = self.connection().await?;
        let _: i64 = self
            .redis_call(
                "barrier release",
                redis::cmd("HDEL")
                    .arg(&self.keys.barriers)
                    .arg(&field)
                    .query_async(&mut connection),
            )
            .await?;
        let _: i64 = self
            .redis_call(
                "barrier index release",
                redis::cmd("SREM")
                    .arg(self.barrier_index_key(&barrier.gid)?)
                    .arg(field)
                    .query_async(&mut connection),
            )
            .await?;
        Ok(())
    }

    async fn release_barrier_fenced(
        &self,
        barrier: &BranchBarrier,
        fence: &RecoveryLeaseFence,
    ) -> anyhow::Result<()> {
        validate_recovery_fence(fence)?;
        let (owner_field, expiry_field, epoch_field) = lease_fields(&fence.name);
        let mut connection = self.connection().await?;
        let deleted: i64 = self
            .redis_call(
                "fenced barrier release",
                redis::Script::new(FENCED_DELETE_HASH_FIELD)
                    .key(&self.keys.barriers)
                    .key(&self.keys.leases)
                    .key(self.barrier_index_key(&barrier.gid)?)
                    .arg(barrier_field(
                        &barrier.gid,
                        &barrier.branch_id,
                        &barrier.op,
                    )?)
                    .arg(owner_field)
                    .arg(expiry_field)
                    .arg(epoch_field)
                    .arg(&fence.owner)
                    .arg(fence.epoch)
                    .invoke_async(&mut connection),
            )
            .await?;
        match deleted {
            0 | 1 => Ok(()),
            -2 => anyhow::bail!("Redis recovery lease fence is no longer valid"),
            other => anyhow::bail!("unknown Redis fenced barrier release result: {other}"),
        }
    }

    async fn try_acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            !name.is_empty() && name.len() <= 128,
            "invalid Redis recovery lease name"
        );
        anyhow::ensure!(
            !owner.is_empty() && owner.len() <= 128,
            "invalid Redis recovery lease owner"
        );
        anyhow::ensure!(
            (1..=86_400_000).contains(&ttl_millis),
            "invalid Redis recovery lease TTL"
        );
        Ok(self
            .acquire_recovery_lease(name, owner, ttl_millis)
            .await?
            .is_some())
    }

    async fn acquire_recovery_lease(
        &self,
        name: &str,
        owner: &str,
        ttl_millis: u64,
    ) -> anyhow::Result<Option<RecoveryLeaseFence>> {
        validate_recovery_lease_input(name, owner, ttl_millis)?;
        let (owner_field, expiry_field, epoch_field) = lease_fields(name);
        let mut connection = self.connection().await?;
        let (acquired, epoch): (i64, i64) = self
            .redis_call(
                "recovery lease acquisition",
                redis::Script::new(ACQUIRE_RECOVERY_LEASE)
                    .key(&self.keys.leases)
                    .arg(owner_field)
                    .arg(expiry_field)
                    .arg(epoch_field)
                    .arg(owner)
                    .arg(ttl_millis)
                    .invoke_async(&mut connection),
            )
            .await?;
        match acquired {
            0 => Ok(None),
            1 => Ok(Some(RecoveryLeaseFence {
                name: name.to_owned(),
                owner: owner.to_owned(),
                epoch: epoch
                    .try_into()
                    .context("invalid Redis recovery lease epoch")?,
            })),
            other => anyhow::bail!("unknown Redis recovery lease result: {other}"),
        }
    }
}

pub fn validate_redis_namespace(namespace: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !namespace.is_empty()
            && namespace.len() <= 64
            && namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "Redis DTM namespace must contain 1 to 64 ASCII letters, digits, '-' or '_'"
    );
    Ok(())
}

fn validate_recovery_lease_input(name: &str, owner: &str, ttl_millis: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty() && name.len() <= 128,
        "invalid Redis recovery lease name"
    );
    anyhow::ensure!(
        !owner.is_empty() && owner.len() <= 128,
        "invalid Redis recovery lease owner"
    );
    anyhow::ensure!(
        (1..=86_400_000).contains(&ttl_millis),
        "invalid Redis recovery lease TTL"
    );
    Ok(())
}

fn validate_recovery_fence(fence: &RecoveryLeaseFence) -> anyhow::Result<()> {
    validate_recovery_lease_input(&fence.name, &fence.owner, 1)?;
    anyhow::ensure!(fence.epoch > 0, "invalid Redis recovery lease epoch");
    Ok(())
}

fn lease_fields(name: &str) -> (String, String, String) {
    let lease = URL_SAFE_NO_PAD.encode(name.as_bytes());
    (
        format!("{lease}:owner"),
        format!("{lease}:expiry"),
        format!("{lease}:epoch"),
    )
}

fn kv_field(category: &str, key: &str) -> String {
    format!("{}:{category}{key}", category.len())
}

fn barrier_field(gid: &str, branch_id: &str, op: &str) -> anyhow::Result<String> {
    serde_json::to_string(&(gid, branch_id, op)).context("encode Redis barrier field")
}

fn transaction_barrier_fields(transaction: &Transaction) -> anyhow::Result<Vec<String>> {
    let mut fields = Vec::with_capacity(transaction.branches.len().saturating_mul(3));
    for branch in &transaction.branches {
        let operations: &[&str] = match branch.kind {
            crate::BranchKind::SagaAction | crate::BranchKind::SagaCompensate => {
                &["action", "compensate"]
            }
            crate::BranchKind::TccTry
            | crate::BranchKind::TccConfirm
            | crate::BranchKind::TccCancel => &["try", "confirm", "cancel"],
            crate::BranchKind::WorkflowAction => &["workflow", "workflow_rollback"],
            crate::BranchKind::MessageAction => &["message"],
            crate::BranchKind::XaAction => &["commit", "rollback"],
        };
        for operation in operations {
            fields.push(barrier_field(&transaction.gid, &branch.id, operation)?);
        }
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{BranchKind, BranchStatus};

    fn test_namespace(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        format!("{prefix}_{}_{nanos}", std::process::id())
    }

    fn cluster_urls() -> Vec<String> {
        std::env::var("ROZE_TEST_REDIS_CLUSTER_URLS")
            .expect("ROZE_TEST_REDIS_CLUSTER_URLS is required")
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    fn cluster_namespace() -> String {
        std::env::var("ROZE_TEST_REDIS_NAMESPACE")
            .unwrap_or_else(|_| test_namespace("cluster_test"))
    }

    async fn cluster_admin_connection(url: &str) -> RedisConnection {
        RedisClient::open(url)
            .expect("open Redis Cluster admin client")
            .connection()
            .await
            .expect("connect Redis Cluster admin client")
    }

    async fn set_cluster_slot(url: &str, slot: u16, state: &str, node_id: Option<&str>) {
        let mut connection = cluster_admin_connection(url).await;
        let mut command = redis::cmd("CLUSTER");
        command.arg("SETSLOT").arg(slot).arg(state);
        if let Some(node_id) = node_id {
            command.arg(node_id);
        }
        let response: String = command
            .query_async(&mut connection)
            .await
            .expect("set Redis Cluster slot state");
        assert_eq!(response, "OK");
    }

    async fn assign_cluster_slot(
        master_urls: &[String],
        first_url: &str,
        last_url: &str,
        slot: u16,
        node_id: &str,
    ) {
        set_cluster_slot(first_url, slot, "NODE", Some(node_id)).await;
        for url in master_urls {
            if url != first_url && url != last_url {
                set_cluster_slot(url, slot, "NODE", Some(node_id)).await;
            }
        }
        set_cluster_slot(last_url, slot, "NODE", Some(node_id)).await;
    }

    #[test]
    fn redis_keys_share_one_explicit_cluster_hash_tag() {
        let store = RedisTransactionStore::open("redis://127.0.0.1/", "orders-prod")
            .expect("construct Redis store");
        for key in [
            &store.keys.transactions,
            &store.keys.kv,
            &store.keys.barriers,
            &store.keys.barrier_index_prefix,
            &store.keys.leases,
        ] {
            assert!(key.contains("{orders-prod}"));
        }
        assert!(validate_redis_namespace("unsafe{slot}").is_err());
        assert!(validate_redis_namespace("").is_err());
        assert!(RedisTransactionStore::open_with_timeout(
            "redis://127.0.0.1/",
            "orders-prod",
            Duration::ZERO,
        )
        .is_err());
    }

    #[test]
    fn redis_fields_are_unambiguous() {
        assert_ne!(kv_field("ab", "c"), kv_field("a", "bc"));
        assert_ne!(
            barrier_field("a:b", "c", "try").expect("field"),
            barrier_field("a", "b:c", "try").expect("field")
        );
        assert_ne!(lease_fields("ab").0, lease_fields("a").0);
        let transaction = Transaction::tcc(
            "legacy-gid",
            vec![Branch::tcc_try(
                "inventory",
                "http://inventory/try",
                "http://inventory/confirm",
                "http://inventory/cancel",
                serde_json::json!({}),
            )],
        );
        let fields = transaction_barrier_fields(&transaction).expect("legacy barrier fields");
        assert_eq!(fields.len(), 3);
        for operation in ["try", "confirm", "cancel"] {
            assert!(fields.contains(
                &barrier_field("legacy-gid", "inventory", operation).expect("barrier field")
            ));
        }
    }

    #[test]
    fn recovery_fencing_scripts_bind_owner_epoch_expiry_and_server_time() {
        for script in [
            FENCED_CAS_HASH_FIELD,
            FENCED_BARRIER_DECISION,
            FENCED_DELETE_HASH_FIELD,
        ] {
            assert!(script.contains("redis.call('TIME')"));
            assert!(script.contains("owner"));
            assert!(script.contains("expiry"));
            assert!(script.contains("epoch"));
            assert!(script.contains("now_millis"));
        }
        assert!(ACQUIRE_RECOVERY_LEASE.contains("next_epoch = current_epoch + 1"));
        assert!(DELETE_TRANSACTION_IF_UNCHANGED.contains("SMEMBERS"));
        assert!(!DELETE_TRANSACTION_IF_UNCHANGED.contains("HKEYS"));
        assert!(validate_recovery_fence(&RecoveryLeaseFence {
            name: "recovery".to_owned(),
            owner: "worker-a".to_owned(),
            epoch: 1,
        })
        .is_ok());
        assert!(validate_recovery_fence(&RecoveryLeaseFence {
            name: "recovery".to_owned(),
            owner: "worker-a".to_owned(),
            epoch: 0,
        })
        .is_err());
    }

    #[tokio::test]
    #[ignore = "requires ROZE_TEST_REDIS_URL"]
    async fn redis_store_round_trip_against_real_service() {
        let url = std::env::var("ROZE_TEST_REDIS_URL").expect("ROZE_TEST_REDIS_URL is required");
        let namespace = test_namespace("test");
        let store = RedisTransactionStore::open(&url, &namespace).expect("open Redis store");
        store.health_check().await.expect("Redis health check");

        let transaction = Transaction::tcc("redis-gid", Vec::new());
        store
            .insert_transaction(transaction)
            .await
            .expect("insert transaction");
        let branch = Branch {
            id: "01".to_owned(),
            kind: BranchKind::TccConfirm,
            action: "http://inventory/try".to_owned(),
            compensate: None,
            confirm: Some("http://inventory/confirm".to_owned()),
            cancel: Some("http://inventory/cancel".to_owned()),
            payload: serde_json::json!({}),
            status: BranchStatus::Succeeded,
            attempts: 1,
            last_error: None,
            next_retry_millis: None,
            dependencies: Vec::new(),
        };
        let transaction = store
            .register_branch("redis-gid", branch)
            .await
            .expect("register branch");
        assert_eq!(transaction.branches.len(), 1);

        let entry = KvEntry::new("topic", "orders", "[]");
        assert!(store.create_kv(entry.clone()).await.expect("create KV"));
        let mut updated = entry;
        updated.version = 2;
        assert!(store.update_kv(updated, 1).await.expect("update KV"));
        let fence = store
            .acquire_recovery_lease("recovery", "worker-a", 5_000)
            .await
            .expect("acquire lease")
            .expect("lease acquired");
        let renewed = store
            .acquire_recovery_lease("recovery", "worker-a", 5_000)
            .await
            .expect("renew lease")
            .expect("lease renewed");
        assert_eq!(renewed.epoch, fence.epoch);
        assert!(store
            .acquire_recovery_lease("recovery", "worker-b", 5_000)
            .await
            .expect("reject competing lease")
            .is_none());

        let mut fenced_transaction = store
            .get_transaction("redis-gid")
            .await
            .expect("read fenced transaction")
            .expect("fenced transaction exists");
        fenced_transaction.status = TransactionStatus::Prepared;
        store
            .update_transaction_fenced(fenced_transaction.clone(), &fence)
            .await
            .expect("valid fence updates transaction");
        let fenced_transaction = store
            .get_transaction("redis-gid")
            .await
            .expect("read updated fenced transaction")
            .expect("updated fenced transaction exists");
        assert_eq!(fenced_transaction.status, TransactionStatus::Prepared);
        let stale = RecoveryLeaseFence {
            epoch: fence.epoch.saturating_add(1),
            ..fence
        };
        assert!(store
            .update_transaction_fenced(fenced_transaction, &stale)
            .await
            .is_err());
        let barrier = BranchBarrier::new("redis-gid", "01", "try");
        assert_eq!(
            store
                .barrier(barrier.clone())
                .await
                .expect("create barrier"),
            BarrierDecision::Execute
        );
        let mut connection = store.connection().await.expect("Redis connection");
        let _: i64 = redis::cmd("DEL")
            .arg(
                store
                    .barrier_index_key("redis-gid")
                    .expect("barrier index key"),
            )
            .query_async(&mut connection)
            .await
            .expect("simulate pre-index barrier record");
        let snapshot = store
            .get_transaction("redis-gid")
            .await
            .expect("read retention transaction")
            .expect("retention transaction exists");
        assert!(store
            .delete_transaction_if_unchanged(&snapshot)
            .await
            .expect("delete unchanged transaction"));
        assert_eq!(
            store.barrier(barrier).await.expect("barrier was cleaned"),
            BarrierDecision::Execute
        );
    }

    #[tokio::test]
    #[ignore = "requires ROZE_TEST_REDIS_CLUSTER_URLS"]
    async fn redis_cluster_store_round_trip_against_real_service() {
        let cluster_urls = cluster_urls();
        let namespace = cluster_namespace();
        let store = RedisTransactionStore::open_topology("", &cluster_urls, &namespace)
            .expect("open Redis Cluster store");
        store
            .health_check()
            .await
            .expect("Redis Cluster health check");

        let transaction = Transaction::message("redis-cluster-gid", Vec::new());
        store
            .insert_transaction(transaction.clone())
            .await
            .expect("insert Cluster transaction");
        assert_eq!(
            store
                .get_transaction(&transaction.gid)
                .await
                .expect("read Cluster transaction"),
            Some(transaction.clone())
        );
        assert_eq!(
            store
                .barrier(BranchBarrier::new("redis-cluster-gid", "01", "try"))
                .await
                .expect("create Cluster barrier"),
            BarrierDecision::Execute
        );
        let snapshot = store
            .get_transaction(&transaction.gid)
            .await
            .expect("read Cluster transaction for cleanup")
            .expect("Cluster transaction exists for cleanup");
        assert!(store
            .delete_transaction_if_unchanged(&snapshot)
            .await
            .expect("clean Cluster transaction"));
    }

    #[tokio::test]
    #[ignore = "requires a disposable Redis Cluster and ROZE_TEST_REDIS_CLUSTER_FAULT_* variables"]
    async fn redis_cluster_handles_ask_and_moved_redirections() {
        let cluster_urls = cluster_urls();
        let namespace = cluster_namespace();
        let source_url = std::env::var("ROZE_TEST_REDIS_CLUSTER_FAULT_SOURCE_URL")
            .expect("ROZE_TEST_REDIS_CLUSTER_FAULT_SOURCE_URL is required");
        let target_url = std::env::var("ROZE_TEST_REDIS_CLUSTER_FAULT_TARGET_URL")
            .expect("ROZE_TEST_REDIS_CLUSTER_FAULT_TARGET_URL is required");
        let source_id = std::env::var("ROZE_TEST_REDIS_CLUSTER_FAULT_SOURCE_ID")
            .expect("ROZE_TEST_REDIS_CLUSTER_FAULT_SOURCE_ID is required");
        let target_id = std::env::var("ROZE_TEST_REDIS_CLUSTER_FAULT_TARGET_ID")
            .expect("ROZE_TEST_REDIS_CLUSTER_FAULT_TARGET_ID is required");
        let slot = std::env::var("ROZE_TEST_REDIS_CLUSTER_FAULT_SLOT")
            .expect("ROZE_TEST_REDIS_CLUSTER_FAULT_SLOT is required")
            .parse::<u16>()
            .expect("fault slot is a u16");
        let master_urls = std::env::var("ROZE_TEST_REDIS_CLUSTER_MASTER_URLS")
            .expect("ROZE_TEST_REDIS_CLUSTER_MASTER_URLS is required")
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        assert!(master_urls.len() >= 3, "fault test requires three masters");

        let store = RedisTransactionStore::open_topology("", &cluster_urls, &namespace)
            .expect("open Redis Cluster fault-test store");
        let mut routed = store
            .connection()
            .await
            .expect("establish routing table before slot changes");
        let ask_key = format!("roze-dtm:{{{namespace}}}:ask-probe");
        let moved_key = format!("roze-dtm:{{{namespace}}}:moved-probe");

        set_cluster_slot(&target_url, slot, "IMPORTING", Some(&source_id)).await;
        set_cluster_slot(&source_url, slot, "MIGRATING", Some(&target_id)).await;
        let response: String = redis::cmd("SET")
            .arg(&ask_key)
            .arg("ask-ok")
            .query_async(&mut routed)
            .await
            .expect("follow Redis ASK redirection");
        assert_eq!(response, "OK");
        let value: String = redis::cmd("GET")
            .arg(&ask_key)
            .query_async(&mut routed)
            .await
            .expect("read through Redis ASK redirection");
        assert_eq!(value, "ask-ok");
        let _: i64 = redis::cmd("DEL")
            .arg(&ask_key)
            .query_async(&mut routed)
            .await
            .expect("clean Redis ASK probe");
        set_cluster_slot(&source_url, slot, "STABLE", None).await;
        set_cluster_slot(&target_url, slot, "STABLE", None).await;

        assign_cluster_slot(&master_urls, &target_url, &source_url, slot, &target_id).await;
        let response: String = redis::cmd("SET")
            .arg(&moved_key)
            .arg("moved-ok")
            .query_async(&mut routed)
            .await
            .expect("follow Redis MOVED redirection");
        assert_eq!(response, "OK");
        let value: String = redis::cmd("GET")
            .arg(&moved_key)
            .query_async(&mut routed)
            .await
            .expect("read after Redis MOVED redirection");
        assert_eq!(value, "moved-ok");
        let _: i64 = redis::cmd("DEL")
            .arg(&moved_key)
            .query_async(&mut routed)
            .await
            .expect("clean Redis MOVED probe");
        assign_cluster_slot(&master_urls, &source_url, &target_url, slot, &source_id).await;
    }
}
