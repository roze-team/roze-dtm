//! XA resource-manager helpers for MySQL and PostgreSQL business databases.
//!
//! The coordinator owns the global decision; this module keeps each local
//! business mutation, idempotency barrier, branch registration, and prepare
//! operation within one explicitly acquired database connection.

use std::{collections::BTreeSet, future::Future, pin::Pin};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::{MySqlConnection, MySqlPool, MySqlRow},
    pool::PoolConnection,
    postgres::{PgConnection, PgPool, PgRow},
    MySql, Postgres, Row,
};

use crate::client::DtmHttpClient;

pub const MYSQL_XA_BARRIER_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS roze_xa_barriers (
    gid VARCHAR(128) NOT NULL,
    branch_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (gid, branch_id)
) ENGINE=InnoDB
"#;

pub const POSTGRES_XA_BARRIER_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS roze_xa_barriers (
    gid VARCHAR(128) NOT NULL,
    branch_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (gid, branch_id)
)
"#;

pub const MYSQL_XA_DECISION_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS roze_xa_decisions (
    decision_id VARCHAR(64) NOT NULL PRIMARY KEY,
    gid VARCHAR(128) NOT NULL,
    branch_id VARCHAR(128) NOT NULL,
    decision VARCHAR(16) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    requested_at_millis BIGINT NOT NULL,
    finished_at_millis BIGINT NULL,
    UNIQUE KEY uniq_roze_xa_decisions_resource (gid, branch_id)
) ENGINE=InnoDB
"#;

pub const POSTGRES_XA_DECISION_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS roze_xa_decisions (
    decision_id VARCHAR(64) PRIMARY KEY,
    gid VARCHAR(128) NOT NULL,
    branch_id VARCHAR(128) NOT NULL,
    decision VARCHAR(16) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    requested_at_millis BIGINT NOT NULL,
    finished_at_millis BIGINT NULL,
    UNIQUE (gid, branch_id)
)
"#;

pub type MySqlXaWork<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;
pub type PostgresXaWork<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XaPhase2 {
    Commit,
    Rollback,
}

impl XaPhase2 {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "commit" => Ok(Self::Commit),
            "rollback" | "abort" => Ok(Self::Rollback),
            _ => anyhow::bail!("XA phase must be commit or rollback"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaLocalOutcome<T> {
    Prepared(T),
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XaPhase2Outcome {
    Applied,
    AlreadyResolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XaHeuristicStatus {
    Requested,
    Applied,
    AlreadyResolved,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XaHeuristicRequest {
    pub decision_id: String,
    pub decision: XaPhase2,
    pub reason: String,
}

impl XaHeuristicRequest {
    pub fn new(
        decision_id: impl Into<String>,
        decision: XaPhase2,
        reason: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let request = Self {
            decision_id: decision_id.into(),
            decision,
            reason: reason.into(),
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.decision_id.is_empty()
                && self.decision_id.len() <= 64
                && self.decision_id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
                }),
            "XA heuristic decision id must contain 1 to 64 safe ASCII bytes"
        );
        anyhow::ensure!(
            !self.reason.trim().is_empty() && self.reason.len() <= 512,
            "XA heuristic reason must contain 1 to 512 bytes"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XaHeuristicRecord {
    pub decision_id: String,
    pub gid: String,
    pub branch_id: String,
    pub decision: XaPhase2,
    pub reason: String,
    pub status: XaHeuristicStatus,
    pub requested_at_millis: u64,
    pub finished_at_millis: Option<u64>,
}

impl XaHeuristicRecord {
    pub fn resource_id(&self) -> String {
        format!("{}-{}", self.gid, self.branch_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XaHeuristicResolution {
    pub outcome: XaPhase2Outcome,
    pub record: XaHeuristicRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XaReconciliationReport {
    pub prepared_resource_ids: Vec<String>,
    pub pending_decisions: Vec<XaHeuristicRecord>,
    pub prepared_without_decision: Vec<String>,
    pub decisions_without_prepared: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XaBranchDescriptor {
    pub gid: String,
    pub branch_id: String,
    pub phase2_url: String,
}

impl XaBranchDescriptor {
    pub fn new(
        gid: impl Into<String>,
        branch_id: impl Into<String>,
        phase2_url: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let branch = Self {
            gid: gid.into(),
            branch_id: branch_id.into(),
            phase2_url: phase2_url.into(),
        };
        branch.validate()?;
        Ok(branch)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_xid_part("gid", &self.gid)?;
        validate_xid_part("branch id", &self.branch_id)?;
        anyhow::ensure!(
            self.gid
                .len()
                .saturating_add(self.branch_id.len())
                .saturating_add(1)
                <= 128,
            "XA resource id exceeds 128 bytes"
        );
        let url = reqwest::Url::parse(&self.phase2_url).context("invalid XA phase-2 URL")?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "XA phase-2 URL must be an HTTP(S) origin URL without credentials or fragment"
        );
        Ok(())
    }

    pub fn resource_id(&self) -> String {
        format!("{}-{}", self.gid, self.branch_id)
    }
}

fn validate_xid_part(name: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            }),
        "XA {name} must contain 1 to 128 safe ASCII bytes"
    );
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MySqlXaResourceManager {
    pool: MySqlPool,
}

impl MySqlXaResourceManager {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn install_barrier_schema(&self) -> anyhow::Result<()> {
        sqlx::query(MYSQL_XA_BARRIER_DDL)
            .execute(&self.pool)
            .await?;
        sqlx::query(MYSQL_XA_DECISION_DDL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn prepare_branch<T, F>(
        &self,
        client: &DtmHttpClient,
        branch: &XaBranchDescriptor,
        work: F,
    ) -> anyhow::Result<XaLocalOutcome<T>>
    where
        T: Send,
        F: for<'connection> FnOnce(&'connection mut MySqlConnection) -> MySqlXaWork<'connection, T>
            + Send,
    {
        branch.validate()?;
        let xid = branch.resource_id();
        anyhow::ensure!(xid.len() <= 64, "MySQL XA resource id exceeds 64 bytes");
        let mut connection = self.pool.acquire().await?;
        execute_mysql(&mut connection, audited_mysql_xa_sql("start", &xid)).await?;

        let inserted =
            match sqlx::query("INSERT IGNORE INTO roze_xa_barriers (gid, branch_id) VALUES (?, ?)")
                .bind(&branch.gid)
                .bind(&branch.branch_id)
                .execute(&mut *connection)
                .await
            {
                Ok(result) => result.rows_affected(),
                Err(error) => {
                    cleanup_mysql(connection, &xid)
                        .await
                        .context("failed to clean up XA branch after barrier error")?;
                    return Err(error).context("failed to insert XA branch barrier");
                }
            };
        if inserted == 0 {
            cleanup_mysql(connection, &xid).await?;
            return Ok(XaLocalOutcome::Duplicate);
        }

        let value = match work(&mut connection).await {
            Ok(value) => value,
            Err(error) => {
                cleanup_mysql(connection, &xid)
                    .await
                    .context("failed to roll back XA branch after business error")?;
                return Err(error.context("XA business operation failed"));
            }
        };
        if let Err(error) = client
            .register_xa_branch(&branch.gid, &branch.branch_id, &branch.phase2_url)
            .await
        {
            cleanup_mysql(connection, &xid)
                .await
                .context("failed to roll back XA branch after registration error")?;
            return Err(error.context("failed to register XA branch"));
        }
        if let Err(error) = execute_mysql(&mut connection, audited_mysql_xa_sql("end", &xid)).await
        {
            cleanup_mysql(connection, &xid)
                .await
                .context("failed to roll back XA branch after XA END error")?;
            return Err(error.context("failed to end XA branch"));
        }
        if let Err(error) =
            execute_mysql(&mut connection, audited_mysql_xa_sql("prepare", &xid)).await
        {
            cleanup_mysql(connection, &xid)
                .await
                .context("failed to roll back XA branch after prepare error")?;
            return Err(error.context("failed to prepare XA branch"));
        }
        Ok(XaLocalOutcome::Prepared(value))
    }

    pub async fn resolve(
        &self,
        branch: &XaBranchDescriptor,
        phase: XaPhase2,
    ) -> anyhow::Result<XaPhase2Outcome> {
        branch.validate()?;
        anyhow::ensure!(
            branch.resource_id().len() <= 64,
            "MySQL XA resource id exceeds 64 bytes"
        );
        let command = match phase {
            XaPhase2::Commit => "commit",
            XaPhase2::Rollback => "rollback",
        };
        let result = sqlx::raw_sql(audited_mysql_xa_sql(command, &branch.resource_id()))
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(XaPhase2Outcome::Applied),
            Err(error) if mysql_already_resolved(&error) => Ok(XaPhase2Outcome::AlreadyResolved),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn recover_prepared(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::raw_sql("XA RECOVER").fetch_all(&self.pool).await?;
        let mut resource_ids = Vec::with_capacity(rows.len());
        for row in rows {
            let data: Vec<u8> = row.try_get(3)?;
            if let Ok(resource_id) = String::from_utf8(data) {
                resource_ids.push(resource_id);
            }
        }
        resource_ids.sort();
        Ok(resource_ids)
    }

    pub async fn resolve_heuristically(
        &self,
        branch: &XaBranchDescriptor,
        request: &XaHeuristicRequest,
    ) -> anyhow::Result<XaHeuristicResolution> {
        branch.validate()?;
        request.validate()?;
        let record = self.record_heuristic_intent(branch, request).await?;
        if let Some(outcome) = completed_heuristic_outcome(record.status) {
            return Ok(XaHeuristicResolution { outcome, record });
        }
        let outcome = match self.resolve(branch, request.decision).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.finish_heuristic_decision(&request.decision_id, XaHeuristicStatus::Failed)
                    .await
                    .context("failed to persist failed XA heuristic outcome")?;
                return Err(error.context("XA heuristic phase-2 failed"));
            }
        };
        let status = heuristic_status_for_outcome(outcome);
        let record = self
            .finish_heuristic_decision(&request.decision_id, status)
            .await?;
        Ok(XaHeuristicResolution { outcome, record })
    }

    pub async fn list_heuristic_decisions(&self) -> anyhow::Result<Vec<XaHeuristicRecord>> {
        let rows = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions ORDER BY requested_at_millis, decision_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(mysql_heuristic_record).collect()
    }

    pub async fn reconcile(&self) -> anyhow::Result<XaReconciliationReport> {
        build_reconciliation_report(
            self.recover_prepared().await?,
            self.list_heuristic_decisions().await?,
        )
    }

    async fn record_heuristic_intent(
        &self,
        branch: &XaBranchDescriptor,
        request: &XaHeuristicRequest,
    ) -> anyhow::Result<XaHeuristicRecord> {
        let requested_at_millis = crate::current_millis();
        sqlx::query(
            "INSERT IGNORE INTO roze_xa_decisions \
             (decision_id, gid, branch_id, decision, reason, status, requested_at_millis) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&request.decision_id)
        .bind(&branch.gid)
        .bind(&branch.branch_id)
        .bind(xa_phase_name(request.decision))
        .bind(&request.reason)
        .bind(heuristic_status_name(XaHeuristicStatus::Requested))
        .bind(requested_at_millis as i64)
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions \
             WHERE decision_id = ? OR (gid = ? AND branch_id = ?)",
        )
        .bind(&request.decision_id)
        .bind(&branch.gid)
        .bind(&branch.branch_id)
        .fetch_one(&self.pool)
        .await?;
        let record = mysql_heuristic_record(row)?;
        ensure_heuristic_request_matches(&record, branch, request)?;
        Ok(record)
    }

    async fn finish_heuristic_decision(
        &self,
        decision_id: &str,
        status: XaHeuristicStatus,
    ) -> anyhow::Result<XaHeuristicRecord> {
        let finished_at_millis = crate::current_millis();
        let changed = sqlx::query(
            "UPDATE roze_xa_decisions SET status = ?, finished_at_millis = ? \
             WHERE decision_id = ?",
        )
        .bind(heuristic_status_name(status))
        .bind(finished_at_millis as i64)
        .bind(decision_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        anyhow::ensure!(changed == 1, "XA heuristic decision record not found");
        let row = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions WHERE decision_id = ?",
        )
        .bind(decision_id)
        .fetch_one(&self.pool)
        .await?;
        mysql_heuristic_record(row)
    }
}

#[derive(Debug, Clone)]
pub struct PostgresXaResourceManager {
    pool: PgPool,
}

impl PostgresXaResourceManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn install_barrier_schema(&self) -> anyhow::Result<()> {
        sqlx::query(POSTGRES_XA_BARRIER_DDL)
            .execute(&self.pool)
            .await?;
        sqlx::query(POSTGRES_XA_DECISION_DDL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn prepare_branch<T, F>(
        &self,
        client: &DtmHttpClient,
        branch: &XaBranchDescriptor,
        work: F,
    ) -> anyhow::Result<XaLocalOutcome<T>>
    where
        T: Send,
        F: for<'connection> FnOnce(&'connection mut PgConnection) -> PostgresXaWork<'connection, T>
            + Send,
    {
        branch.validate()?;
        let xid = branch.resource_id();
        let mut connection = self.pool.acquire().await?;
        execute_postgres(&mut connection, "BEGIN").await?;
        let inserted = match sqlx::query(
            "INSERT INTO roze_xa_barriers (gid, branch_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(&branch.gid)
        .bind(&branch.branch_id)
        .execute(&mut *connection)
        .await
        {
            Ok(result) => result.rows_affected(),
            Err(error) => {
                cleanup_postgres(connection)
                    .await
                    .context("failed to clean up XA branch after barrier error")?;
                return Err(error).context("failed to insert XA branch barrier");
            }
        };
        if inserted == 0 {
            cleanup_postgres(connection).await?;
            return Ok(XaLocalOutcome::Duplicate);
        }

        let value = match work(&mut connection).await {
            Ok(value) => value,
            Err(error) => {
                cleanup_postgres(connection)
                    .await
                    .context("failed to roll back XA branch after business error")?;
                return Err(error.context("XA business operation failed"));
            }
        };
        if let Err(error) = client
            .register_xa_branch(&branch.gid, &branch.branch_id, &branch.phase2_url)
            .await
        {
            cleanup_postgres(connection)
                .await
                .context("failed to roll back XA branch after registration error")?;
            return Err(error.context("failed to register XA branch"));
        }
        if let Err(error) =
            execute_postgres(&mut connection, audited_postgres_xa_sql("prepare", &xid)).await
        {
            cleanup_postgres(connection)
                .await
                .context("failed to roll back XA branch after prepare error")?;
            return Err(error.context("failed to prepare XA branch"));
        }
        Ok(XaLocalOutcome::Prepared(value))
    }

    pub async fn resolve(
        &self,
        branch: &XaBranchDescriptor,
        phase: XaPhase2,
    ) -> anyhow::Result<XaPhase2Outcome> {
        branch.validate()?;
        let command = match phase {
            XaPhase2::Commit => "commit",
            XaPhase2::Rollback => "rollback",
        };
        let result = sqlx::raw_sql(audited_postgres_xa_sql(command, &branch.resource_id()))
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(XaPhase2Outcome::Applied),
            Err(error) if postgres_already_resolved(&error) => Ok(XaPhase2Outcome::AlreadyResolved),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn recover_prepared(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT gid FROM pg_prepared_xacts WHERE database = current_database() ORDER BY gid",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| row.try_get::<String, _>(0).map_err(Into::into))
            .collect()
    }

    pub async fn resolve_heuristically(
        &self,
        branch: &XaBranchDescriptor,
        request: &XaHeuristicRequest,
    ) -> anyhow::Result<XaHeuristicResolution> {
        branch.validate()?;
        request.validate()?;
        let record = self.record_heuristic_intent(branch, request).await?;
        if let Some(outcome) = completed_heuristic_outcome(record.status) {
            return Ok(XaHeuristicResolution { outcome, record });
        }
        let outcome = match self.resolve(branch, request.decision).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.finish_heuristic_decision(&request.decision_id, XaHeuristicStatus::Failed)
                    .await
                    .context("failed to persist failed XA heuristic outcome")?;
                return Err(error.context("XA heuristic phase-2 failed"));
            }
        };
        let status = heuristic_status_for_outcome(outcome);
        let record = self
            .finish_heuristic_decision(&request.decision_id, status)
            .await?;
        Ok(XaHeuristicResolution { outcome, record })
    }

    pub async fn list_heuristic_decisions(&self) -> anyhow::Result<Vec<XaHeuristicRecord>> {
        let rows = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions ORDER BY requested_at_millis, decision_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(postgres_heuristic_record).collect()
    }

    pub async fn reconcile(&self) -> anyhow::Result<XaReconciliationReport> {
        build_reconciliation_report(
            self.recover_prepared().await?,
            self.list_heuristic_decisions().await?,
        )
    }

    async fn record_heuristic_intent(
        &self,
        branch: &XaBranchDescriptor,
        request: &XaHeuristicRequest,
    ) -> anyhow::Result<XaHeuristicRecord> {
        let requested_at_millis = crate::current_millis();
        sqlx::query(
            "INSERT INTO roze_xa_decisions \
             (decision_id, gid, branch_id, decision, reason, status, requested_at_millis) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (decision_id) DO NOTHING",
        )
        .bind(&request.decision_id)
        .bind(&branch.gid)
        .bind(&branch.branch_id)
        .bind(xa_phase_name(request.decision))
        .bind(&request.reason)
        .bind(heuristic_status_name(XaHeuristicStatus::Requested))
        .bind(requested_at_millis as i64)
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions \
             WHERE decision_id = $1 OR (gid = $2 AND branch_id = $3)",
        )
        .bind(&request.decision_id)
        .bind(&branch.gid)
        .bind(&branch.branch_id)
        .fetch_one(&self.pool)
        .await?;
        let record = postgres_heuristic_record(row)?;
        ensure_heuristic_request_matches(&record, branch, request)?;
        Ok(record)
    }

    async fn finish_heuristic_decision(
        &self,
        decision_id: &str,
        status: XaHeuristicStatus,
    ) -> anyhow::Result<XaHeuristicRecord> {
        let finished_at_millis = crate::current_millis();
        let changed = sqlx::query(
            "UPDATE roze_xa_decisions SET status = $1, finished_at_millis = $2 \
             WHERE decision_id = $3",
        )
        .bind(heuristic_status_name(status))
        .bind(finished_at_millis as i64)
        .bind(decision_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        anyhow::ensure!(changed == 1, "XA heuristic decision record not found");
        let row = sqlx::query(
            "SELECT decision_id, gid, branch_id, decision, reason, status, \
             requested_at_millis, finished_at_millis \
             FROM roze_xa_decisions WHERE decision_id = $1",
        )
        .bind(decision_id)
        .fetch_one(&self.pool)
        .await?;
        postgres_heuristic_record(row)
    }
}

fn xa_phase_name(phase: XaPhase2) -> &'static str {
    match phase {
        XaPhase2::Commit => "commit",
        XaPhase2::Rollback => "rollback",
    }
}

fn parse_xa_phase(value: &str) -> anyhow::Result<XaPhase2> {
    XaPhase2::parse(value)
}

fn heuristic_status_name(status: XaHeuristicStatus) -> &'static str {
    match status {
        XaHeuristicStatus::Requested => "requested",
        XaHeuristicStatus::Applied => "applied",
        XaHeuristicStatus::AlreadyResolved => "already_resolved",
        XaHeuristicStatus::Failed => "failed",
    }
}

fn parse_heuristic_status(value: &str) -> anyhow::Result<XaHeuristicStatus> {
    match value {
        "requested" => Ok(XaHeuristicStatus::Requested),
        "applied" => Ok(XaHeuristicStatus::Applied),
        "already_resolved" => Ok(XaHeuristicStatus::AlreadyResolved),
        "failed" => Ok(XaHeuristicStatus::Failed),
        _ => anyhow::bail!("invalid XA heuristic status"),
    }
}

fn heuristic_status_for_outcome(outcome: XaPhase2Outcome) -> XaHeuristicStatus {
    match outcome {
        XaPhase2Outcome::Applied => XaHeuristicStatus::Applied,
        XaPhase2Outcome::AlreadyResolved => XaHeuristicStatus::AlreadyResolved,
    }
}

fn completed_heuristic_outcome(status: XaHeuristicStatus) -> Option<XaPhase2Outcome> {
    match status {
        XaHeuristicStatus::Applied => Some(XaPhase2Outcome::Applied),
        XaHeuristicStatus::AlreadyResolved => Some(XaPhase2Outcome::AlreadyResolved),
        XaHeuristicStatus::Requested | XaHeuristicStatus::Failed => None,
    }
}

fn ensure_heuristic_request_matches(
    record: &XaHeuristicRecord,
    branch: &XaBranchDescriptor,
    request: &XaHeuristicRequest,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.decision_id == request.decision_id
            && record.gid == branch.gid
            && record.branch_id == branch.branch_id
            && record.decision == request.decision
            && record.reason == request.reason,
        "XA heuristic request conflicts with an existing decision or resource"
    );
    Ok(())
}

fn mysql_heuristic_record(row: MySqlRow) -> anyhow::Result<XaHeuristicRecord> {
    heuristic_record_from_values(RawXaHeuristicRecord {
        decision_id: row.try_get("decision_id")?,
        gid: row.try_get("gid")?,
        branch_id: row.try_get("branch_id")?,
        decision: row.try_get("decision")?,
        reason: row.try_get("reason")?,
        status: row.try_get("status")?,
        requested_at_millis: row.try_get::<i64, _>("requested_at_millis")?,
        finished_at_millis: row.try_get::<Option<i64>, _>("finished_at_millis")?,
    })
}

fn postgres_heuristic_record(row: PgRow) -> anyhow::Result<XaHeuristicRecord> {
    heuristic_record_from_values(RawXaHeuristicRecord {
        decision_id: row.try_get("decision_id")?,
        gid: row.try_get("gid")?,
        branch_id: row.try_get("branch_id")?,
        decision: row.try_get("decision")?,
        reason: row.try_get("reason")?,
        status: row.try_get("status")?,
        requested_at_millis: row.try_get::<i64, _>("requested_at_millis")?,
        finished_at_millis: row.try_get::<Option<i64>, _>("finished_at_millis")?,
    })
}

struct RawXaHeuristicRecord {
    decision_id: String,
    gid: String,
    branch_id: String,
    decision: String,
    reason: String,
    status: String,
    requested_at_millis: i64,
    finished_at_millis: Option<i64>,
}

fn heuristic_record_from_values(raw: RawXaHeuristicRecord) -> anyhow::Result<XaHeuristicRecord> {
    Ok(XaHeuristicRecord {
        decision_id: raw.decision_id,
        gid: raw.gid,
        branch_id: raw.branch_id,
        decision: parse_xa_phase(&raw.decision)?,
        reason: raw.reason,
        status: parse_heuristic_status(&raw.status)?,
        requested_at_millis: u64::try_from(raw.requested_at_millis)
            .context("invalid XA heuristic request timestamp")?,
        finished_at_millis: raw
            .finished_at_millis
            .map(u64::try_from)
            .transpose()
            .context("invalid XA heuristic completion timestamp")?,
    })
}

fn build_reconciliation_report(
    mut prepared_resource_ids: Vec<String>,
    decisions: Vec<XaHeuristicRecord>,
) -> anyhow::Result<XaReconciliationReport> {
    prepared_resource_ids.sort();
    prepared_resource_ids.dedup();
    let prepared = prepared_resource_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let pending_decisions = decisions
        .into_iter()
        .filter(|record| {
            matches!(
                record.status,
                XaHeuristicStatus::Requested | XaHeuristicStatus::Failed
            )
        })
        .collect::<Vec<_>>();
    let decided_resources = pending_decisions
        .iter()
        .map(XaHeuristicRecord::resource_id)
        .collect::<BTreeSet<_>>();
    let prepared_without_decision = prepared.difference(&decided_resources).cloned().collect();
    let decisions_without_prepared = decided_resources.difference(&prepared).cloned().collect();
    Ok(XaReconciliationReport {
        prepared_resource_ids,
        pending_decisions,
        prepared_without_decision,
        decisions_without_prepared,
    })
}

fn mysql_xa_sql(command: &str, xid: &str) -> String {
    match command {
        "start" | "end" | "prepare" | "commit" | "rollback" => {
            format!("XA {command} '{xid}'")
        }
        _ => unreachable!("validated MySQL XA command"),
    }
}

fn postgres_xa_sql(command: &str, xid: &str) -> String {
    match command {
        "prepare" => format!("PREPARE TRANSACTION '{xid}'"),
        "commit" => format!("COMMIT PREPARED '{xid}'"),
        "rollback" => format!("ROLLBACK PREPARED '{xid}'"),
        _ => unreachable!("validated PostgreSQL XA command"),
    }
}

fn audited_mysql_xa_sql(command: &str, xid: &str) -> sqlx::AssertSqlSafe<String> {
    debug_assert!(validate_xid_part("resource id", xid).is_ok());
    sqlx::AssertSqlSafe(mysql_xa_sql(command, xid))
}

fn audited_postgres_xa_sql(command: &str, xid: &str) -> sqlx::AssertSqlSafe<String> {
    debug_assert!(validate_xid_part("resource id", xid).is_ok());
    sqlx::AssertSqlSafe(postgres_xa_sql(command, xid))
}

async fn execute_mysql(
    connection: &mut MySqlConnection,
    statement: impl sqlx::SqlSafeStr,
) -> anyhow::Result<()> {
    sqlx::raw_sql(statement).execute(connection).await?;
    Ok(())
}

async fn rollback_mysql_active(connection: &mut MySqlConnection, xid: &str) -> anyhow::Result<()> {
    let _ = execute_mysql(connection, audited_mysql_xa_sql("end", xid)).await;
    execute_mysql(connection, audited_mysql_xa_sql("rollback", xid)).await
}

async fn cleanup_mysql(mut connection: PoolConnection<MySql>, xid: &str) -> anyhow::Result<()> {
    if let Err(error) = rollback_mysql_active(&mut connection, xid).await {
        let _ = connection.close().await;
        return Err(error);
    }
    Ok(())
}

async fn execute_postgres(
    connection: &mut PgConnection,
    statement: impl sqlx::SqlSafeStr,
) -> anyhow::Result<()> {
    sqlx::raw_sql(statement).execute(connection).await?;
    Ok(())
}

async fn cleanup_postgres(mut connection: PoolConnection<Postgres>) -> anyhow::Result<()> {
    if let Err(error) = execute_postgres(&mut connection, "ROLLBACK").await {
        let _ = connection.close().await;
        return Err(error);
    }
    Ok(())
}

fn mysql_already_resolved(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(error) = error else {
        return false;
    };
    matches!(error.code().as_deref(), Some("1397" | "XAE04"))
        || error.message().contains("XAER_NOTA")
}

fn postgres_already_resolved(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(error) = error else {
        return false;
    };
    error.code().as_deref() == Some("42704")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_builds_injection_safe_resource_id() {
        let branch =
            XaBranchDescriptor::new("order-2026", "01", "https://business.example.com/xa/phase2")
                .expect("valid XA branch");
        assert_eq!(branch.resource_id(), "order-2026-01");
        assert!(XaBranchDescriptor::new(
            "order'; DROP TABLE accounts; --",
            "01",
            "https://business.example.com/xa/phase2",
        )
        .is_err());
        assert!(XaBranchDescriptor::new(
            "order",
            "01",
            "https://user:secret@business.example.com/xa/phase2",
        )
        .is_err());
    }

    #[test]
    fn dialect_sql_matches_upstream_xa_contract() {
        assert_eq!(mysql_xa_sql("start", "gid-01"), "XA start 'gid-01'");
        assert_eq!(mysql_xa_sql("prepare", "gid-01"), "XA prepare 'gid-01'");
        assert_eq!(
            postgres_xa_sql("prepare", "gid-01"),
            "PREPARE TRANSACTION 'gid-01'"
        );
        assert_eq!(
            postgres_xa_sql("rollback", "gid-01"),
            "ROLLBACK PREPARED 'gid-01'"
        );
    }

    #[test]
    fn phase_parser_accepts_upstream_abort_alias() {
        assert_eq!(XaPhase2::parse("commit").unwrap(), XaPhase2::Commit);
        assert_eq!(XaPhase2::parse("rollback").unwrap(), XaPhase2::Rollback);
        assert_eq!(XaPhase2::parse("abort").unwrap(), XaPhase2::Rollback);
        assert!(XaPhase2::parse("prepare").is_err());
    }

    #[test]
    fn heuristic_requests_are_bounded_and_reconciliation_is_deterministic() {
        let request = XaHeuristicRequest::new(
            "incident-2026-001",
            XaPhase2::Rollback,
            "approved after prepared-transaction reconciliation",
        )
        .expect("valid heuristic request");
        assert_eq!(request.decision, XaPhase2::Rollback);
        assert!(XaHeuristicRequest::new("bad id", XaPhase2::Commit, "reason").is_err());
        assert!(XaHeuristicRequest::new("decision", XaPhase2::Commit, " ").is_err());

        let report = build_reconciliation_report(
            vec!["gid-01".to_owned(), "gid-02".to_owned()],
            vec![XaHeuristicRecord {
                decision_id: "incident-2026-001".to_owned(),
                gid: "gid".to_owned(),
                branch_id: "01".to_owned(),
                decision: XaPhase2::Rollback,
                reason: "approved".to_owned(),
                status: XaHeuristicStatus::Requested,
                requested_at_millis: 1,
                finished_at_millis: None,
            }],
        )
        .expect("reconciliation report");
        assert_eq!(report.pending_decisions.len(), 1);
        assert_eq!(report.prepared_without_decision, vec!["gid-02".to_owned()]);
        assert!(report.decisions_without_prepared.is_empty());
    }

    #[test]
    fn heuristic_schema_keeps_decisions_separate_from_business_barriers() {
        for ddl in [MYSQL_XA_DECISION_DDL, POSTGRES_XA_DECISION_DDL] {
            assert!(ddl.contains("roze_xa_decisions"));
            assert!(ddl.contains("decision_id"));
            assert!(ddl.contains("reason"));
            assert!(ddl.contains("finished_at_millis"));
        }
    }
}
