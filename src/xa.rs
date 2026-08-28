//! XA resource-manager helpers for MySQL and PostgreSQL business databases.
//!
//! The coordinator owns the global decision; this module keeps each local
//! business mutation, idempotency barrier, branch registration, and prepare
//! operation within one explicitly acquired database connection.

use std::{future::Future, pin::Pin};

use anyhow::Context as _;
use sqlx::{
    mysql::{MySqlConnection, MySqlPool},
    pool::PoolConnection,
    postgres::{PgConnection, PgPool},
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

pub type MySqlXaWork<'a, T> =
    Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;
pub type PostgresXaWork<'a, T> =
    Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaPhase2Outcome {
    Applied,
    AlreadyResolved,
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
            self.gid.len().saturating_add(self.branch_id.len()).saturating_add(1) <= 128,
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
        F: for<'connection> FnOnce(
                &'connection mut MySqlConnection,
            ) -> MySqlXaWork<'connection, T>
            + Send,
    {
        branch.validate()?;
        let xid = branch.resource_id();
        anyhow::ensure!(xid.len() <= 64, "MySQL XA resource id exceeds 64 bytes");
        let mut connection = self.pool.acquire().await?;
        execute_mysql(&mut connection, &mysql_xa_sql("start", &xid)).await?;

        let inserted = match sqlx::query(
            "INSERT IGNORE INTO roze_xa_barriers (gid, branch_id) VALUES (?, ?)",
        )
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
        if let Err(error) = execute_mysql(&mut connection, &mysql_xa_sql("end", &xid)).await {
            cleanup_mysql(connection, &xid)
                .await
                .context("failed to roll back XA branch after XA END error")?;
            return Err(error.context("failed to end XA branch"));
        }
        if let Err(error) = execute_mysql(&mut connection, &mysql_xa_sql("prepare", &xid)).await {
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
        let result = sqlx::query(&mysql_xa_sql(command, &branch.resource_id()))
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(XaPhase2Outcome::Applied),
            Err(error) if mysql_already_resolved(&error) => Ok(XaPhase2Outcome::AlreadyResolved),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn recover_prepared(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query("XA RECOVER").fetch_all(&self.pool).await?;
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
        F: for<'connection> FnOnce(
                &'connection mut PgConnection,
            ) -> PostgresXaWork<'connection, T>
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
        if let Err(error) = execute_postgres(
            &mut connection,
            &postgres_xa_sql("prepare", &xid),
        )
        .await
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
        let result = sqlx::query(&postgres_xa_sql(command, &branch.resource_id()))
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(XaPhase2Outcome::Applied),
            Err(error) if postgres_already_resolved(&error) => {
                Ok(XaPhase2Outcome::AlreadyResolved)
            }
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
}

fn mysql_xa_sql(command: &str, xid: &str) -> String {
    format!("XA {command} '{xid}'")
}

fn postgres_xa_sql(command: &str, xid: &str) -> String {
    match command {
        "prepare" => format!("PREPARE TRANSACTION '{xid}'"),
        "commit" => format!("COMMIT PREPARED '{xid}'"),
        "rollback" => format!("ROLLBACK PREPARED '{xid}'"),
        _ => unreachable!("validated PostgreSQL XA command"),
    }
}

async fn execute_mysql(connection: &mut MySqlConnection, statement: &str) -> anyhow::Result<()> {
    sqlx::query(statement).execute(connection).await?;
    Ok(())
}

async fn rollback_mysql_active(
    connection: &mut MySqlConnection,
    xid: &str,
) -> anyhow::Result<()> {
    let _ = execute_mysql(connection, &mysql_xa_sql("end", xid)).await;
    execute_mysql(connection, &mysql_xa_sql("rollback", xid)).await
}

async fn cleanup_mysql(
    mut connection: PoolConnection<MySql>,
    xid: &str,
) -> anyhow::Result<()> {
    if let Err(error) = rollback_mysql_active(&mut connection, xid).await {
        let _ = connection.close().await;
        return Err(error);
    }
    Ok(())
}

async fn execute_postgres(
    connection: &mut PgConnection,
    statement: &str,
) -> anyhow::Result<()> {
    sqlx::query(statement).execute(connection).await?;
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
        let branch = XaBranchDescriptor::new(
            "order-2026",
            "01",
            "https://business.example.com/xa/phase2",
        )
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
}
