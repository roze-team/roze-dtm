use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use roze_dtm::{
    client::DtmHttpClient,
    xa::{
        MySqlXaResourceManager, PostgresXaResourceManager, XaBranchDescriptor, XaHeuristicRequest,
        XaHeuristicStatus, XaLocalOutcome, XaPhase2, XaPhase2Outcome,
    },
};
use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Row};

fn unique_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn registration_server() -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind registration server");
    let address = listener.local_addr().expect("registration server address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept registration request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set registration read timeout");
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stream.read(&mut buffer).expect("read registration request");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if request_is_complete(&bytes) {
                break;
            }
        }
        let body = r#"{"dtm_result":"SUCCESS"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write registration response");
        String::from_utf8(bytes).expect("registration request is UTF-8")
    });
    (format!("http://{address}"), handle)
}

fn request_is_complete(bytes: &[u8]) -> bool {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    bytes.len() >= header_end + 4 + content_length
}

#[tokio::test]
async fn mysql_xa_resource_manager_round_trip() {
    let Ok(database_url) = std::env::var("ROZE_DTM_TEST_MYSQL_URL") else {
        eprintln!("ROZE_DTM_TEST_MYSQL_URL is unset; skipping MySQL XA integration test");
        return;
    };
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect MySQL XA database");
    let manager = MySqlXaResourceManager::new(pool.clone());
    manager
        .install_barrier_schema()
        .await
        .expect("install MySQL XA schemas");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS roze_xa_integration_accounts (id VARCHAR(64) PRIMARY KEY, amount BIGINT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create MySQL XA business table");

    let commit_id = unique_id("mysql-commit");
    let commit_branch =
        XaBranchDescriptor::new(&commit_id, "account", "http://127.0.0.1:18091/xa/phase2")
            .expect("MySQL commit branch");
    let (registration_url, registration) = registration_server();
    let client = DtmHttpClient::new(registration_url).expect("MySQL registration client");
    let inserted_id = commit_id.clone();
    let prepared = manager
        .prepare_branch(&client, &commit_branch, move |connection| {
            Box::pin(async move {
                sqlx::query("INSERT INTO roze_xa_integration_accounts (id, amount) VALUES (?, ?)")
                    .bind(&inserted_id)
                    .bind(42_i64)
                    .execute(&mut *connection)
                    .await?;
                Ok(42_i64)
            })
        })
        .await
        .expect("prepare MySQL XA branch");
    assert!(matches!(prepared, XaLocalOutcome::Prepared(42)));
    let registration = registration.join().expect("join MySQL registration server");
    assert!(registration.contains("/api/dtmsvr/registerXaBranch"));
    assert!(registration.contains(&commit_id));
    assert!(manager
        .recover_prepared()
        .await
        .expect("recover MySQL XA branches")
        .contains(&commit_branch.resource_id()));

    let decision = XaHeuristicRequest::new(
        unique_id("mysql-decision"),
        XaPhase2::Commit,
        "integration commit",
    )
    .expect("MySQL heuristic request");
    let resolved = manager
        .resolve_heuristically(&commit_branch, &decision)
        .await
        .expect("commit MySQL XA branch");
    assert_eq!(resolved.outcome, XaPhase2Outcome::Applied);
    assert_eq!(resolved.record.status, XaHeuristicStatus::Applied);
    assert_eq!(
        manager
            .resolve_heuristically(&commit_branch, &decision)
            .await
            .expect("replay MySQL heuristic request")
            .record,
        resolved.record
    );
    let amount: i64 = sqlx::query("SELECT amount FROM roze_xa_integration_accounts WHERE id = ?")
        .bind(&commit_id)
        .fetch_one(&pool)
        .await
        .expect("read committed MySQL XA row")
        .try_get(0)
        .expect("decode committed MySQL amount");
    assert_eq!(amount, 42);

    let duplicate = manager
        .prepare_branch(&client, &commit_branch, |_| Box::pin(async { Ok(()) }))
        .await
        .expect("repeat MySQL XA branch");
    assert!(matches!(duplicate, XaLocalOutcome::Duplicate));
    assert!(manager
        .reconcile()
        .await
        .expect("reconcile MySQL XA state")
        .pending_decisions
        .is_empty());

    let rollback_id = unique_id("mysql-rollback");
    let rollback_branch =
        XaBranchDescriptor::new(&rollback_id, "account", "http://127.0.0.1:18091/xa/phase2")
            .expect("MySQL rollback branch");
    let (registration_url, registration) = registration_server();
    let client = DtmHttpClient::new(registration_url).expect("MySQL rollback client");
    let inserted_id = rollback_id.clone();
    manager
        .prepare_branch(&client, &rollback_branch, move |connection| {
            Box::pin(async move {
                sqlx::query("INSERT INTO roze_xa_integration_accounts (id, amount) VALUES (?, ?)")
                    .bind(&inserted_id)
                    .bind(7_i64)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .await
        .expect("prepare MySQL rollback branch");
    registration
        .join()
        .expect("join MySQL rollback registration");
    assert_eq!(
        manager
            .resolve(&rollback_branch, XaPhase2::Rollback)
            .await
            .expect("roll back MySQL XA branch"),
        XaPhase2Outcome::Applied
    );
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM roze_xa_integration_accounts WHERE id = ?")
        .bind(&rollback_id)
        .fetch_one(&pool)
        .await
        .expect("count rolled back MySQL row")
        .try_get(0)
        .expect("decode MySQL rollback count");
    assert_eq!(count, 0);
    sqlx::query("DELETE FROM roze_xa_integration_accounts WHERE id = ?")
        .bind(&commit_id)
        .execute(&pool)
        .await
        .expect("clean MySQL XA row");
}

#[tokio::test]
async fn postgres_xa_resource_manager_round_trip() {
    let Ok(database_url) = std::env::var("ROZE_DTM_TEST_POSTGRES_URL") else {
        eprintln!("ROZE_DTM_TEST_POSTGRES_URL is unset; skipping PostgreSQL XA integration test");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect PostgreSQL XA database");
    let manager = PostgresXaResourceManager::new(pool.clone());
    manager
        .install_barrier_schema()
        .await
        .expect("install PostgreSQL XA schemas");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS roze_xa_integration_accounts (id VARCHAR(64) PRIMARY KEY, amount BIGINT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create PostgreSQL XA business table");

    let commit_id = unique_id("pg-commit");
    let commit_branch =
        XaBranchDescriptor::new(&commit_id, "account", "http://127.0.0.1:18091/xa/phase2")
            .expect("PostgreSQL commit branch");
    let (registration_url, registration) = registration_server();
    let client = DtmHttpClient::new(registration_url).expect("PostgreSQL registration client");
    let inserted_id = commit_id.clone();
    let prepared = manager
        .prepare_branch(&client, &commit_branch, move |connection| {
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO roze_xa_integration_accounts (id, amount) VALUES ($1, $2)",
                )
                .bind(&inserted_id)
                .bind(84_i64)
                .execute(&mut *connection)
                .await?;
                Ok(84_i64)
            })
        })
        .await
        .expect("prepare PostgreSQL XA branch");
    assert!(matches!(prepared, XaLocalOutcome::Prepared(84)));
    let registration = registration
        .join()
        .expect("join PostgreSQL registration server");
    assert!(registration.contains("/api/dtmsvr/registerXaBranch"));
    assert!(registration.contains(&commit_id));
    assert!(manager
        .recover_prepared()
        .await
        .expect("recover PostgreSQL XA branches")
        .contains(&commit_branch.resource_id()));

    let decision = XaHeuristicRequest::new(
        unique_id("pg-decision"),
        XaPhase2::Commit,
        "integration commit",
    )
    .expect("PostgreSQL heuristic request");
    let resolved = manager
        .resolve_heuristically(&commit_branch, &decision)
        .await
        .expect("commit PostgreSQL XA branch");
    assert_eq!(resolved.outcome, XaPhase2Outcome::Applied);
    assert_eq!(resolved.record.status, XaHeuristicStatus::Applied);
    assert_eq!(
        manager
            .resolve_heuristically(&commit_branch, &decision)
            .await
            .expect("replay PostgreSQL heuristic request")
            .record,
        resolved.record
    );
    let amount: i64 = sqlx::query("SELECT amount FROM roze_xa_integration_accounts WHERE id = $1")
        .bind(&commit_id)
        .fetch_one(&pool)
        .await
        .expect("read committed PostgreSQL XA row")
        .try_get(0)
        .expect("decode committed PostgreSQL amount");
    assert_eq!(amount, 84);

    let duplicate = manager
        .prepare_branch(&client, &commit_branch, |_| Box::pin(async { Ok(()) }))
        .await
        .expect("repeat PostgreSQL XA branch");
    assert!(matches!(duplicate, XaLocalOutcome::Duplicate));
    assert!(manager
        .reconcile()
        .await
        .expect("reconcile PostgreSQL XA state")
        .pending_decisions
        .is_empty());

    let rollback_id = unique_id("pg-rollback");
    let rollback_branch =
        XaBranchDescriptor::new(&rollback_id, "account", "http://127.0.0.1:18091/xa/phase2")
            .expect("PostgreSQL rollback branch");
    let (registration_url, registration) = registration_server();
    let client = DtmHttpClient::new(registration_url).expect("PostgreSQL rollback client");
    let inserted_id = rollback_id.clone();
    manager
        .prepare_branch(&client, &rollback_branch, move |connection| {
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO roze_xa_integration_accounts (id, amount) VALUES ($1, $2)",
                )
                .bind(&inserted_id)
                .bind(9_i64)
                .execute(&mut *connection)
                .await?;
                Ok(())
            })
        })
        .await
        .expect("prepare PostgreSQL rollback branch");
    registration
        .join()
        .expect("join PostgreSQL rollback registration");
    assert_eq!(
        manager
            .resolve(&rollback_branch, XaPhase2::Rollback)
            .await
            .expect("roll back PostgreSQL XA branch"),
        XaPhase2Outcome::Applied
    );
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM roze_xa_integration_accounts WHERE id = $1")
        .bind(&rollback_id)
        .fetch_one(&pool)
        .await
        .expect("count rolled back PostgreSQL row")
        .try_get(0)
        .expect("decode PostgreSQL rollback count");
    assert_eq!(count, 0);
    sqlx::query("DELETE FROM roze_xa_integration_accounts WHERE id = $1")
        .bind(&commit_id)
        .execute(&pool)
        .await
        .expect("clean PostgreSQL XA row");
}
