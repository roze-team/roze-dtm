use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use roze_dtm::{
    BarrierDecision, Branch, BranchBarrier, MySqlTransactionStore, PostgresTransactionStore,
    Transaction, TransactionStatus, TransactionStore,
};

fn unique_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

async fn exercise_store(store: Arc<dyn TransactionStore>, prefix: &str) {
    let gid = unique_id(prefix);
    let mut transaction = Transaction::tcc(
        &gid,
        vec![Branch::tcc_try(
            "inventory",
            "http://inventory/try",
            "http://inventory/confirm",
            "http://inventory/cancel",
            serde_json::json!({"sku": "A", "count": 1}),
        )],
    );
    store
        .insert_transaction(transaction.clone())
        .await
        .expect("insert transaction");
    let stored = store
        .get_transaction(&gid)
        .await
        .expect("read transaction")
        .expect("transaction exists");
    assert_eq!(stored, transaction);

    transaction.status = TransactionStatus::Trying;
    store
        .update_transaction(transaction.clone())
        .await
        .expect("update transaction");
    assert_eq!(
        store
            .get_transaction(&gid)
            .await
            .expect("read updated transaction")
            .expect("updated transaction exists")
            .status,
        TransactionStatus::Trying
    );

    let barrier = BranchBarrier::new(&gid, "inventory", "try");
    assert_eq!(
        store.barrier(barrier.clone()).await.expect("first barrier"),
        BarrierDecision::Execute
    );
    assert_eq!(
        store
            .barrier(barrier.clone())
            .await
            .expect("duplicate barrier"),
        BarrierDecision::SkipDuplicate
    );

    assert!(!store
        .delete_transaction_if_unchanged(&transaction)
        .await
        .expect("reject stale retention snapshot"));
    let current = store
        .get_transaction(&gid)
        .await
        .expect("read retention snapshot")
        .expect("retention transaction exists");
    assert!(store
        .delete_transaction_if_unchanged(&current)
        .await
        .expect("delete unchanged retention snapshot"));
    assert_eq!(
        store.barrier(barrier).await.expect("barrier was cleaned"),
        BarrierDecision::Execute
    );

    let lease_name = unique_id("recovery");
    assert!(store
        .try_acquire_recovery_lease(&lease_name, "worker-a", 10_000)
        .await
        .expect("acquire lease"));
    assert!(!store
        .try_acquire_recovery_lease(&lease_name, "worker-b", 10_000)
        .await
        .expect("reject competing lease"));
    assert!(store
        .try_acquire_recovery_lease(&lease_name, "worker-a", 10_000)
        .await
        .expect("renew lease"));
}

#[tokio::test]
async fn postgres_store_contract() {
    let Ok(database_url) = std::env::var("ROZE_DTM_TEST_POSTGRES_URL") else {
        eprintln!("ROZE_DTM_TEST_POSTGRES_URL is unset; skipping PostgreSQL integration test");
        return;
    };
    let store = PostgresTransactionStore::connect(&database_url)
        .await
        .expect("connect PostgreSQL store");
    exercise_store(Arc::new(store), "postgres").await;
}

#[tokio::test]
async fn mysql_store_contract() {
    let Ok(database_url) = std::env::var("ROZE_DTM_TEST_MYSQL_URL") else {
        eprintln!("ROZE_DTM_TEST_MYSQL_URL is unset; skipping MySQL integration test");
        return;
    };
    let store = MySqlTransactionStore::connect(&database_url)
        .await
        .expect("connect MySQL store");
    exercise_store(Arc::new(store), "mysql").await;
}
