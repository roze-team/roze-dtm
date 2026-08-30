#!/usr/bin/env python3
"""Static closure gate for the Roze-native DTM migration."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORE = (ROOT / "src/lib.rs").read_text(encoding="utf-8")
SERVICE = (ROOT / "service/src/main.rs").read_text(encoding="utf-8")
GRPC = (ROOT / "service/src/grpc.rs").read_text(encoding="utf-8")
HTTP_CLIENT = (ROOT / "src/client.rs").read_text(encoding="utf-8")
GRPC_CLIENT = (ROOT / "src/grpc_client.rs").read_text(encoding="utf-8")
XA = (ROOT / "src/xa.rs").read_text(encoding="utf-8")
REDIS_STORE = (ROOT / "src/redis_store.rs").read_text(encoding="utf-8")
README = (ROOT / "README.md").read_text(encoding="utf-8")
CLOSURE = (ROOT / "docs/migration-closure.md").read_text(encoding="utf-8")
PRODUCTION = (ROOT / "docs/production-validation.md").read_text(encoding="utf-8")
CI = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
MESSAGE_RESTART = (ROOT / "scripts/message-restart-integration.mjs").read_text(encoding="utf-8")

for variant in ["Tcc", "Saga", "Workflow", "Message", "Xa"]:
    assert f"    {variant}," in CORE, {"missing_transaction_kind": variant}

for store in [
    "InMemoryTransactionStore", "SqliteTransactionStore",
    "PostgresTransactionStore", "MySqlTransactionStore",
]:
    assert f"pub struct {store}" in CORE, {"missing_store": store}
assert "pub struct RedisTransactionStore" in REDIS_STORE

for method in [
    "prepare_tcc", "confirm_tcc", "cancel_tcc", "start_saga", "abort_saga",
    "prepare_message", "dispatch_message", "abort_message",
    "prepare_workflow", "start_workflow", "abort_workflow",
    "prepare_xa", "commit_xa", "rollback_xa", "tick_recover_once_with_lease",
]:
    assert f"pub async fn {method}" in CORE, {"missing_core_method": method}

for contract in [
    "register_branch", "record_workflow_progress", "finish_workflow",
    "defer_workflow_recovery", "barrier_fenced", "acquire_recovery_lease",
    "delete_transaction_if_unchanged", "get_kv", "list_kv", "create_kv",
    "update_kv", "delete_kv",
]:
    assert f"async fn {contract}" in CORE, {"missing_store_contract": contract}

for source, methods in [
    (HTTP_CLIENT, ["prepare_xa", "register_xa_branch", "prepare_callback_workflow", "subscribe_topic", "query_kv"]),
    (GRPC_CLIENT, ["new_gid", "register_branch", "prepare_workflow", "subscribe", "delete_topic"]),
    (GRPC, ["new_gid", "submit", "prepare", "abort", "register_branch", "prepare_workflow", "subscribe", "unsubscribe", "delete_topic"]),
    (XA, ["prepare_branch", "recover_prepared", "resolve_heuristically", "reconcile"]),
]:
    for method in methods:
        assert f"fn {method}" in source, {"missing_client_or_xa_method": method}

for requirement in [
    "ServiceGroup", "HealthRegistry", "control_token", "release_revision",
    "allowed_branch_origins", "roze_log::audit_", "roze_dtm_metrics_registry_available",
    "roze_dtm_transaction_transitions_total", "roze_dtm_branch_state_observations_total",
    "roze_dtm_retry_scheduled_observations_total",
    "roze_dtm_retention_deleted_total", "roze_dtm_retention_conflicts_total",
    "dtm.compat.http.prepare", "dtm.compat.json_rpc.prepare",
    "dtm.recovery.completed", "dtm.retention.completed", "audit_resource_operation",
]:
    assert requirement in SERVICE, {"missing_roze_governance": requirement}

for requirement in [
    "dtm.compat.grpc.register_branch", "dtm.compat.grpc.prepare_workflow",
    "audit_transition", "audit_compat_failure", "audit_resource_operation",
]:
    assert requirement in GRPC, {"missing_grpc_governance": requirement}

for requirement in [
    "DELETE_TRANSACTION_IF_UNCHANGED", "barrier_index_prefix", "SMEMBERS",
]:
    assert requirement in REDIS_STORE, {"missing_redis_retention_contract": requirement}

for artifact in [
    "docs/dtm.md", "docs/dtm-grpc.md", "docs/dtm-compatibility.md",
    "docs/production-validation.md", "docs/migration-closure.md",
    "THIRD_PARTY_NOTICES.md", "service/static/openapi.json",
    "sdk/roze-dtm.ts", "sdk/roze-dtm-compat.ts",
    "scripts/message-restart-integration.mjs",
]:
    assert (ROOT / artifact).is_file(), {"missing_artifact": artifact}

for phrase in [
    "Roze 原生替代，不作为未迁移缺口",
    "编译禁令解除后的验收进展",
    "cargo fmt --all -- --check",
    "完整本地短时验收与真实依赖 CI 已通过",
    "DataExpire", "FinishedDataExpire",
    "24h/72h soak",
]:
    assert phrase in CLOSURE, {"missing_closure_statement": phrase}

assert "scripts/validate-migration-closure.py" in README
assert "node scripts/message-restart-integration.mjs" in CI
for evidence in [
    'delay_millis: 4_000', 'child.kill("SIGKILL")',
    '"restart-worker-before"', '"restart-worker-after"',
    'branchCalls === 1', 'waitForStatus(gid, "succeeded"',
]:
    assert evidence in MESSAGE_RESTART, {"missing_message_restart_evidence": evidence}
assert "scripts/message-restart-integration.mjs" in PRODUCTION
print("validated Roze baseline superset, five transaction modes, five stores, clients, governance and closure evidence")
