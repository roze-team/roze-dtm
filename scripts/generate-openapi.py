#!/usr/bin/env python3
"""Generate the checked-in OpenAPI 3.1 contract for the handwritten Roze DTM service."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "service" / "static" / "openapi.json"
REF = "#/components/schemas/"


def ref(name: str) -> dict:
    return {"$ref": REF + name}


def response(schema: dict, content_type: str = "application/json") -> dict:
    return {"200": {"description": "Success", "content": {content_type: {"schema": schema}}}}


def operation(operation_id: str, tag: str, schema: dict, *, body: str | None = None,
              params: list[dict] | None = None, public: bool = False,
              content_type: str = "application/json") -> dict:
    item = {
        "operationId": operation_id,
        "summary": re.sub(r"(?<!^)(?=[A-Z])", " ", operation_id).replace("Kv", "KV").replace("Gid", "GID").strip().capitalize(),
        "tags": [tag],
        "responses": response(schema, content_type),
    }
    if body:
        item["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": ref(body)}},
        }
    if params:
        item["parameters"] = params
    if public:
        item["security"] = []
    return item


def query(name: str, schema: dict | None = None, required: bool = False) -> dict:
    return {"name": name, "in": "query", "required": required, "schema": schema or {"type": "string"}}


def path_param(name: str) -> dict:
    return {"name": name, "in": "path", "required": True, "schema": {"type": "string", "minLength": 1, "maxLength": 128}}


string_map = {"type": "object", "additionalProperties": {"type": "string"}}
nullable_string = {"type": ["string", "null"]}
nullable_u64 = {"type": ["integer", "null"], "minimum": 0}
kind_values = ["Saga", "Workflow", "Message", "Xa", "Tcc"]
status_values = ["Submitted", "Trying", "Prepared", "Succeeding", "Succeeded", "Aborting", "Aborted", "Failed"]
branch_kind_values = ["SagaAction", "SagaCompensate", "TccTry", "TccConfirm", "TccCancel", "WorkflowAction", "MessageAction", "XaAction"]
branch_status_values = ["Pending", "Running", "Compensating", "Succeeded", "Failed", "Skipped"]

schemas = {
    "JsonValue": {},
    "TransactionOptions": {
        "type": "object",
        "properties": {
            "wait_result": {"type": "boolean", "default": False},
            "concurrent": {"type": "boolean", "default": False, "description": "Run Saga branches by dependency-ready concurrent layers"},
            "delay_millis": {"type": ["integer", "null"], "minimum": 1, "maximum": 31_536_000_000, "description": "Message-only delivery delay from transaction creation time"},
            "retry_interval_millis": nullable_u64,
            "request_timeout_millis": nullable_u64,
            "retry_limit": nullable_u64,
            "branch_headers": string_map,
        },
        "additionalProperties": False,
    },
    "BranchRequest": {
        "type": "object", "required": ["id", "action"], "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 128},
            "kind": {"anyOf": [{"type": "string", "enum": branch_kind_values}, {"type": "null"}]},
            "action": {"type": "string"}, "compensate": nullable_string,
            "confirm": nullable_string, "cancel": nullable_string,
            "payload": ref("JsonValue"),
            "dependencies": {"type": "array", "items": {"type": "string"}},
        },
    },
    "SubmitTransactionRequest": {
        "type": "object", "required": ["gid", "branches"], "additionalProperties": False,
        "properties": {
            "gid": {"type": "string", "minLength": 1, "maxLength": 128},
            "kind": {"anyOf": [{"type": "string", "enum": kind_values}, {"type": "null"}]},
            "branches": {"type": "array", "items": ref("BranchRequest")},
            "timeout_millis": nullable_u64, "metadata": string_map,
            "options": ref("TransactionOptions"),
        },
    },
    "Branch": {
        "allOf": [ref("BranchRequest"), {"type": "object", "required": ["kind", "payload", "status", "attempts", "last_error", "next_retry_millis", "dependencies"], "properties": {
            "kind": {"type": "string", "enum": branch_kind_values}, "payload": ref("JsonValue"),
            "status": {"type": "string", "enum": branch_status_values}, "attempts": {"type": "integer", "minimum": 0},
            "last_error": nullable_string, "next_retry_millis": nullable_u64,
            "dependencies": {"type": "array", "items": {"type": "string"}},
        }}],
    },
    "Transaction": {
        "type": "object", "required": ["gid", "kind", "status", "branches", "created_at_millis", "updated_at_millis", "revision", "timeout_millis", "options", "metadata"],
        "properties": {
            "gid": {"type": "string"}, "kind": {"type": "string", "enum": kind_values},
            "status": {"type": "string", "enum": status_values}, "branches": {"type": "array", "items": ref("Branch")},
            "created_at_millis": {"type": "integer", "minimum": 0}, "updated_at_millis": {"type": "integer", "minimum": 0},
            "revision": {"type": "integer", "minimum": 0}, "timeout_millis": nullable_u64,
            "options": ref("TransactionOptions"), "workflow_progresses": {"type": "array", "items": {"type": "object"}},
            "metadata": string_map,
        },
    },
    "TransactionPage": {"type": "object", "required": ["items", "offset", "limit", "total"], "properties": {
        "items": {"type": "array", "items": ref("Transaction")}, "offset": {"type": "integer", "minimum": 0},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200}, "total": {"type": "integer", "minimum": 0},
    }},
    "TransactionStats": {"type": "object", "required": ["total", "by_kind", "by_status"], "properties": {
        "total": {"type": "integer", "minimum": 0}, "by_kind": {"type": "object", "additionalProperties": {"type": "integer"}},
        "by_status": {"type": "object", "additionalProperties": {"type": "integer"}},
    }},
    "RecoveryResult": {"type": "object", "required": ["recovered", "count"], "properties": {
        "recovered": {"type": "array", "items": ref("Transaction")}, "count": {"type": "integer", "minimum": 0},
    }},
    "DashboardSnapshot": {"type": "object", "description": "Bounded, redacted Roze Admin dashboard snapshot"},
    "XaReconciliationSnapshot": {"type": "object", "description": "Redacted XA reconciliation snapshot"},
    "CompatTransactionRequest": {"type": "object", "required": ["gid", "trans_type"], "properties": {
        "gid": {"type": "string"}, "trans_type": {"type": "string", "enum": ["tcc", "saga", "workflow", "msg", "message", "xa"]},
        "steps": {"type": "array", "items": string_map}, "payloads": {"type": "array", "items": ref("JsonValue")},
        "timeout_to_fail": nullable_u64, "rollback_reason": nullable_string,
        "custom_data": {**nullable_string, "description": "Opaque upstream data; Saga concurrent/orders maps to the execution DAG and Message delay is measured in seconds"},
        "query_prepared": nullable_string, "wait_result": {"type": "boolean"}, "retry_interval": nullable_u64,
        "request_timeout": nullable_u64, "retry_limit": nullable_u64, "branch_headers": string_map, "req_extra": string_map,
    }},
    "CompatBranchRequest": {"type": "object", "required": ["gid", "trans_type", "branch_id"], "properties": {
        "gid": {"type": "string"}, "trans_type": {"type": "string", "enum": ["tcc", "xa", "workflow"]},
        "branch_id": {"type": "string"}, "data": nullable_string, "op": nullable_string, "status": nullable_string,
        "confirm": nullable_string, "cancel": nullable_string, "url": nullable_string,
    }},
    "CompatAdminRequest": {"type": "object", "required": ["gid"], "properties": {"gid": {"type": "string"}}},
    "CompatSuccess": {"type": "object", "required": ["dtm_result"], "properties": {"dtm_result": {"const": "SUCCESS"}}},
    "CompatVersion": {"type": "object", "required": ["version", "release_revision"], "properties": {
        "version": {"type": "string"}, "release_revision": {"type": ["string", "null"], "pattern": "^[0-9a-f]{40}$"},
    }},
    "CompatPayload": {"type": "object", "required": ["dtm_result"], "properties": {"dtm_result": {"enum": ["SUCCESS", "FAILURE"]}}, "additionalProperties": True},
    "JsonRpcRequest": {"type": "object", "required": ["jsonrpc", "id", "method"], "properties": {
        "jsonrpc": {"const": "2.0"}, "id": ref("JsonValue"), "method": {"enum": ["newGid", "prepare", "submit", "abort", "registerBranch"]}, "params": ref("JsonValue"),
    }},
    "JsonRpcResponse": {"type": "object", "required": ["jsonrpc", "id"], "properties": {
        "jsonrpc": {"const": "2.0"}, "id": ref("JsonValue"), "result": ref("JsonValue"),
        "error": {"type": "object", "required": ["code", "message"], "properties": {"code": {"type": "integer"}, "message": {"type": "string"}}},
    }},
}


def envelope(name: str) -> dict:
    return {"type": "object", "required": ["code", "message", "data"], "properties": {
        "code": {"type": "integer"}, "message": {"type": "string"}, "data": ref(name), "trace_id": {"type": "string"},
    }}


for name in ["Transaction", "TransactionPage", "TransactionStats", "RecoveryResult", "DashboardSnapshot", "XaReconciliationSnapshot"]:
    schemas[name + "Envelope"] = envelope(name)

paths: dict[str, dict] = {}
paths["/dashboard"] = {"get": operation("dashboardHtml", "Management", {"type": "string"}, public=True, content_type="text/html")}
for route, op in [("/healthz", "health"), ("/startupz", "startup"), ("/readyz", "ready")]:
    paths[route] = {"get": operation(op, "Operations", envelope("JsonValue"), public=True)}
for route, op in [("/metrics", "metrics"), ("/api/metrics", "compatMetrics")]:
    paths[route] = {"get": operation(op, "Operations", {"type": "string"}, public=True, content_type="text/plain")}
paths["/openapi.json"] = {"get": operation("openapi", "Operations", {"type": "object"}, public=True)}

submits = [("/v1/tcc", "submitTcc"), ("/v1/saga", "submitSaga"), ("/v1/workflows", "submitWorkflow"), ("/v1/messages", "submitMessage"), ("/v1/xa", "submitXa")]
for route, op in submits:
    paths[route] = {"post": operation(op, "Native", ref("TransactionEnvelope"), body="SubmitTransactionRequest")}
transitions = [
    ("tcc", "prepare"), ("tcc", "confirm"), ("tcc", "cancel"), ("saga", "start"), ("saga", "abort"),
    ("workflows", "start"), ("workflows", "abort"), ("messages", "prepare"), ("messages", "dispatch"),
    ("messages", "abort"), ("xa", "prepare"), ("xa", "commit"), ("xa", "rollback"),
]
for group, action in transitions:
    route = f"/v1/{group}/{{gid}}/{action}"
    paths[route] = {"post": operation(action + group.title(), "Native", ref("TransactionEnvelope"), params=[path_param("gid")])}
paths["/v1/xa/reconciliation"] = {"get": operation("xaReconciliation", "Native", ref("XaReconciliationSnapshotEnvelope"))}
list_params = [query("gid"), query("kind"), query("status"), query("offset", {"type": "integer", "minimum": 0}), query("limit", {"type": "integer", "minimum": 1, "maximum": 200})]
paths["/v1/transactions"] = {"get": operation("listTransactions", "Native", ref("TransactionPageEnvelope"), params=list_params)}
paths["/v1/transactions/{gid}"] = {"get": operation("getTransaction", "Native", ref("TransactionEnvelope"), params=[path_param("gid")])}
for action in ["recover", "force-stop", "reset-retry"]:
    paths[f"/v1/transactions/{{gid}}/{action}"] = {"post": operation(action.replace("-", "") + "Transaction", "Native", ref("TransactionEnvelope"), params=[path_param("gid")])}
paths["/v1/recover"] = {"post": operation("recoverAll", "Native", ref("RecoveryResultEnvelope"))}
paths["/v1/stats"] = {"get": operation("stats", "Native", ref("TransactionStatsEnvelope"))}
paths["/v1/dashboard"] = {"get": operation("dashboardSnapshot", "Native", ref("DashboardSnapshotEnvelope"), params=list_params)}

compat_gets = ["version", "newGid", "query", "all", "resetCronTime", "subscribe", "unsubscribe", "scanKV", "queryKV"]
compat_params = {
    "query": [query("gid", required=True)],
    "all": [query("gid"), query("transType"), query("status"), query("position"), query("limit", {"type": "integer"}), query("createTimeStart", {"type": "integer"}), query("createTimeEnd", {"type": "integer"})],
    "resetCronTime": [query("timeout", {"type": "integer", "default": 105}), query("limit", {"type": "integer", "default": 100})],
    "subscribe": [query("topic", required=True), query("url", {"type": "string", "format": "uri"}, True), query("remark")],
    "unsubscribe": [query("topic", required=True), query("url", {"type": "string", "format": "uri"}, True)],
    "scanKV": [query("cat"), query("position"), query("limit", {"type": "integer"})],
    "queryKV": [query("cat"), query("key")],
}
for name in compat_gets:
    response_schema = ref("CompatVersion") if name == "version" else ref("CompatPayload")
    paths[f"/api/dtmsvr/{name}"] = {"get": operation("compat" + name[0].upper() + name[1:], "Compatibility", response_schema, params=compat_params.get(name), public=name == "version")}
for name in ["prepare", "submit", "abort"]:
    paths[f"/api/dtmsvr/{name}"] = {"post": operation("compat" + name.title(), "Compatibility", ref("CompatSuccess"), body="CompatTransactionRequest")}
for name in ["registerBranch", "registerTccBranch", "registerXaBranch"]:
    paths[f"/api/dtmsvr/{name}"] = {"post": operation("compat" + name[0].upper() + name[1:], "Compatibility", ref("CompatSuccess"), body="CompatBranchRequest")}
paths["/api/dtmsvr/prepareWorkflow"] = {"post": operation("compatPrepareWorkflow", "Compatibility", ref("CompatPayload"), body="CompatTransactionRequest")}
for name in ["forceStop", "resetNextCronTime"]:
    paths[f"/api/dtmsvr/{name}"] = {"post": operation("compat" + name[0].upper() + name[1:], "Compatibility", ref("CompatSuccess"), body="CompatAdminRequest")}
paths["/api/dtmsvr/topic/{topic_name}"] = {"delete": operation("compatDeleteTopic", "Compatibility", ref("CompatSuccess"), params=[path_param("topic_name")])}
paths["/api/json-rpc"] = {"post": operation("jsonRpc", "Compatibility", ref("JsonRpcResponse"), body="JsonRpcRequest")}

document = {
    "openapi": "3.1.0",
    "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema",
    "info": {"title": "Roze DTM API", "version": "0.1.0", "license": {"name": "MIT", "identifier": "MIT"}},
    "servers": [{"url": "/"}],
    "security": [{"bearerAuth": []}],
    "tags": [
        {"name": "Native", "description": "Roze-native transaction control plane"},
        {"name": "Compatibility", "description": "dtm-labs HTTP and JSON-RPC compatibility surface"},
        {"name": "Management", "description": "Roze Admin-compatible management resources"},
        {"name": "Operations", "description": "Health, readiness, metrics, and contract endpoints"},
    ],
    "paths": dict(sorted(paths.items())),
    "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}, "schemas": schemas},
}

OUT.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"wrote {OUT} with {len(paths)} paths")
