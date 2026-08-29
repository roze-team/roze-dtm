#!/usr/bin/env python3
"""Validate the pinned dtm-labs HTTP, JSON-RPC, gRPC and DTO surface statically."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICE = (ROOT / "service/src/main.rs").read_text(encoding="utf-8")
PROTO = (ROOT / "proto/dtmgimp.proto").read_text(encoding="utf-8")
SDK = (ROOT / "sdk/roze-dtm-compat.js").read_text(encoding="utf-8")
DOC = (ROOT / "docs/dtm-compatibility.md").read_text(encoding="utf-8")
NOTICE = (ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")
GO_INTEROP_MOD = (ROOT / "interop/dtm-labs-go/go.mod").read_text(encoding="utf-8")
GO_INTEROP_CLIENT = (ROOT / "interop/dtm-labs-go/main.go").read_text(encoding="utf-8")
CI_PROTOCOL = (ROOT / "scripts/ci-protocol-integration.sh").read_text(encoding="utf-8")

PINNED_REVISION = "18146ee53bafbf094b1a5f12ca7e8a29bdb57edd"

expected_http = {
    ("GET", "/api/dtmsvr/version"),
    ("GET", "/api/dtmsvr/newGid"),
    ("POST", "/api/dtmsvr/prepare"),
    ("POST", "/api/dtmsvr/submit"),
    ("POST", "/api/dtmsvr/abort"),
    ("POST", "/api/dtmsvr/forceStop"),
    ("POST", "/api/dtmsvr/registerBranch"),
    ("POST", "/api/dtmsvr/registerXaBranch"),
    ("POST", "/api/dtmsvr/registerTccBranch"),
    ("POST", "/api/dtmsvr/prepareWorkflow"),
    ("GET", "/api/dtmsvr/query"),
    ("GET", "/api/dtmsvr/all"),
    ("GET", "/api/dtmsvr/resetCronTime"),
    ("GET", "/api/dtmsvr/subscribe"),
    ("GET", "/api/dtmsvr/unsubscribe"),
    ("DELETE", "/api/dtmsvr/topic/{topic_name}"),
    ("GET", "/api/dtmsvr/scanKV"),
    ("GET", "/api/dtmsvr/queryKV"),
    ("POST", "/api/dtmsvr/resetNextCronTime"),
    ("GET", "/api/metrics"),
    ("POST", "/api/json-rpc"),
}
route_calls = re.findall(
    r'\.route\(\s*"([^"]+)",\s*(get|post|delete)\(', SERVICE
)
actual_http = {(method.upper(), path) for path, method in route_calls}
missing_http = expected_http - actual_http
assert not missing_http, {"missing_compatibility_routes": sorted(missing_http)}

expected_rpcs = [
    "NewGid", "Submit", "Prepare", "Abort", "RegisterBranch",
    "PrepareWorkflow", "Subscribe", "Unsubscribe", "DeleteTopic",
]
actual_rpcs = re.findall(r"\brpc\s+(\w+)\s*\(", PROTO)
assert actual_rpcs == expected_rpcs, {"grpc_methods": actual_rpcs}

for declaration in [
    "bool WaitResult = 1;",
    "int64 TimeoutToFail = 2;",
    "int64 RetryInterval = 3;",
    "reserved 4;",
    "map<string, string> BranchHeaders = 5;",
    "int64 RequestTimeout = 6;",
    "int64 RetryLimit = 7;",
    "string Gid = 1;",
    "string TransType = 2;",
    "DtmTransOptions TransOptions = 3;",
    "string CustomedData = 4;",
    "repeated bytes BinPayloads = 5;",
    "string QueryPrepared = 6;",
    "string Steps = 7;",
    "map<string, string> ReqExtra = 8;",
    "string RollbackReason = 9;",
    "string BranchID = 3;",
    "string Op = 4;",
    "map<string, string> Data = 5;",
    "bytes BusiPayload = 6;",
]:
    assert declaration in PROTO, {"missing_proto_declaration": declaration}

for method in ["newGid", "prepare", "submit", "abort", "registerBranch"]:
    assert f'"{method}"' in SERVICE, {"missing_json_rpc_method": method}

for endpoint in [
    "/api/dtmsvr/version", "/api/dtmsvr/newGid", "/api/dtmsvr/query",
    "/api/dtmsvr/all", "/api/dtmsvr/prepareWorkflow",
    "/api/dtmsvr/resetCronTime", "/api/dtmsvr/scanKV",
    "/api/dtmsvr/queryKV", "/api/json-rpc",
]:
    assert endpoint in SDK, {"missing_sdk_endpoint": endpoint}

for wire_name in [
    "CompatGlobalTransaction", "CompatBranchTransaction", "CompatKvEntry",
    "compat_global_transaction", "compat_branch_transactions", "compat_kv_entry",
    'TransactionKind::Message => "msg"',
    'TransactionStatus::Succeeded => "succeed"',
]:
    assert wire_name in SERVICE, {"missing_wire_contract": wire_name}
assert '"transaction": &transaction' not in SERVICE
assert '"kv": entries,' not in SERVICE

assert PINNED_REVISION in DOC
assert PINNED_REVISION in NOTICE
assert "BSD 3-Clause License" in NOTICE
assert "github.com/dtm-labs/dtm v1.19.1-0.20260103134746-18146ee53baf" in GO_INTEROP_MOD
assert PINNED_REVISION in GO_INTEROP_CLIENT
assert "go run ." in CI_PROTOCOL

print(
    f"validated {len(expected_http)} compatibility routes, "
    f"{len(expected_rpcs)} gRPC methods and pinned attribution"
)
