#!/usr/bin/env python3
"""Validate OpenAPI structure and exact coverage of the current HTTP router."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
document = json.loads((ROOT / "service/static/openapi.json").read_text(encoding="utf-8"))
source = (ROOT / "service/src/main.rs").read_text(encoding="utf-8")
routes = set(re.findall(r'\.route\("([^"]+)"', source))
paths = set(document["paths"])
assert routes == paths, {
    "missing_from_openapi": sorted(routes - paths),
    "not_in_router": sorted(paths - routes),
}

schemas = document["components"]["schemas"]
references = re.findall(
    r"#/components/schemas/([^\"/]+)", json.dumps(document)
)
operation_ids: list[str] = []
for path_item in document["paths"].values():
    operation_ids.extend(
        operation["operationId"]
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    )
    assert all(
        operation.get("summary")
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    ), "every operation must have a summary"

missing_references = sorted(set(references) - set(schemas))
assert not missing_references, {"missing_schema_references": missing_references}
assert len(operation_ids) == len(set(operation_ids)), "operationId values must be unique"
assert document["openapi"] == "3.1.0"
assert schemas["JsonValue"] == {}, "JsonValue must remain unrestricted"
assert schemas["DashboardSnapshot"]["additionalProperties"] is False
assert schemas["DashboardSnapshot"]["properties"]["transactions"] == {
    "$ref": "#/components/schemas/DashboardTransactionPage"
}
assert schemas["DashboardTransactionRow"]["properties"]["available_actions"]["items"][
    "enum"
] == ["reset-retry", "force-stop"]
for path, schema in {
    "/api/dtmsvr/query": "CompatQueryResponse",
    "/api/dtmsvr/all": "CompatAllResponse",
    "/api/dtmsvr/scanKV": "CompatKvScanResponse",
    "/api/dtmsvr/queryKV": "CompatKvResponse",
}.items():
    actual = document["paths"][path]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert actual == {"$ref": f"#/components/schemas/{schema}"}, (path, actual)
assert schemas["CompatGlobalTransaction"]["properties"]["trans_type"]["enum"] == [
    "tcc", "saga", "workflow", "msg", "xa"
]
assert schemas["CompatGlobalTransaction"]["properties"]["status"]["enum"] == [
    "prepared", "submitted", "succeed", "aborting", "failed"
]
assert schemas["CompatBranchTransaction"]["properties"]["bin_data"][
    "contentEncoding"
] == "base64"
assert "created_at_millis" not in schemas["CompatGlobalTransaction"]["properties"]
assert all(tag.get("description") for tag in document["tags"])
assert len(paths) == 54
print(
    f"validated {len(paths)} routes, {len(operation_ids)} operations, "
    f"and {len(references)} schema references"
)
