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
references: list[str] = []
operation_ids: list[str] = []
for path_item in document["paths"].values():
    references.extend(
        re.findall(r"#/components/schemas/([^\"/]+)", json.dumps(path_item))
    )
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
assert all(tag.get("description") for tag in document["tags"])
assert len(paths) == 54
print(
    f"validated {len(paths)} routes, {len(operation_ids)} operations, "
    f"and {len(references)} schema references"
)
