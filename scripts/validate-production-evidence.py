#!/usr/bin/env python3
"""Fail-closed validation for Roze DTM HTTP production evidence bundles."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit("usage: validate-production-evidence.py <http-contract-report.json>")

report_path = Path(sys.argv[1]).resolve()
report = json.loads(report_path.read_text(encoding="utf-8"))
required = {
    "schema_version", "area", "verdict", "revision", "started_at", "finished_at",
    "duration_ms", "command", "base_origin", "topology", "checks", "artifacts",
}
assert required <= set(report), {"missing_fields": sorted(required - set(report))}
assert report["schema_version"] == 1
assert report["area"] == "http-contract"
assert report["verdict"] in {"pass", "fail", "inconclusive"}
assert re.fullmatch(r"[0-9a-f]{40}", report["revision"])
started = datetime.fromisoformat(report["started_at"].replace("Z", "+00:00"))
finished = datetime.fromisoformat(report["finished_at"].replace("Z", "+00:00"))
assert finished >= started
assert isinstance(report["duration_ms"], int) and report["duration_ms"] >= 0
assert report["command"] == "node scripts/production-http-contract-smoke.mjs"
assert re.fullmatch(r"https?://[^/?#]+(?::\d+)?", report["base_origin"])

topology = report["topology"]
assert isinstance(topology, dict)
assert isinstance(topology.get("store"), str) and topology["store"]
assert isinstance(topology.get("replica_count"), int) and topology["replica_count"] > 0
assert isinstance(topology.get("dependencies"), list)

checks = report["checks"]
assert isinstance(checks, list) and len(checks) >= 10
names = [item.get("name") for item in checks]
assert len(names) == len(set(names))
for item in checks:
    assert set(item) == {"name", "outcome", "elapsed_ms", "detail"}
    assert isinstance(item["name"], str) and item["name"]
    assert item["outcome"] in {"pass", "fail"}
    assert isinstance(item["elapsed_ms"], int) and item["elapsed_ms"] >= 0
    assert isinstance(item["detail"], str) and len(item["detail"]) <= 512

if report["verdict"] == "pass":
    assert all(item["outcome"] == "pass" for item in checks)
elif report["verdict"] == "fail":
    assert any(item["outcome"] == "fail" for item in checks)

artifacts = report["artifacts"]
assert isinstance(artifacts, list) and len(artifacts) >= 2
for artifact in artifacts:
    assert set(artifact) == {"path", "sha256", "bytes"}
    relative = Path(artifact["path"])
    assert not relative.is_absolute() and ".." not in relative.parts
    path = (report_path.parent / relative).resolve()
    assert path.is_relative_to(report_path.parent)
    payload = path.read_bytes()
    assert len(payload) == artifact["bytes"]
    assert hashlib.sha256(payload).hexdigest() == artifact["sha256"]

print(f"validated {report['verdict']} evidence for revision {report['revision']} with {len(checks)} checks")
