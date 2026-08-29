#!/usr/bin/env python3
"""Fail-closed validation for revision-bound Roze DTM soak evidence."""

import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit("usage: validate-soak-evidence.py <soak-report.json>")

report_path = Path(sys.argv[1]).resolve()
evidence_dir = report_path.parent
report = json.loads(report_path.read_text(encoding="utf-8"))
required = {
    "schema_version", "area", "profile", "qualification", "verdict", "revision",
    "topology", "started_at", "finished_at", "target_duration_seconds",
    "elapsed_seconds", "interval_seconds", "workload", "error_budget",
    "interrupted", "fault_timeline", "samples",
}
assert required <= report.keys()
assert report["schema_version"] == 1 and report["area"] == "production-soak"
assert report["profile"] in {"smoke", "24h", "72h"}
assert report["qualification"] == ("harness_only" if report["profile"] == "smoke" else report["profile"])
assert report["verdict"] == "pass" and report["interrupted"] is False
assert re.fullmatch(r"[0-9a-f]{40}", report["revision"])
minimums = {"smoke": 1, "24h": 86_400, "72h": 259_200}
assert report["target_duration_seconds"] >= minimums[report["profile"]]
assert report["elapsed_seconds"] >= report["target_duration_seconds"]
started = datetime.fromisoformat(report["started_at"].replace("Z", "+00:00"))
finished = datetime.fromisoformat(report["finished_at"].replace("Z", "+00:00"))
assert finished >= started
assert (finished - started).total_seconds() + 2 >= report["elapsed_seconds"]
assert isinstance(report["topology"], dict) and report["topology"]
assert isinstance(report["fault_timeline"], list)
for entry in report["fault_timeline"]:
    assert isinstance(entry, dict)
    assert all(isinstance(entry.get(field), str) and entry[field].strip() for field in ("at", "fault", "outcome"))
assert report["workload"] == {"kind": "production-http-contract", "samples": len(report["samples"])}
assert report["samples"]
failed = 0
validator = Path(__file__).with_name("validate-production-evidence.py")
for sequence, sample in enumerate(report["samples"], 1):
    assert sample["sequence"] == sequence
    assert sample["verdict"] in {"pass", "fail"}
    failed += sample["verdict"] != "pass"
    relative_path = Path(sample["report_path"])
    assert not relative_path.is_absolute() and ".." not in relative_path.parts
    sample_path = (evidence_dir / relative_path).resolve()
    assert evidence_dir in sample_path.parents
    content = sample_path.read_bytes()
    assert hashlib.sha256(content).hexdigest() == sample["report_sha256"]
    sample_report = json.loads(content)
    assert sample_report["revision"] == report["revision"]
    assert sample_report["topology"] == report["topology"]
    if sample["verdict"] == "pass":
        subprocess.run([sys.executable, str(validator), str(sample_path)], check=True)
assert report["error_budget"]["failed_samples"] == failed
assert failed <= report["error_budget"]["max_failed_samples"]
print(f"validated {report['qualification']} soak evidence for revision {report['revision']} with {len(report['samples'])} samples")
