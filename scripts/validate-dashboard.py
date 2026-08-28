#!/usr/bin/env python3
"""Validate security and management invariants of the static DTM Dashboard."""

from __future__ import annotations

from collections import Counter
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "service" / "static" / "dashboard.html"


class DashboardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []
        self.external_assets: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if identifier := attributes.get("id"):
            self.ids.append(identifier)
        if tag == "script" and attributes.get("src"):
            self.external_assets.append(attributes["src"] or "")
        if tag == "link" and attributes.get("href"):
            self.external_assets.append(attributes["href"] or "")


source = DASHBOARD.read_text(encoding="utf-8")
parser = DashboardParser()
parser.feed(source)

duplicates = sorted(identifier for identifier, count in Counter(parser.ids).items() if count > 1)
assert not duplicates, {"duplicate_dashboard_ids": duplicates}
assert not parser.external_assets, {"external_dashboard_assets": parser.external_assets}
for required_id in [
    "token",
    "rows",
    "timeline",
    "action-modal",
    "action-gid",
    "action-confirm",
    "action-cancel",
]:
    assert required_id in parser.ids, {"missing_dashboard_id": required_id}

for forbidden in ["localStorage", "sessionStorage", "document.cookie"]:
    assert forbidden not in source, {"forbidden_dashboard_storage": forbidden}

for required in [
    "default-src 'none'",
    "connect-src 'self'",
    "available_actions",
    "encodeURIComponent(pending.gid)",
    "/v1/transactions/",
    "reset-retry",
    "force-stop",
]:
    assert required in source, {"missing_dashboard_contract": required}

print(f"validated Dashboard HTML with {len(parser.ids)} unique element ids")
