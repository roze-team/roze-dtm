#!/usr/bin/env bash
set -euo pipefail

ran=0

if [[ -n "${ROZE_TEST_REDIS_URL:-}" ]]; then
  cargo test -p roze-dtm redis_store_round_trip_against_real_service -- --ignored --nocapture
  ran=1
fi

if [[ -n "${ROZE_TEST_REDIS_CLUSTER_URLS:-}" ]]; then
  cargo test -p roze-dtm redis_cluster_store_round_trip_against_real_service -- --ignored --nocapture
  if [[ -n "${ROZE_TEST_REDIS_CLUSTER_FAULT_SLOT:-}" ]]; then
    cargo test -p roze-dtm redis_cluster_handles_ask_and_moved_redirections -- --ignored --nocapture
  fi
  ran=1
fi

if [[ "$ran" -eq 0 ]]; then
  echo "set ROZE_TEST_REDIS_URL and/or ROZE_TEST_REDIS_CLUSTER_URLS" >&2
  exit 2
fi
