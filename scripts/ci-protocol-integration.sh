#!/usr/bin/env bash
set -euo pipefail

export ROZE_CONFIG_PATH="${ROZE_CONFIG_PATH:-service/config.sqlite.smoke.yaml}"
export ROZE_DTM_CONTROL_TOKEN="${ROZE_DTM_CONTROL_TOKEN:-roze-dtm-smoke-token-32-bytes!!}"
export ROZE_DTM_RELEASE_REVISION="${ROZE_DTM_RELEASE_REVISION:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
export ROZE_DTM_EXPECTED_REVISION="$ROZE_DTM_RELEASE_REVISION"
export ROZE_DTM_BASE_URL="${ROZE_DTM_BASE_URL:-http://127.0.0.1:18090}"
export ROZE_DTM_GRPC_ENDPOINT="${ROZE_DTM_GRPC_ENDPOINT:-http://127.0.0.1:36791}"

service_log="${RUNNER_TEMP:-/tmp}/roze-dtm-protocol-service.log"
cargo run -p roze-dtm-service >"$service_log" 2>&1 &
service_pid=$!

cleanup() {
  kill "$service_pid" 2>/dev/null || true
  wait "$service_pid" 2>/dev/null || true
}
failure() {
  status=$?
  printf 'Protocol integration failed; service log follows:\n' >&2
  tail -n 200 "$service_log" >&2 || true
  exit "$status"
}
trap cleanup EXIT
trap failure ERR

for attempt in $(seq 1 120); do
  if curl --fail --silent --show-error "$ROZE_DTM_BASE_URL/readyz" >/dev/null; then
    break
  fi
  if ! kill -0 "$service_pid" 2>/dev/null; then
    printf 'roze-dtm-service exited before readiness\n' >&2
    false
  fi
  if [[ "$attempt" -eq 120 ]]; then
    printf 'roze-dtm-service did not become ready\n' >&2
    false
  fi
  sleep 1
done

node scripts/local-protocol-integration.mjs
node scripts/sdk-protocol-integration.mjs
node --experimental-transform-types scripts/sdk-typescript-integration.ts
(cd interop/dtm-labs-go && go run .)
cargo run --example grpc_smoke
cargo run --example grpc_callback_smoke
