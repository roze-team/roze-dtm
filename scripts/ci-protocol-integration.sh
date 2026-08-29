#!/usr/bin/env bash
set -euo pipefail

umask 077
tls_dir="$(mktemp -d)"
service_pid=""
service_log="${RUNNER_TEMP:-/tmp}/roze-dtm-protocol-service.log"

cleanup() {
  if [[ -n "$service_pid" ]]; then
    kill "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
  fi
  rm -rf -- "$tls_dir"
}
failure() {
  status=$?
  printf 'Protocol integration failed; service log follows:\n' >&2
  if [[ -f "$service_log" ]]; then
    tail -n 200 "$service_log" >&2 || true
  fi
  exit "$status"
}
trap cleanup EXIT
trap failure ERR

if command -v cygpath >/dev/null 2>&1; then
  export MSYS2_ARG_CONV_EXCL="/CN="
fi
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 \
  -keyout "$tls_dir/ca.key" -out "$tls_dir/ca.pem" \
  -subj "/CN=Roze DTM protocol test CA" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "$tls_dir/server.key" -out "$tls_dir/server.csr" \
  -subj "/CN=localhost" >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' >"$tls_dir/server.ext"
openssl x509 -req -sha256 -days 1 \
  -in "$tls_dir/server.csr" -CA "$tls_dir/ca.pem" -CAkey "$tls_dir/ca.key" \
  -CAcreateserial -extfile "$tls_dir/server.ext" -out "$tls_dir/server.pem" \
  >/dev/null 2>&1

tls_ca_file="$tls_dir/ca.pem"
tls_cert_file="$tls_dir/server.pem"
tls_key_file="$tls_dir/server.key"
if command -v cygpath >/dev/null 2>&1; then
  tls_ca_file="$(cygpath -w "$tls_ca_file")"
  tls_cert_file="$(cygpath -w "$tls_cert_file")"
  tls_key_file="$(cygpath -w "$tls_key_file")"
fi

export ROZE_CONFIG_PATH="${ROZE_CONFIG_PATH:-service/config.sqlite.tls-smoke.yaml}"
export ROZE_DTM_CONTROL_TOKEN="${ROZE_DTM_CONTROL_TOKEN:-roze-dtm-smoke-token-32-bytes!!}"
export ROZE_DTM_RELEASE_REVISION="${ROZE_DTM_RELEASE_REVISION:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
export ROZE_DTM_EXPECTED_REVISION="$ROZE_DTM_RELEASE_REVISION"
export ROZE_DTM_BASE_URL="${ROZE_DTM_BASE_URL:-http://127.0.0.1:18090}"
export ROZE_DTM_GRPC_ENDPOINT="${ROZE_DTM_GRPC_ENDPOINT:-http://127.0.0.1:36791}"
export ROZE_DTM_BRANCH_TLS_CA_FILE="$tls_ca_file"
export ROZE_DTM_TEST_TLS_CERT_FILE="$tls_cert_file"
export ROZE_DTM_TEST_TLS_KEY_FILE="$tls_key_file"

cargo run -p roze-dtm-service >"$service_log" 2>&1 &
service_pid=$!

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
