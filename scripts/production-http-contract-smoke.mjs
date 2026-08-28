#!/usr/bin/env node
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const started = new Date();
const startedMonotonic = performance.now();
const baseUrl = requiredUrl("ROZE_DTM_BASE_URL");
const token = required("ROZE_DTM_CONTROL_TOKEN");
const revision = required("ROZE_DTM_EXPECTED_REVISION");
if (!/^[0-9a-f]{40}$/i.test(revision)) throw new Error("ROZE_DTM_EXPECTED_REVISION must be a full 40-character Git revision");
const evidenceDir = resolve(required("ROZE_DTM_EVIDENCE_DIR"));
const timeoutMs = boundedInteger(process.env.ROZE_DTM_SMOKE_TIMEOUT_MS ?? "5000", 100, 60000, "ROZE_DTM_SMOKE_TIMEOUT_MS");
const topology = parseTopology(required("ROZE_DTM_TOPOLOGY_JSON"));
await mkdir(evidenceDir, { recursive: true });

const checks = [];
let openapiText = "";
let metricsText = "";

async function check(name, work) {
  const begin = performance.now();
  try {
    const detail = await work();
    checks.push({ name, outcome: "pass", elapsed_ms: Math.round(performance.now() - begin), detail });
  } catch (error) {
    checks.push({ name, outcome: "fail", elapsed_ms: Math.round(performance.now() - begin), detail: publicError(error) });
  }
}

for (const path of ["/healthz", "/startupz", "/readyz"]) {
  await check(`probe:${path}`, async () => {
    const response = await request(path);
    const payload = await json(response);
    assert(response.status === 200, `${path} returned HTTP ${response.status}`);
    assert(payload?.code === 0, `${path} returned non-zero Roze code`);
    return `HTTP ${response.status}`;
  });
}

await check("operations:metrics", async () => {
  const response = await request("/metrics");
  metricsText = await response.text();
  assert(response.status === 200, `/metrics returned HTTP ${response.status}`);
  assert(!metricsText.includes(token), "metrics response exposed the control token");
  return `${metricsText.length} bytes`;
});

await check("contract:openapi", async () => {
  const response = await request("/openapi.json");
  openapiText = await response.text();
  const document = JSON.parse(openapiText);
  assert(response.status === 200, `/openapi.json returned HTTP ${response.status}`);
  assert(document.openapi === "3.1.0", "unexpected OpenAPI version");
  assert(Object.keys(document.paths ?? {}).length === 54, "OpenAPI path count is not 54");
  assert(document.paths["/api/json-rpc"], "OpenAPI omits JSON-RPC");
  assert(document.paths["/v1/dashboard"], "OpenAPI omits Dashboard API");
  assert(document.components?.schemas?.DashboardTransactionRow?.properties?.available_actions?.items?.enum?.join(",") === "reset-retry,force-stop", "OpenAPI omits Dashboard management action contract");
  return "OpenAPI 3.1, 54 paths";
});

await check("security:unauthorized-native", async () => {
  const response = await request("/v1/stats");
  assert(response.status === 401, `unauthorized /v1/stats returned HTTP ${response.status}`);
  return "HTTP 401";
});

await check("native:stats", async () => {
  const { response, payload } = await authorizedJson("/v1/stats");
  assert(response.status === 200 && payload?.code === 0, "authorized stats request failed");
  assert(Number.isInteger(payload.data?.total), "stats total is not an integer");
  return `total=${payload.data.total}`;
});

await check("management:dashboard-redaction", async () => {
  const { response, payload } = await authorizedJson("/v1/dashboard?offset=0&limit=10");
  assert(response.status === 200 && payload?.code === 0, "authorized Dashboard request failed");
  const forbidden = new Set(["action", "compensate", "confirm", "cancel", "payload", "metadata", "branch_headers", "workflow_progresses", "last_error"]);
  const leaked = findForbiddenKeys(payload.data, forbidden);
  assert(leaked.length === 0, `Dashboard exposed forbidden keys: ${leaked.join(", ")}`);
  assert(JSON.stringify(payload).indexOf(token) === -1, "Dashboard response exposed the control token");
  const rows = payload.data?.transactions?.items;
  assert(Array.isArray(rows), "Dashboard transaction rows are missing");
  for (const row of rows) {
    assert(Array.isArray(row.available_actions), "Dashboard row omits available_actions");
    assert(row.available_actions.every((action) => action === "reset-retry" || action === "force-stop"), "Dashboard row exposes an unknown management action");
    assert(!row.terminal || row.available_actions.length === 0, "terminal Dashboard row exposes management actions");
    assert(row.terminal || row.available_actions.includes("force-stop"), "non-terminal Dashboard row omits force-stop");
  }
  return `rows=${payload.data?.transactions?.items?.length ?? 0}`;
});

await check("management:xa-reconciliation", async () => {
  const { response, payload } = await authorizedJson("/v1/xa/reconciliation");
  assert(response.status === 200 && payload?.code === 0, "XA reconciliation request failed");
  assert(Array.isArray(payload.data?.items), "XA reconciliation items are missing");
  return `items=${payload.data.items.length}`;
});

await check("compatibility:version-revision", async () => {
  const response = await request("/api/dtmsvr/version");
  const payload = await json(response);
  assert(response.status === 200, `version endpoint returned HTTP ${response.status}`);
  assert(typeof payload?.version === "string" && payload.version.length > 0, "service version is missing");
  assert(payload.release_revision === revision.toLowerCase(), `deployed revision ${payload.release_revision ?? "missing"} does not match expected revision`);
  return `revision=${payload.release_revision}`;
});

await check("compatibility:http", async () => {
  const response = await request("/api/dtmsvr/newGid", true);
  const payload = await json(response);
  assert(response.status === 200 && payload?.dtm_result === "SUCCESS", "compatibility newGid failed");
  assert(typeof payload.gid === "string" && payload.gid.length > 0, "compatibility GID is missing");
  return "dtm_result=SUCCESS";
});

await check("compatibility:json-rpc", async () => {
  const response = await request("/api/json-rpc", true, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: "smoke", method: "newGid", params: {} }),
  });
  const payload = await json(response);
  assert(response.status === 200 && payload?.jsonrpc === "2.0", "JSON-RPC transport failed");
  assert(payload.id === "smoke" && typeof payload.result?.gid === "string", "JSON-RPC newGid result is invalid");
  return "jsonrpc=2.0";
});

const verdict = checks.every((item) => item.outcome === "pass") ? "pass" : "fail";
const artifacts = [];
if (openapiText) artifacts.push(await artifact("openapi.json", openapiText));
if (metricsText) artifacts.push(await artifact("metrics.txt", metricsText));
const finished = new Date();
const report = {
  schema_version: 1,
  area: "http-contract",
  verdict,
  revision: revision.toLowerCase(),
  started_at: started.toISOString(),
  finished_at: finished.toISOString(),
  duration_ms: Math.round(performance.now() - startedMonotonic),
  command: "node scripts/production-http-contract-smoke.mjs",
  base_origin: baseUrl.origin,
  topology,
  checks,
  artifacts,
};
await writeFile(resolve(evidenceDir, "http-contract-report.json"), JSON.stringify(report, null, 2) + "\n", { encoding: "utf8", mode: 0o600 });
console.log(JSON.stringify({ verdict, checks: checks.length, failed: checks.filter((item) => item.outcome === "fail").map((item) => item.name), evidence_dir: evidenceDir }));
process.exitCode = verdict === "pass" ? 0 : 1;

async function authorizedJson(path) {
  const response = await request(path, true);
  return { response, payload: await json(response) };
}

async function request(path, authorized = false, init = {}) {
  const headers = { accept: "application/json", ...(init.headers ?? {}) };
  if (authorized) headers.authorization = `Bearer ${token}`;
  return fetch(new URL(path, baseUrl), { ...init, headers, redirect: "error", signal: AbortSignal.timeout(timeoutMs) });
}

async function json(response) {
  const text = await response.text();
  try { return JSON.parse(text); } catch { throw new Error(`expected JSON from HTTP ${response.status}`); }
}

async function artifact(name, content) {
  const path = resolve(evidenceDir, name);
  await writeFile(path, content, { encoding: "utf8", mode: 0o600 });
  const digest = createHash("sha256").update(await readFile(path)).digest("hex");
  return { path: name, sha256: digest, bytes: Buffer.byteLength(content) };
}

function findForbiddenKeys(value, forbidden, path = "data") {
  if (!value || typeof value !== "object") return [];
  const found = [];
  for (const [key, child] of Object.entries(value)) {
    const childPath = `${path}.${key}`;
    if (forbidden.has(key)) found.push(childPath);
    found.push(...findForbiddenKeys(child, forbidden, childPath));
  }
  return found;
}

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function requiredUrl(name) {
  const url = new URL(required(name));
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error(`${name} must be an HTTP(S) URL without credentials, query, or fragment`);
  return new URL(url.toString().replace(/\/$/, "") + "/");
}

function boundedInteger(value, minimum, maximum, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  return parsed;
}

function parseTopology(value) {
  const parsed = JSON.parse(value);
  assert(parsed && typeof parsed === "object" && !Array.isArray(parsed), "ROZE_DTM_TOPOLOGY_JSON must be an object");
  assert(typeof parsed.store === "string" && parsed.store.length > 0, "topology.store is required");
  assert(Number.isInteger(parsed.replica_count) && parsed.replica_count > 0, "topology.replica_count must be positive");
  assert(Array.isArray(parsed.dependencies), "topology.dependencies must be an array");
  return parsed;
}

function assert(condition, message) { if (!condition) throw new Error(message); }
function publicError(error) { return error instanceof Error ? error.message.slice(0, 512) : "unknown failure"; }
