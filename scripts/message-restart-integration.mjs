#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const targetDir = resolve(root, process.env.CARGO_TARGET_DIR ?? "target");
mkdirSync(targetDir, { recursive: true });
const runDir = await mkdtemp(join(targetDir, "message-restart-"));
const databasePath = join(runDir, "coordinator.sqlite");
const configPath = join(runDir, "config.yaml");
const serviceLogPath = join(runDir, "service.log");
const serviceBinary = join(
  targetDir,
  "debug",
  process.platform === "win32" ? "roze-dtm-service.exe" : "roze-dtm-service",
);
const baseUrl = new URL("http://127.0.0.1:18100");
const branchPort = 18101;
const branchOrigin = `http://127.0.0.1:${branchPort}`;
const token = "roze-dtm-restart-token-32-bytes";
const revision = process.env.GITHUB_SHA
  ?? process.env.ROZE_DTM_RELEASE_REVISION
  ?? readGitRevision();
const gid = `restart-message-${Date.now()}-${process.pid}`;
let branchCalls = 0;
let service;
let logDescriptor;

assert(existsSync(serviceBinary), `service binary is missing: ${serviceBinary}`);
writeFileSync(databasePath, "", { mode: 0o600 });
writeFileSync(configPath, serviceConfig(), { mode: 0o600 });

const branchServer = createServer(async (request, response) => {
  for await (const _chunk of request) {
    // Drain the request body before responding.
  }
  branchCalls += 1;
  response.writeHead(200, { "content-type": "application/json" });
  response.end('{"dtm_result":"SUCCESS"}');
});

try {
  await listen(branchServer, branchPort);
  service = await startService("restart-worker-before");

  await nativePost("/v1/messages", {
    gid,
    branches: [{
      id: "publish",
      action: `${branchOrigin}/delayed-message/publish`,
      payload: { source: "restart-integration" },
    }],
    options: { delay_millis: 4_000 },
  });
  await nativePost(`/v1/messages/${encodeURIComponent(gid)}/prepare`);
  const scheduled = await nativePost(`/v1/messages/${encodeURIComponent(gid)}/dispatch`);
  assert(String(scheduled.status).toLowerCase() === "succeeding", "delayed Message decision was not persisted");
  assert(scheduled.branches?.[0]?.attempts === 0, "delayed Message ran before its delivery point");

  await delay(250);
  assert(branchCalls === 0, "delayed Message branch ran before coordinator termination");
  await stopService(service);
  service = undefined;

  // Cross both the delivery point and the old recovery lease expiry while the
  // coordinator is down, then recover with a deployment-distinct worker.
  await delay(4_250);
  service = await startService("restart-worker-after");
  const recovered = await waitForStatus(gid, "succeeded", 10_000);
  assert(recovered.branches?.[0]?.attempts === 1, "recovered Message did not persist one branch attempt");
  assert(branchCalls === 1, `recovered Message produced ${branchCalls} branch calls instead of one`);

  await delay(750);
  const stable = await nativeTransaction(gid);
  assert(String(stable.status).toLowerCase() === "succeeded", "recovered Message terminal state was not stable");
  assert(branchCalls === 1, "recovery replayed a terminal Message branch");

  console.log(JSON.stringify({
    verdict: "pass",
    scenario: "delayed-message-restart",
    storage: "sqlite-file",
    forced_terminations: 1,
    worker_ids: 2,
    branch_calls: branchCalls,
  }));
} catch (error) {
  console.error(error);
  if (existsSync(serviceLogPath)) {
    console.error("roze-dtm restart integration service log follows:");
    console.error(readFileSync(serviceLogPath, "utf8"));
  }
  process.exitCode = 1;
} finally {
  if (service) await stopService(service).catch(() => {});
  await close(branchServer).catch(() => {});
  if (logDescriptor !== undefined) closeSync(logDescriptor);
  await rm(runDir, { recursive: true, force: true });
}

function serviceConfig() {
  const relativeDatabase = relative(root, databasePath).replaceAll("\\", "/");
  return `name: roze-dtm-restart-integration
profile: development

logging:
  enabled: true
  level: info
  format: json
  stdout: true
  ansi: false
  target: true
  span_events: none
  utc_time: true
  non_blocking_buffer: 1024
  lossy: false

rest:
  addr: 127.0.0.1:18100
  register: false

governance: {}

application:
  dtm:
    control_token: env://ROZE_DTM_CONTROL_TOKEN
    release_revision: env://ROZE_DTM_RELEASE_REVISION
    recover_interval_ms: 250
    recovery_lease_ttl_ms: 1000
    data_expire_seconds: 604800
    finished_data_expire_seconds: 86400
    retention_interval_ms: 60000
    retention_batch_size: 64
    worker_id: env://ROZE_DTM_WORKER_ID
    allowed_branch_origins:
      - ${branchOrigin}
    store:
      kind: sqlite
      database_url: "sqlite://${relativeDatabase}"
      max_connections: 1
    max_attempts: 3
    retry_backoff_ms: 100
    max_retry_backoff_ms: 1000
    branch_call_timeout_ms: 1000
    transaction_timeout_ms: 15000
    alert_retry_limit: 2
    alert_webhook_timeout_ms: 1000
`;
}

async function startService(workerId) {
  if (logDescriptor === undefined) {
    logDescriptor = openSync(serviceLogPath, "a", 0o600);
  }
  const child = spawn(serviceBinary, [], {
    cwd: root,
    env: {
      ...process.env,
      ROZE_CONFIG_PATH: configPath,
      ROZE_DTM_CONTROL_TOKEN: token,
      ROZE_DTM_RELEASE_REVISION: revision,
      ROZE_DTM_WORKER_ID: workerId,
    },
    stdio: ["ignore", logDescriptor, logDescriptor],
    windowsHide: true,
  });
  await waitForReady(child, 15_000);
  return child;
}

async function stopService(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolveExit) => child.once("exit", resolveExit));
  child.kill("SIGKILL");
  await Promise.race([
    exited,
    delay(5_000).then(() => { throw new Error(`service process ${child.pid} did not terminate`); }),
  ]);
}

async function waitForReady(child, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    assert(child.exitCode === null && child.signalCode === null, "service exited before readiness");
    try {
      const response = await fetch(new URL("/readyz", baseUrl), {
        redirect: "error",
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return;
    } catch {
      // Startup connection failures are expected until the listener binds.
    }
    await delay(100);
  }
  throw new Error("service did not become ready");
}

async function waitForStatus(transactionGid, expected, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const transaction = await nativeTransaction(transactionGid);
    if (String(transaction.status).toLowerCase() === expected) return transaction;
    await delay(100);
  }
  throw new Error(`transaction ${transactionGid} did not reach ${expected}`);
}

async function nativeTransaction(transactionGid) {
  return nativeRequest(`/v1/transactions/${encodeURIComponent(transactionGid)}`);
}

async function nativePost(path, body) {
  return nativeRequest(path, {
    method: "POST",
    headers: body === undefined ? {} : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function nativeRequest(path, init = {}) {
  const response = await fetch(new URL(path, baseUrl), {
    ...init,
    headers: {
      accept: "application/json",
      authorization: `Bearer ${token}`,
      ...(init.headers ?? {}),
    },
    redirect: "error",
    signal: AbortSignal.timeout(5_000),
  });
  const payload = await response.json();
  assert(response.ok && payload.code === 0, `${path} failed with HTTP ${response.status}`);
  return payload.data;
}

function listen(server, port) {
  return new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolveListen);
  });
}

function close(server) {
  return new Promise((resolveClose, reject) => {
    server.close((error) => error ? reject(error) : resolveClose());
  });
}

function delay(milliseconds) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function readGitRevision() {
  const dotGit = join(root, ".git");
  const gitDirectory = statSync(dotGit).isDirectory()
    ? dotGit
    : resolve(root, readFileSync(dotGit, "utf8").trim().replace(/^gitdir:\s*/u, ""));
  const head = readFileSync(join(gitDirectory, "HEAD"), "utf8").trim();
  if (!head.startsWith("ref: ")) return head;
  const ref = head.slice(5);
  const looseRef = join(gitDirectory, ...ref.split("/"));
  if (existsSync(looseRef)) return readFileSync(looseRef, "utf8").trim();
  const packedRefs = readFileSync(join(gitDirectory, "packed-refs"), "utf8");
  const match = packedRefs
    .split(/\r?\n/u)
    .find((line) => line.endsWith(` ${ref}`));
  assert(match, `Git revision is unavailable for ${ref}`);
  return match.split(" ", 1)[0];
}
