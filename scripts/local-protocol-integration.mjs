#!/usr/bin/env node
import { createServer } from "node:http";

const baseUrl = new URL(process.env.ROZE_DTM_BASE_URL ?? "http://127.0.0.1:18090");
const token = required("ROZE_DTM_CONTROL_TOKEN");
const branchPort = Number(process.env.ROZE_DTM_BRANCH_PORT ?? "18091");
const branchOrigin = `http://127.0.0.1:${branchPort}`;
const suffix = `${Date.now()}-${process.pid}`;
const branchCalls = [];
const branchAttempts = new Map();
const server = createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  branchCalls.push({
    method: request.method,
    url: request.url,
    body: Buffer.concat(chunks).toString("utf8"),
  });
  const attempts = (branchAttempts.get(request.url) ?? 0) + 1;
  branchAttempts.set(request.url, attempts);
  if (request.url?.startsWith("/message/retry-once") && attempts === 1) {
    response.writeHead(503, { "content-type": "application/json" });
    response.end('{"dtm_result":"FAILURE"}');
    return;
  }
  response.writeHead(200, { "content-type": "application/json" });
  response.end('{"dtm_result":"SUCCESS"}');
});

await listen(server, branchPort);
try {
  const gids = {
    tcc: `smoke-tcc-${suffix}`,
    saga: `smoke-saga-${suffix}`,
    workflow: `smoke-workflow-${suffix}`,
    message: `smoke-message-${suffix}`,
    xa: `smoke-xa-${suffix}`,
    jsonRpc: `smoke-jsonrpc-${suffix}`,
    retry: `smoke-retry-${suffix}`,
    forceStop: `smoke-force-${suffix}`,
  };

  await compatibility("/api/dtmsvr/prepare", {
    gid: gids.tcc,
    trans_type: "tcc",
    wait_result: true,
  });
  await compatibility("/api/dtmsvr/registerTccBranch", {
    gid: gids.tcc,
    trans_type: "tcc",
    branch_id: "inventory",
    confirm: `${branchOrigin}/tcc/confirm`,
    cancel: `${branchOrigin}/tcc/cancel`,
    data: JSON.stringify({ sku: "A" }),
  });
  await compatibility("/api/dtmsvr/submit", {
    gid: gids.tcc,
    trans_type: "tcc",
    wait_result: true,
  });

  await compatibility("/api/dtmsvr/submit", transactionRequest(
    gids.saga,
    "saga",
    [{ action: `${branchOrigin}/saga/action`, compensate: `${branchOrigin}/saga/compensate` }],
  ));

  await compatibility("/api/dtmsvr/submit", transactionRequest(
    gids.workflow,
    "workflow",
    [
      { action: `${branchOrigin}/workflow/reserve`, compensate: `${branchOrigin}/workflow/release` },
      { action: `${branchOrigin}/workflow/charge`, compensate: `${branchOrigin}/workflow/refund` },
    ],
  ));

  const message = transactionRequest(
    gids.message,
    "msg",
    [{ action: `${branchOrigin}/message/publish` }],
  );
  await compatibility("/api/dtmsvr/prepare", message);
  await compatibility("/api/dtmsvr/submit", message);

  await compatibility("/api/dtmsvr/prepare", {
    gid: gids.xa,
    trans_type: "xa",
    wait_result: true,
  });
  await compatibility("/api/dtmsvr/registerXaBranch", {
    gid: gids.xa,
    trans_type: "xa",
    branch_id: "account",
    url: `${branchOrigin}/xa/phase2`,
  });
  await compatibility("/api/dtmsvr/submit", {
    gid: gids.xa,
    trans_type: "xa",
    wait_result: true,
  });

  await jsonRpc("submit", transactionRequest(
    gids.jsonRpc,
    "saga",
    [{ action: `${branchOrigin}/json-rpc/action`, compensate: `${branchOrigin}/json-rpc/compensate` }],
  ));

  const retryMessage = transactionRequest(
    gids.retry,
    "msg",
    [{ action: `${branchOrigin}/message/retry-once` }],
  );
  retryMessage.retry_interval = 1;
  await compatibility("/api/dtmsvr/prepare", retryMessage);
  await compatibility("/api/dtmsvr/submit", retryMessage);
  const failedAttempt = await nativeTransaction(gids.retry);
  assert(
    String(failedAttempt.status).toLowerCase() === "succeeding"
      && String(failedAttempt.branches?.[0]?.status).toLowerCase() === "failed",
    "transient Message failure was not persisted for recovery",
  );
  await waitForStatus(gids.retry, "succeeded", 5000);

  await compatibility("/api/dtmsvr/prepare", {
    gid: gids.forceStop,
    trans_type: "msg",
    wait_result: true,
  });
  await authorized(`/v1/transactions/${encodeURIComponent(gids.forceStop)}/force-stop`, {
    method: "POST",
  });

  for (const [kind, gid] of Object.entries(gids)) {
    const transaction = await nativeTransaction(gid);
    const status = String(transaction.status).toLowerCase();
    if (kind === "forceStop") {
      assert(status === "failed", `force-stop ended as ${transaction.status}`);
    } else {
      assert(status === "succeeded", `${kind} ended as ${transaction.status}`);
    }
  }

  const dashboard = await authorizedJson("/v1/dashboard?offset=0&limit=50");
  const auditEvents = dashboard.data?.audit?.items ?? [];
  assert(auditEvents.some((item) => item.event === "dtm.compat.http.submit"), "HTTP compatibility audit is missing");
  assert(auditEvents.some((item) => item.event === "dtm.compat.json_rpc.submit"), "JSON-RPC audit is missing");
  assert(auditEvents.some((item) => item.event === "dtm.transaction.force_stop"), "management audit is missing");

  const metrics = await text("/metrics");
  assert(metrics.includes("roze_dtm_transaction_transitions_total"), "transaction metrics are missing");
  assert(metrics.includes('operation="dtm.compat.http.submit"'), "HTTP compatibility metric is missing");
  assert(metrics.includes('operation="dtm.compat.json_rpc.submit"'), "JSON-RPC metric is missing");
  assert(metrics.includes("roze_dtm_retry_scheduled_observations_total"), "retry metric is missing");
  assert(!metrics.includes(token), "metrics leaked the control token");

  for (const expected of [
    "/tcc/confirm",
    "/saga/action",
    "/workflow/reserve",
    "/workflow/charge",
    "/message/publish",
    "/xa/phase2",
    "/json-rpc/action",
    "/message/retry-once",
  ]) {
    assert(branchCalls.some((call) => call.url?.startsWith(expected)), `branch call ${expected} is missing`);
  }

  console.log(JSON.stringify({
    verdict: "pass",
    transactions: Object.keys(gids).length,
    branch_calls: branchCalls.length,
    protocols: ["http", "json-rpc"],
    modes: ["tcc", "saga", "workflow", "message", "xa"],
  }));
} finally {
  await close(server);
}

function transactionRequest(gid, transType, steps) {
  return {
    gid,
    trans_type: transType,
    steps,
    payloads: steps.map((_, index) => ({ index })),
    wait_result: true,
  };
}

async function compatibility(path, body) {
  const response = await authorized(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  assert(response.ok && payload.dtm_result === "SUCCESS", `${path} failed: HTTP ${response.status}`);
  return payload;
}

async function jsonRpc(method, params) {
  const response = await authorized("/api/json-rpc", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: `${method}-${suffix}`, method, params }),
  });
  const payload = await response.json();
  assert(response.ok && payload.jsonrpc === "2.0" && !payload.error, `JSON-RPC ${method} failed`);
  return payload.result;
}

async function nativeTransaction(gid) {
  const payload = await authorizedJson(`/v1/transactions/${encodeURIComponent(gid)}`);
  assert(payload.code === 0 && payload.data?.gid === gid, `native query failed for ${gid}`);
  return payload.data;
}

async function waitForStatus(gid, expected, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const transaction = await nativeTransaction(gid);
    if (String(transaction.status).toLowerCase() === expected) return transaction;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`transaction ${gid} did not reach ${expected}`);
}

async function authorizedJson(path) {
  const response = await authorized(path);
  const payload = await response.json();
  assert(response.ok, `${path} returned HTTP ${response.status}`);
  return payload;
}

async function text(path) {
  const response = await fetch(new URL(path, baseUrl), { signal: AbortSignal.timeout(5000) });
  assert(response.ok, `${path} returned HTTP ${response.status}`);
  return response.text();
}

function authorized(path, init = {}) {
  return fetch(new URL(path, baseUrl), {
    ...init,
    headers: {
      accept: "application/json",
      authorization: `Bearer ${token}`,
      ...(init.headers ?? {}),
    },
    redirect: "error",
    signal: AbortSignal.timeout(5000),
  });
}

function listen(httpServer, port) {
  return new Promise((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(port, "127.0.0.1", resolve);
  });
}

function close(httpServer) {
  return new Promise((resolve, reject) => httpServer.close((error) => error ? reject(error) : resolve()));
}

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}
