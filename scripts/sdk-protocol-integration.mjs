#!/usr/bin/env node
import { createServer } from "node:http";
import { RozeDtmApiError, RozeDtmClient } from "../sdk/roze-dtm.js";
import { RozeDtmCompatClient } from "../sdk/roze-dtm-compat.js";

const baseUrl = process.env.ROZE_DTM_BASE_URL ?? "http://127.0.0.1:18090";
const token = required("ROZE_DTM_CONTROL_TOKEN");
const expectedRevision = required("ROZE_DTM_EXPECTED_REVISION");
const branchPort = Number(process.env.ROZE_DTM_BRANCH_PORT ?? "18091");
const branchOrigin = `http://127.0.0.1:${branchPort}`;
const suffix = `${Date.now()}-${process.pid}`;
const native = new RozeDtmClient(baseUrl, token);
const compat = new RozeDtmCompatClient(baseUrl, token);
const calls = [];
const attempts = new Map();

const server = createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const body = Buffer.concat(chunks);
  const url = new URL(request.url ?? "/", branchOrigin);
  const attempt = (attempts.get(url.pathname) ?? 0) + 1;
  attempts.set(url.pathname, attempt);
  calls.push({ method: request.method, url, body: body.toString("utf8"), attempt });

  if (url.pathname === "/callback/http-fail") {
    assertCallbackQuery(url, "sdk-http-fail");
    assert(body.toString("utf8") === '{"source":"javascript-sdk"}', "HTTP callback body changed");
    if (attempt === 1) return reply(response, 425, "ongoing");
    return reply(response, 409, "sdk HTTP business failure");
  }

  if (url.pathname === "/callback/json-fail") {
    const payload = JSON.parse(body.toString("utf8"));
    assert(payload.jsonrpc === "2.0" && payload.method === "sdk-json-fail", "JSON-RPC callback envelope changed");
    assert(payload.params?.trans_type === "workflow" && payload.params?.branch_id === "00", "JSON-RPC callback reserved params changed");
    assert(payload.params?.source === "javascript-sdk", "JSON-RPC callback custom data changed");
    const code = attempt === 1 ? -32902 : -32901;
    return json(response, { jsonrpc: "2.0", id: payload.id, error: { code, message: code === -32902 ? "ongoing" : "sdk JSON-RPC business failure" } });
  }

  if (url.pathname === "/callback/terminal-race") {
    assertCallbackQuery(url, "sdk-terminal-race");
    await compat.submit({
      gid: url.searchParams.get("gid"),
      trans_type: "workflow",
      req_extra: { status: "succeed", result: Buffer.from("sdk callback result").toString("base64") },
    });
    return reply(response, 200, "completed");
  }

  return json(response, { dtm_result: "SUCCESS" });
});

await listen(server, branchPort);
try {
  await assertUnauthorized();
  const version = await compat.version();
  assert(version.release_revision === expectedRevision, `release revision mismatch: ${version.release_revision}`);
  const generated = await compat.newGid();
  assert(typeof generated.gid === "string" && generated.gid.length > 0, "compat newGid returned no gid");
  const rpcGenerated = await compat.rpcNewGid();
  assert(typeof rpcGenerated.gid === "string" && rpcGenerated.gid.length > 0, "JSON-RPC newGid returned no gid");

  const nativeGids = await exerciseNativeModes();
  const compatGid = await exerciseCompatTcc();
  const callbackGids = await exerciseCallbacks();

  const listed = await compat.all({ gid: compatGid, limit: 10 });
  assert(listed.transactions.some((transaction) => transaction.gid === compatGid), "compat all omitted SDK transaction");
  const stats = await native.stats();
  assert(stats.total >= Object.keys(nativeGids).length + 4, "native stats omitted SDK transactions");

  for (const path of ["/native/tcc/try", "/native/saga/action", "/native/workflow/action", "/native/message/action", "/native/xa/action", "/compat/tcc/confirm"]) {
    assert(calls.some((call) => call.url.pathname === path), `branch call ${path} is missing`);
  }
  assert(attempts.get("/callback/http-fail") >= 2, "HTTP callback was not retried");
  assert(attempts.get("/callback/json-fail") >= 2, "JSON-RPC callback was not retried");

  console.log(JSON.stringify({
    verdict: "pass",
    clients: ["javascript-native", "javascript-dtm-compat"],
    modes: Object.keys(nativeGids),
    callbacks: callbackGids,
    branch_calls: calls.length,
  }));
} finally {
  await close(server);
}

async function exerciseNativeModes() {
  const gids = {
    tcc: `sdk-native-tcc-${suffix}`,
    saga: `sdk-native-saga-${suffix}`,
    workflow: `sdk-native-workflow-${suffix}`,
    message: `sdk-native-message-${suffix}`,
    xa: `sdk-native-xa-${suffix}`,
  };
  await native.submitTcc({ gid: gids.tcc, branches: [{ id: "inventory", action: `${branchOrigin}/native/tcc/try`, confirm: `${branchOrigin}/native/tcc/confirm`, cancel: `${branchOrigin}/native/tcc/cancel`, payload: { sku: "SDK" } }] });
  await native.prepareTcc(gids.tcc);
  await native.confirmTcc(gids.tcc);

  await native.submitSaga({ gid: gids.saga, branches: [{ id: "reserve", action: `${branchOrigin}/native/saga/action`, compensate: `${branchOrigin}/native/saga/compensate`, payload: { source: "sdk" } }] });
  await native.startSaga(gids.saga);

  await native.submitWorkflow({ gid: gids.workflow, branches: [{ id: "work", action: `${branchOrigin}/native/workflow/action`, compensate: `${branchOrigin}/native/workflow/compensate` }] });
  await native.startWorkflow(gids.workflow);

  await native.submitMessage({ gid: gids.message, branches: [{ id: "publish", action: `${branchOrigin}/native/message/action`, payload: { event: "sdk" } }] });
  await native.prepareMessage(gids.message);
  await native.dispatchMessage(gids.message);

  await native.submitXa({ gid: gids.xa, branches: [{ id: "phase-two", action: `${branchOrigin}/native/xa/action`, cancel: `${branchOrigin}/native/xa/rollback`, payload: { source: "sdk" } }] });
  await native.prepareXa(gids.xa);
  await native.commitXa(gids.xa);

  for (const gid of Object.values(gids)) await waitForStatus(gid, "succeeded");
  return gids;
}

async function exerciseCompatTcc() {
  const gid = `sdk-compat-tcc-${suffix}`;
  await compat.prepare({ gid, trans_type: "tcc", wait_result: true });
  await compat.registerTccBranch({
    gid,
    trans_type: "tcc",
    branch_id: "inventory",
    confirm: `${branchOrigin}/compat/tcc/confirm`,
    cancel: `${branchOrigin}/compat/tcc/cancel`,
    data: JSON.stringify({ source: "sdk-compat" }),
  });
  await compat.submit({ gid, trans_type: "tcc", wait_result: true });
  const queried = await compat.query(gid);
  assert(queried.transaction.gid === gid && queried.transaction.status === "succeed", "compat TCC did not succeed");
  return gid;
}

async function exerciseCallbacks() {
  const gids = {
    httpFailed: `sdk-callback-http-${suffix}`,
    jsonFailed: `sdk-callback-json-${suffix}`,
    terminalRace: `sdk-callback-race-${suffix}`,
  };
  const common = { trans_type: "workflow", retry_interval: 1, request_timeout: 2, retry_limit: 3 };
  await compat.prepareWorkflow({
    ...common,
    gid: gids.httpFailed,
    query_prepared: `${branchOrigin}/callback/http-fail`,
    custom_data: callbackData("sdk-http-fail", Buffer.from('{"source":"javascript-sdk"}')),
  });
  await compat.rpcPrepare({
    ...common,
    gid: gids.jsonFailed,
    query_prepared: `${branchOrigin}/callback/json-fail?method=sdk-json-fail`,
    custom_data: callbackData("sdk-json-fail", Buffer.from('{"source":"javascript-sdk"}')),
  });
  await compat.prepareWorkflow({
    ...common,
    gid: gids.terminalRace,
    query_prepared: `${branchOrigin}/callback/terminal-race`,
    custom_data: callbackData("sdk-terminal-race", Buffer.alloc(0)),
  });

  const httpFailed = await waitForStatus(gids.httpFailed, "failed", 10_000);
  const jsonFailed = await waitForStatus(gids.jsonFailed, "failed", 10_000);
  const raced = await waitForStatus(gids.terminalRace, "succeeded", 10_000);
  assert(httpFailed.metadata?.rollback_reason === "sdk HTTP business failure", "HTTP callback failure reason was not persisted");
  assert(jsonFailed.metadata?.rollback_reason === "sdk JSON-RPC business failure", "JSON-RPC callback failure reason was not persisted");
  assert(raced.metadata?.["dtm.workflow.result"] === Buffer.from("sdk callback result").toString("base64"), "terminal callback result was not persisted");
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert((await native.getTransaction(gids.terminalRace)).status === "Succeeded", "stale callback recovery overwrote terminal state");
  return gids;
}

async function assertUnauthorized() {
  try {
    await new RozeDtmClient(baseUrl).stats();
    throw new Error("unauthorized SDK request unexpectedly succeeded");
  } catch (error) {
    assert(error instanceof RozeDtmApiError && error.status === 401, `unexpected unauthorized error: ${error}`);
  }
}

async function waitForStatus(gid, expected, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const transaction = await native.getTransaction(gid);
    if (String(transaction.status).toLowerCase() === expected) return transaction;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`transaction ${gid} did not reach ${expected}`);
}

function callbackData(name, data) {
  return JSON.stringify({ name, data: data.toString("base64") });
}

function assertCallbackQuery(url, operation) {
  assert(url.searchParams.get("gid"), "HTTP callback gid is missing");
  assert(url.searchParams.get("trans_type") === "workflow", "HTTP callback trans_type changed");
  assert(url.searchParams.get("branch_id") === "00", "HTTP callback branch_id changed");
  assert(url.searchParams.get("op") === operation, "HTTP callback operation changed");
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function reply(response, status, body) {
  response.writeHead(status, { "content-type": "text/plain; charset=utf-8" });
  response.end(body);
}

function json(response, body) {
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

function listen(target, port) {
  return new Promise((resolve, reject) => {
    target.once("error", reject);
    target.listen(port, "127.0.0.1", resolve);
  });
}

function close(target) {
  return new Promise((resolve, reject) => target.close((error) => error ? reject(error) : resolve()));
}
