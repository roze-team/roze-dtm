import { RozeDtmApiError } from "./roze-dtm.js";

export class RozeDtmJsonRpcError extends Error {
  constructor(code, message, id) { super(message); this.name = "RozeDtmJsonRpcError"; this.code = code; this.id = id; }
}

export class RozeDtmCompatClient {
  constructor(baseUrl, token, fetcher = fetch) {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("baseUrl must use HTTP(S)");
    this.baseUrl = url.toString().replace(/\/$/, ""); this.token = token; this.fetcher = fetcher; this.rpcSequence = 0;
  }
  version(options) { return this.raw("GET", "/api/dtmsvr/version", undefined, undefined, options, false); }
  newGid(options) { return this.compat("GET", "/api/dtmsvr/newGid", undefined, undefined, options); }
  query(gid, options) { return this.compat("GET", "/api/dtmsvr/query", { gid }, undefined, options); }
  all(query = {}, options) { return this.compat("GET", "/api/dtmsvr/all", query, undefined, options); }
  prepare(body, options) { return this.write("prepare", body, options); }
  submit(body, options) { return this.write("submit", body, options); }
  abort(body, options) { return this.write("abort", body, options); }
  prepareWorkflow(body, options) { return this.compat("POST", "/api/dtmsvr/prepareWorkflow", undefined, body, options); }
  registerBranch(body, options) { return this.register("registerBranch", body, options); }
  registerTccBranch(body, options) { return this.register("registerTccBranch", body, options); }
  registerXaBranch(body, options) { return this.register("registerXaBranch", body, options); }
  forceStop(gid, options) { return this.admin("forceStop", gid, options); }
  resetNextCronTime(gid, options) { return this.admin("resetNextCronTime", gid, options); }
  resetCronTime(timeout = 105, limit = 100, options) { return this.compat("GET", "/api/dtmsvr/resetCronTime", { timeout, limit }, undefined, options); }
  subscribe(topic, url, remark = "", options) { return this.compat("GET", "/api/dtmsvr/subscribe", { topic, url, remark }, undefined, options); }
  unsubscribe(topic, url, options) { return this.compat("GET", "/api/dtmsvr/unsubscribe", { topic, url }, undefined, options); }
  deleteTopic(topic, options) { return this.compat("DELETE", `/api/dtmsvr/topic/${encodeURIComponent(topic)}`, undefined, undefined, options); }
  scanKv(query = {}, options) { return this.compat("GET", "/api/dtmsvr/scanKV", query, undefined, options); }
  queryKv(query = {}, options) { return this.compat("GET", "/api/dtmsvr/queryKV", query, undefined, options); }
  rpcNewGid(options) { return this.rpc("newGid", {}, options); }
  rpcPrepare(params, options) { return this.rpc("prepare", params, options); }
  rpcSubmit(params, options) { return this.rpc("submit", params, options); }
  rpcAbort(params, options) { return this.rpc("abort", params, options); }
  rpcRegisterBranch(params, options) { return this.rpc("registerBranch", params, options); }
  write(operation, body, options) { return this.compat("POST", `/api/dtmsvr/${operation}`, undefined, body, options); }
  register(operation, body, options) { return this.compat("POST", `/api/dtmsvr/${operation}`, undefined, body, options); }
  admin(operation, gid, options) { return this.compat("POST", `/api/dtmsvr/${operation}`, undefined, { gid }, options); }
  async rpc(method, params, options) {
    const id = ++this.rpcSequence;
    const response = await this.raw("POST", "/api/json-rpc", undefined, { jsonrpc: "2.0", id, method, params }, options, false);
    if ("error" in response) throw new RozeDtmJsonRpcError(response.error.code, response.error.message, response.id);
    return response.result;
  }
  compat(method, path, query, body, options) { return this.raw(method, path, query, body, options, true); }
  async raw(method, path, query, body, options = {}, requireSuccess = true) {
    const url = new URL(this.baseUrl + path);
    if (query) for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
    const headers = { accept: "application/json", ...options.headers };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await this.fetcher(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: options.signal });
    const payload = await response.json().catch(() => undefined);
    if (!response.ok || !payload || (requireSuccess && payload.dtm_result !== "SUCCESS")) throw new RozeDtmApiError(response.status, payload?.code ?? response.status, payload?.message ?? `HTTP ${response.status}`, payload?.trace_id, payload?.data);
    return payload;
  }
}
