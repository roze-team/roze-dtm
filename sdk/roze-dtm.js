/** @typedef {null|boolean|number|string|JsonValue[]|Object<string, JsonValue>} JsonValue */

export class RozeDtmApiError extends Error {
  constructor(status, code, message, traceId, data) {
    super(message); this.name = "RozeDtmApiError";
    this.status = status; this.code = code; this.traceId = traceId; this.data = data;
  }
}

export class RozeDtmClient {
  constructor(baseUrl, token, fetcher = fetch) {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("baseUrl must use HTTP(S)");
    this.baseUrl = url.toString().replace(/\/$/, ""); this.token = token; this.fetcher = fetcher;
  }
  submitTcc(body, options) { return this.request("POST", "/v1/tcc", undefined, body, options); }
  submitSaga(body, options) { return this.request("POST", "/v1/saga", undefined, body, options); }
  submitWorkflow(body, options) { return this.request("POST", "/v1/workflows", undefined, body, options); }
  submitMessage(body, options) { return this.request("POST", "/v1/messages", undefined, body, options); }
  submitXa(body, options) { return this.request("POST", "/v1/xa", undefined, body, options); }
  getTransaction(gid, options) { return this.request("GET", this.gidPath(gid), undefined, undefined, options); }
  listTransactions(query = {}, options) { return this.request("GET", "/v1/transactions", query, undefined, options); }
  recoverAll(options) { return this.request("POST", "/v1/recover", undefined, undefined, options); }
  stats(options) { return this.request("GET", "/v1/stats", undefined, undefined, options); }
  dashboard(query = {}, options) { return this.request("GET", "/v1/dashboard", query, undefined, options); }
  xaReconciliation(options) { return this.request("GET", "/v1/xa/reconciliation", undefined, undefined, options); }
  prepareTcc(gid, options) { return this.transition("tcc", gid, "prepare", options); }
  confirmTcc(gid, options) { return this.transition("tcc", gid, "confirm", options); }
  cancelTcc(gid, options) { return this.transition("tcc", gid, "cancel", options); }
  startSaga(gid, options) { return this.transition("saga", gid, "start", options); }
  abortSaga(gid, options) { return this.transition("saga", gid, "abort", options); }
  startWorkflow(gid, options) { return this.transition("workflows", gid, "start", options); }
  abortWorkflow(gid, options) { return this.transition("workflows", gid, "abort", options); }
  prepareMessage(gid, options) { return this.transition("messages", gid, "prepare", options); }
  dispatchMessage(gid, options) { return this.transition("messages", gid, "dispatch", options); }
  abortMessage(gid, options) { return this.transition("messages", gid, "abort", options); }
  prepareXa(gid, options) { return this.transition("xa", gid, "prepare", options); }
  commitXa(gid, options) { return this.transition("xa", gid, "commit", options); }
  rollbackXa(gid, options) { return this.transition("xa", gid, "rollback", options); }
  recoverTransaction(gid, options) { return this.adminTransition(gid, "recover", options); }
  forceStopTransaction(gid, options) { return this.adminTransition(gid, "force-stop", options); }
  resetRetryTransaction(gid, options) { return this.adminTransition(gid, "reset-retry", options); }
  transition(kind, gid, action, options) { return this.request("POST", `/v1/${kind}/${encodeURIComponent(gid)}/${action}`, undefined, undefined, options); }
  adminTransition(gid, action, options) { return this.request("POST", `${this.gidPath(gid)}/${action}`, undefined, undefined, options); }
  gidPath(gid) { if (!gid) throw new TypeError("gid must not be empty"); return `/v1/transactions/${encodeURIComponent(gid)}`; }
  async request(method, path, query, body, options = {}) {
    const url = new URL(this.baseUrl + path);
    if (query) for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
    const headers = { accept: "application/json", ...options.headers };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await this.fetcher(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: options.signal });
    const payload = await response.json().catch(() => undefined);
    if (!response.ok || !payload || typeof payload.code !== "number" || payload.code !== 0) throw new RozeDtmApiError(response.status, payload?.code ?? response.status, payload?.msg ?? payload?.message ?? `HTTP ${response.status}`, payload?.trace_id, payload?.data);
    return payload.data;
  }
}
