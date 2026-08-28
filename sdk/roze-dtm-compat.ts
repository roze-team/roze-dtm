import { RozeDtmApiError, type FetchLike, type JsonValue, type RequestOptions, type Transaction } from "./roze-dtm.js";

export type CompatTransactionType = "tcc" | "saga" | "workflow" | "msg" | "message" | "xa";
export interface CompatTransactionRequest {
  gid: string; trans_type: CompatTransactionType;
  steps?: Array<Record<string, string>>; payloads?: JsonValue[];
  timeout_to_fail?: number; rollback_reason?: string; custom_data?: string;
  query_prepared?: string; wait_result?: boolean; retry_interval?: number;
  request_timeout?: number; retry_limit?: number; branch_headers?: Record<string, string>;
  req_extra?: Record<string, string>;
}
export interface CompatBranchRequest {
  gid: string; trans_type: "tcc" | "xa" | "workflow"; branch_id: string;
  data?: string; op?: string; status?: "succeed" | "succeeded" | "failed";
  confirm?: string; cancel?: string; url?: string;
}
export interface CompatAllQuery {
  gid?: string; transType?: CompatTransactionType; status?: string; position?: string;
  limit?: number; createTimeStart?: number; createTimeEnd?: number;
}
export interface KvEntry { cat: string; k: string; v: string; version: number; created_at_millis: number; updated_at_millis: number }
export interface CompatSuccess { dtm_result: "SUCCESS" }
export interface CompatWorkflowSnapshot {
  transaction: { gid: string; status: string; rollback_reason: string; result: string };
  progresses: Array<{ status: string; bin_data: string; branch_id: string; op: string }>;
  dtm_result: "SUCCESS";
}
export interface JsonRpcErrorBody { code: number; message: string }
export type JsonRpcResponse<T> = { jsonrpc: "2.0"; id: JsonValue; result: T } | { jsonrpc: "2.0"; id: JsonValue; error: JsonRpcErrorBody };

export class RozeDtmJsonRpcError extends Error {
  constructor(public readonly code: number, message: string, public readonly id: JsonValue) { super(message); this.name = "RozeDtmJsonRpcError"; }
}

export class RozeDtmCompatClient {
  private readonly baseUrl: string;
  private rpcSequence = 0;
  constructor(baseUrl: string, private readonly token?: string, private readonly fetcher: FetchLike = fetch) {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("baseUrl must use HTTP(S)");
    this.baseUrl = url.toString().replace(/\/$/, "");
  }

  version(options?: RequestOptions) { return this.raw<{ version: string }>("GET", "/api/dtmsvr/version", undefined, undefined, options, false); }
  newGid(options?: RequestOptions) { return this.compat<{ gid: string; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/newGid", undefined, undefined, options); }
  query(gid: string, options?: RequestOptions) { return this.compat<{ transaction: Transaction; branches: Transaction["branches"]; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/query", { gid }, undefined, options); }
  all(query: CompatAllQuery = {}, options?: RequestOptions) { return this.compat<{ transactions: Transaction[]; next_position: string; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/all", query, undefined, options); }
  prepare(body: CompatTransactionRequest, options?: RequestOptions) { return this.write("prepare", body, options); }
  submit(body: CompatTransactionRequest, options?: RequestOptions) { return this.write("submit", body, options); }
  abort(body: CompatTransactionRequest, options?: RequestOptions) { return this.write("abort", body, options); }
  prepareWorkflow(body: CompatTransactionRequest, options?: RequestOptions) { return this.compat<CompatWorkflowSnapshot>("POST", "/api/dtmsvr/prepareWorkflow", undefined, body, options); }
  registerBranch(body: CompatBranchRequest, options?: RequestOptions) { return this.register("registerBranch", body, options); }
  registerTccBranch(body: CompatBranchRequest, options?: RequestOptions) { return this.register("registerTccBranch", body, options); }
  registerXaBranch(body: CompatBranchRequest, options?: RequestOptions) { return this.register("registerXaBranch", body, options); }
  forceStop(gid: string, options?: RequestOptions) { return this.admin("forceStop", gid, options); }
  resetNextCronTime(gid: string, options?: RequestOptions) { return this.admin("resetNextCronTime", gid, options); }
  resetCronTime(timeout = 105, limit = 100, options?: RequestOptions) { return this.compat<{ succeed_count: number; has_remaining: boolean; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/resetCronTime", { timeout, limit }, undefined, options); }
  subscribe(topic: string, url: string, remark = "", options?: RequestOptions) { return this.compat<CompatSuccess>("GET", "/api/dtmsvr/subscribe", { topic, url, remark }, undefined, options); }
  unsubscribe(topic: string, url: string, options?: RequestOptions) { return this.compat<CompatSuccess>("GET", "/api/dtmsvr/unsubscribe", { topic, url }, undefined, options); }
  deleteTopic(topic: string, options?: RequestOptions) { return this.compat<CompatSuccess>("DELETE", `/api/dtmsvr/topic/${encodeURIComponent(topic)}`, undefined, undefined, options); }
  scanKv(query: { cat?: string; position?: string; limit?: number } = {}, options?: RequestOptions) { return this.compat<{ kv: KvEntry[]; next_position: string; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/scanKV", query, undefined, options); }
  queryKv(query: { cat?: string; key?: string } = {}, options?: RequestOptions) { return this.compat<{ kv: KvEntry[]; dtm_result: "SUCCESS" }>("GET", "/api/dtmsvr/queryKV", query, undefined, options); }

  rpcNewGid(options?: RequestOptions) { return this.rpc<{ gid: string }>("newGid", {}, options); }
  rpcPrepare(params: CompatTransactionRequest, options?: RequestOptions) { return this.rpc<Record<string, never>>("prepare", params, options); }
  rpcSubmit(params: CompatTransactionRequest, options?: RequestOptions) { return this.rpc<Record<string, never>>("submit", params, options); }
  rpcAbort(params: CompatTransactionRequest, options?: RequestOptions) { return this.rpc<Record<string, never>>("abort", params, options); }
  rpcRegisterBranch(params: CompatBranchRequest, options?: RequestOptions) { return this.rpc<Record<string, never>>("registerBranch", params, options); }

  private write(operation: string, body: CompatTransactionRequest, options?: RequestOptions) { return this.compat<CompatSuccess>("POST", `/api/dtmsvr/${operation}`, undefined, body, options); }
  private register(operation: string, body: CompatBranchRequest, options?: RequestOptions) { return this.compat<CompatSuccess>("POST", `/api/dtmsvr/${operation}`, undefined, body, options); }
  private admin(operation: string, gid: string, options?: RequestOptions) { return this.compat<CompatSuccess>("POST", `/api/dtmsvr/${operation}`, undefined, { gid }, options); }

  private async rpc<T>(method: string, params: unknown, options?: RequestOptions): Promise<T> {
    const id = ++this.rpcSequence;
    const response = await this.raw<JsonRpcResponse<T>>("POST", "/api/json-rpc", undefined, { jsonrpc: "2.0", id, method, params }, options, false);
    if ("error" in response) throw new RozeDtmJsonRpcError(response.error.code, response.error.message, response.id);
    return response.result;
  }

  private async compat<T>(method: string, path: string, query?: object, body?: unknown, options?: RequestOptions): Promise<T> {
    return this.raw<T>(method, path, query, body, options, true);
  }
  private async raw<T>(method: string, path: string, query?: object, body?: unknown, options: RequestOptions = {}, requireSuccess = true): Promise<T> {
    const url = new URL(this.baseUrl + path);
    if (query) for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
    const headers: Record<string, string> = { accept: "application/json", ...options.headers };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await this.fetcher(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: options.signal });
    const payload = await response.json().catch(() => undefined) as ({ dtm_result?: string; message?: string; code?: number; trace_id?: string; data?: JsonValue } & T) | undefined;
    if (!response.ok || !payload || (requireSuccess && payload.dtm_result !== "SUCCESS")) throw new RozeDtmApiError(response.status, payload?.code ?? response.status, payload?.message ?? `HTTP ${response.status}`, payload?.trace_id, payload?.data);
    return payload;
  }
}
