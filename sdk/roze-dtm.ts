export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type TransactionKind = "Saga" | "Workflow" | "Message" | "Xa" | "Tcc";
export type TransactionStatus = "Submitted" | "Trying" | "Prepared" | "Succeeding" | "Succeeded" | "Aborting" | "Aborted" | "Failed";
export type BranchKind = "SagaAction" | "SagaCompensate" | "TccTry" | "TccConfirm" | "TccCancel" | "WorkflowAction" | "MessageAction" | "XaAction";
export type BranchStatus = "Pending" | "Running" | "Compensating" | "Succeeded" | "Failed" | "Skipped";

export interface TransactionOptions {
  wait_result?: boolean;
  retry_interval_millis?: number | null;
  request_timeout_millis?: number | null;
  retry_limit?: number | null;
  branch_headers?: Record<string, string>;
}

export interface BranchRequest {
  id: string;
  kind?: BranchKind | null;
  action: string;
  compensate?: string | null;
  confirm?: string | null;
  cancel?: string | null;
  payload?: JsonValue;
  dependencies?: string[];
}

export interface SubmitTransactionRequest {
  gid: string;
  kind?: TransactionKind | null;
  branches: BranchRequest[];
  timeout_millis?: number | null;
  metadata?: Record<string, string>;
  options?: TransactionOptions;
}

export interface Branch extends BranchRequest {
  kind: BranchKind;
  payload: JsonValue;
  status: BranchStatus;
  attempts: number;
  last_error: string | null;
  next_retry_millis: number | null;
  dependencies: string[];
}

export interface WorkflowProgress {
  branch_id: string;
  operation: string;
  status: "Succeeded" | "Failed";
  data: string;
}

export interface Transaction {
  gid: string;
  kind: TransactionKind;
  status: TransactionStatus;
  branches: Branch[];
  created_at_millis: number;
  updated_at_millis: number;
  revision: number;
  timeout_millis: number | null;
  options: TransactionOptions;
  workflow_progresses?: WorkflowProgress[];
  metadata: Record<string, string>;
}

export interface TransactionQuery {
  gid?: string;
  kind?: TransactionKind | Lowercase<TransactionKind>;
  status?: TransactionStatus | Lowercase<TransactionStatus>;
  offset?: number;
  limit?: number;
}

export interface TransactionPage { items: Transaction[]; offset: number; limit: number; total: number }
export interface TransactionStats { total: number; by_kind: Record<string, number>; by_status: Record<string, number> }
export interface RecoveryResult { recovered: Transaction[]; count: number }

export interface DashboardTransactionRow {
  gid: string; kind: string; status: string; branch_count: number;
  completed_branch_count: number; failed_branch_count: number; total_attempts: number;
  next_retry_millis: number | null; created_at_millis: number; updated_at_millis: number;
  timeout_millis: number | null; terminal: boolean; xa_reconciliation_state: string | null;
}

export interface DashboardAuditEvent {
  sequence: number; occurred_at_millis: number; event: string; outcome: string;
  resource_id?: string; transaction_status?: string;
}

export interface DashboardSnapshot {
  generated_at_millis: number;
  summary: TransactionStats & { active: number; succeeded: number; aborted: number; failed: number; retry_scheduled: number; xa_awaiting_decision: number; xa_phase2_in_progress: number; xa_manual_reconciliation_required: number };
  transactions: { items: DashboardTransactionRow[]; offset: number; limit: number; total: number };
  audit: { items: DashboardAuditEvent[]; limit: number; capacity: number };
}

export interface XaReconciliationSnapshot {
  generated_at_millis: number; awaiting_decision: number; phase2_in_progress: number;
  manual_reconciliation_required: number;
  items: Array<{ gid: string; status: string; reconciliation_state: string; branch_count: number; unresolved_branch_count: number; total_attempts: number; next_retry_millis: number | null; updated_at_millis: number }>;
}

export interface RequestOptions { signal?: AbortSignal; headers?: Record<string, string> }
export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export class RozeDtmApiError extends Error {
  constructor(public readonly status: number, public readonly code: number, message: string, public readonly traceId?: string, public readonly data?: JsonValue) {
    super(message);
    this.name = "RozeDtmApiError";
  }
}

export class RozeDtmClient {
  private readonly baseUrl: string;
  constructor(baseUrl: string, private readonly token?: string, private readonly fetcher: FetchLike = fetch) {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("baseUrl must use HTTP(S)");
    this.baseUrl = url.toString().replace(/\/$/, "");
  }

  submitTcc(body: SubmitTransactionRequest, options?: RequestOptions) { return this.request<Transaction>("POST", "/v1/tcc", undefined, body, options); }
  submitSaga(body: SubmitTransactionRequest, options?: RequestOptions) { return this.request<Transaction>("POST", "/v1/saga", undefined, body, options); }
  submitWorkflow(body: SubmitTransactionRequest, options?: RequestOptions) { return this.request<Transaction>("POST", "/v1/workflows", undefined, body, options); }
  submitMessage(body: SubmitTransactionRequest, options?: RequestOptions) { return this.request<Transaction>("POST", "/v1/messages", undefined, body, options); }
  submitXa(body: SubmitTransactionRequest, options?: RequestOptions) { return this.request<Transaction>("POST", "/v1/xa", undefined, body, options); }
  getTransaction(gid: string, options?: RequestOptions) { return this.request<Transaction>("GET", this.gidPath(gid), undefined, undefined, options); }
  listTransactions(query: TransactionQuery = {}, options?: RequestOptions) { return this.request<TransactionPage>("GET", "/v1/transactions", query, undefined, options); }
  recoverAll(options?: RequestOptions) { return this.request<RecoveryResult>("POST", "/v1/recover", undefined, undefined, options); }
  stats(options?: RequestOptions) { return this.request<TransactionStats>("GET", "/v1/stats", undefined, undefined, options); }
  dashboard(query: TransactionQuery = {}, options?: RequestOptions) { return this.request<DashboardSnapshot>("GET", "/v1/dashboard", query, undefined, options); }
  xaReconciliation(options?: RequestOptions) { return this.request<XaReconciliationSnapshot>("GET", "/v1/xa/reconciliation", undefined, undefined, options); }

  prepareTcc(gid: string, options?: RequestOptions) { return this.transition("tcc", gid, "prepare", options); }
  confirmTcc(gid: string, options?: RequestOptions) { return this.transition("tcc", gid, "confirm", options); }
  cancelTcc(gid: string, options?: RequestOptions) { return this.transition("tcc", gid, "cancel", options); }
  startSaga(gid: string, options?: RequestOptions) { return this.transition("saga", gid, "start", options); }
  abortSaga(gid: string, options?: RequestOptions) { return this.transition("saga", gid, "abort", options); }
  startWorkflow(gid: string, options?: RequestOptions) { return this.transition("workflows", gid, "start", options); }
  abortWorkflow(gid: string, options?: RequestOptions) { return this.transition("workflows", gid, "abort", options); }
  prepareMessage(gid: string, options?: RequestOptions) { return this.transition("messages", gid, "prepare", options); }
  dispatchMessage(gid: string, options?: RequestOptions) { return this.transition("messages", gid, "dispatch", options); }
  abortMessage(gid: string, options?: RequestOptions) { return this.transition("messages", gid, "abort", options); }
  prepareXa(gid: string, options?: RequestOptions) { return this.transition("xa", gid, "prepare", options); }
  commitXa(gid: string, options?: RequestOptions) { return this.transition("xa", gid, "commit", options); }
  rollbackXa(gid: string, options?: RequestOptions) { return this.transition("xa", gid, "rollback", options); }
  recoverTransaction(gid: string, options?: RequestOptions) { return this.adminTransition(gid, "recover", options); }
  forceStopTransaction(gid: string, options?: RequestOptions) { return this.adminTransition(gid, "force-stop", options); }
  resetRetryTransaction(gid: string, options?: RequestOptions) { return this.adminTransition(gid, "reset-retry", options); }

  private transition(kind: string, gid: string, action: string, options?: RequestOptions) { return this.request<Transaction>("POST", `/v1/${kind}/${encodeURIComponent(gid)}/${action}`, undefined, undefined, options); }
  private adminTransition(gid: string, action: string, options?: RequestOptions) { return this.request<Transaction>("POST", `${this.gidPath(gid)}/${action}`, undefined, undefined, options); }
  private gidPath(gid: string) { if (!gid) throw new TypeError("gid must not be empty"); return `/v1/transactions/${encodeURIComponent(gid)}`; }

  private async request<T>(method: string, path: string, query?: Record<string, unknown>, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const url = new URL(this.baseUrl + path);
    if (query) for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
    const headers: Record<string, string> = { accept: "application/json", ...options.headers };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await this.fetcher(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: options.signal });
    const payload = await response.json().catch(() => undefined) as { code?: number; msg?: string; message?: string; data?: T; trace_id?: string } | undefined;
    if (!response.ok || !payload || typeof payload.code !== "number" || payload.code !== 0) {
      throw new RozeDtmApiError(response.status, payload?.code ?? response.status, payload?.msg ?? payload?.message ?? `HTTP ${response.status}`, payload?.trace_id, payload?.data as JsonValue | undefined);
    }
    return payload.data as T;
  }
}
