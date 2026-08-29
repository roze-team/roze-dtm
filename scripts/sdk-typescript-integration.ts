#!/usr/bin/env node
import { RozeDtmClient, type TransactionStats } from "../sdk/roze-dtm.ts";
import { RozeDtmCompatClient, type CompatGlobalTransaction } from "../sdk/roze-dtm-compat.ts";

declare const process: { env: Record<string, string | undefined> };

const baseUrl = process.env.ROZE_DTM_BASE_URL ?? "http://127.0.0.1:18090";
const token = required("ROZE_DTM_CONTROL_TOKEN");
const expectedRevision = required("ROZE_DTM_EXPECTED_REVISION");
const native = new RozeDtmClient(baseUrl, token);
const compat = new RozeDtmCompatClient(baseUrl, token);

const version = await compat.version();
assert(version.release_revision === expectedRevision, "TypeScript SDK observed the wrong release revision");
const generated = await compat.rpcNewGid();
assert(generated.gid.length > 0, "TypeScript JSON-RPC client returned an empty gid");
const stats: TransactionStats = await native.stats();
assert(stats.total >= 1, "TypeScript native client observed no transactions");
const page = await compat.all({ limit: 1 });
const transaction: CompatGlobalTransaction | undefined = page.transactions[0];
assert(transaction?.gid.length > 0, "TypeScript compatibility client could not query transactions");

console.log(JSON.stringify({ verdict: "pass", client: "typescript", transaction: transaction.gid }));

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
