#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const evidenceDir = resolve(required("ROZE_DTM_EVIDENCE_DIR"));
const revision = required("ROZE_DTM_EXPECTED_REVISION").toLowerCase();
if (!/^[0-9a-f]{40}$/.test(revision)) throw new Error("ROZE_DTM_EXPECTED_REVISION must be a full lowercase Git revision");
const topology = JSON.parse(required("ROZE_DTM_TOPOLOGY_JSON"));
const profile = process.env.ROZE_DTM_SOAK_PROFILE?.trim() || "smoke";
const profileMinimums = { smoke: 1, "24h": 86_400, "72h": 259_200 };
if (!(profile in profileMinimums)) throw new Error("ROZE_DTM_SOAK_PROFILE must be smoke, 24h, or 72h");
const defaultDuration = profileMinimums[profile] === 1 ? 60 : profileMinimums[profile];
const targetDurationSeconds = integer(process.env.ROZE_DTM_SOAK_DURATION_SECONDS ?? String(defaultDuration), profileMinimums[profile], 604_800, "ROZE_DTM_SOAK_DURATION_SECONDS");
const intervalSeconds = integer(process.env.ROZE_DTM_SOAK_INTERVAL_SECONDS ?? (profile === "smoke" ? "30" : "300"), 1, 3600, "ROZE_DTM_SOAK_INTERVAL_SECONDS");
const maxFailedSamples = integer(process.env.ROZE_DTM_SOAK_MAX_FAILED_SAMPLES ?? "0", 0, 100_000, "ROZE_DTM_SOAK_MAX_FAILED_SAMPLES");
const faultTimeline = await loadFaultTimeline(process.env.ROZE_DTM_FAULT_TIMELINE_JSON);
await mkdir(evidenceDir, { recursive: true });

const startedAt = new Date();
const startedMonotonic = performance.now();
const samples = [];
let interrupted = false;
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => { interrupted = true; });

while (!interrupted) {
  const sampleNumber = samples.length + 1;
  const sampleDir = resolve(evidenceDir, `sample-${String(sampleNumber).padStart(6, "0")}`);
  await mkdir(sampleDir, { recursive: true });
  const sampledAt = new Date();
  const exitCode = await runNode(resolve(scriptDir, "production-http-contract-smoke.mjs"), {
    ...process.env,
    ROZE_DTM_EVIDENCE_DIR: sampleDir,
  });
  const reportPath = resolve(sampleDir, "http-contract-report.json");
  let report;
  try {
    report = JSON.parse(await readFile(reportPath, "utf8"));
  } catch (error) {
    report = { verdict: "fail", revision: null, error: publicError(error) };
  }
  samples.push({
    sequence: sampleNumber,
    sampled_at: sampledAt.toISOString(),
    exit_code: exitCode,
    verdict: exitCode === 0 && report.verdict === "pass" && report.revision === revision ? "pass" : "fail",
    report_path: relative(evidenceDir, reportPath).replaceAll("\\", "/"),
    report_sha256: await sha256IfPresent(reportPath),
  });
  const elapsedSeconds = (performance.now() - startedMonotonic) / 1000;
  if (elapsedSeconds >= targetDurationSeconds) break;
  await sleep(Math.min(intervalSeconds, targetDurationSeconds - elapsedSeconds) * 1000);
}

const finishedAt = new Date();
const elapsedSeconds = (performance.now() - startedMonotonic) / 1000;
const failedSamples = samples.filter((sample) => sample.verdict !== "pass").length;
const durationSatisfied = elapsedSeconds >= targetDurationSeconds;
const verdict = !interrupted && durationSatisfied && failedSamples <= maxFailedSamples ? "pass" : "inconclusive";
const report = {
  schema_version: 1,
  area: "production-soak",
  profile,
  qualification: profile === "smoke" ? "harness_only" : profile,
  verdict,
  revision,
  topology,
  started_at: startedAt.toISOString(),
  finished_at: finishedAt.toISOString(),
  target_duration_seconds: targetDurationSeconds,
  elapsed_seconds: Number(elapsedSeconds.toFixed(3)),
  interval_seconds: intervalSeconds,
  workload: { kind: "production-http-contract", samples: samples.length },
  error_budget: { max_failed_samples: maxFailedSamples, failed_samples: failedSamples },
  interrupted,
  fault_timeline: faultTimeline,
  samples,
};
await writeFile(resolve(evidenceDir, "soak-report.json"), JSON.stringify(report, null, 2) + "\n", { encoding: "utf8", mode: 0o600 });
console.log(JSON.stringify({ verdict, profile, samples: samples.length, failed_samples: failedSamples, elapsed_seconds: report.elapsed_seconds, evidence_dir: evidenceDir }));
if (verdict !== "pass") process.exitCode = 1;

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function integer(value, minimum, maximum, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  return parsed;
}

function runNode(script, env) {
  return new Promise((resolveCode, reject) => {
    const child = spawn(process.execPath, [script], { env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolveCode(signal ? 128 : (code ?? 1)));
  });
}

async function loadFaultTimeline(path) {
  if (!path?.trim()) return [];
  const parsed = JSON.parse(await readFile(resolve(path), "utf8"));
  if (!Array.isArray(parsed)) throw new Error("ROZE_DTM_FAULT_TIMELINE_JSON must contain a JSON array");
  for (const entry of parsed) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new Error("fault timeline entries must be objects");
    for (const field of ["at", "fault", "outcome"]) {
      if (typeof entry[field] !== "string" || !entry[field].trim()) throw new Error(`fault timeline entry ${field} is required`);
    }
  }
  return parsed;
}

async function sha256IfPresent(path) {
  try { return createHash("sha256").update(await readFile(path)).digest("hex"); }
  catch { return null; }
}

function sleep(milliseconds) { return new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)); }
function publicError(error) { return error instanceof Error ? error.message.slice(0, 512) : "unknown failure"; }
