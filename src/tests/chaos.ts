/**
 * Stage 4 — destructive local integration / chaos harness.
 * Run against an ISOLATED development DB, NEVER production.
 * Requirements: docker compose with worker-1..worker-8, local API + Postgres.
 * Compile as src/tests/chaos.ts -> dist/tests/chaos.js.
 * Logs EVERY request/kill and preserves failures in evidence/chaos/<run-id>/.
 */

import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdirSync, writeFileSync, createWriteStream } from "node:fs";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { pool } from "../db";

const API = (process.env.API_BASE_URL ?? "http://localhost:3000").replace(/\/$/, "");
const JOBS = Number(process.env.CHAOS_JOBS ?? 10_000);
const CREATE_RPS = Number(process.env.CHAOS_RPS ?? 100);
const TIMEOUT_MS = Number(process.env.CHAOS_TIMEOUT_MS ?? 600_000);
const KILL_INTERVAL_MS = Number(process.env.CHAOS_KILL_INTERVAL_MS ?? 2_000);
const RUN_ID = randomUUID();
const PREFIX = `chaos-${RUN_ID}-`;
const WORKERS = Array.from({ length: 8 }, (_, i) => `worker-${i + 1}`);
const DIR = resolve("evidence", "chaos", RUN_ID);
const EVENT_PATH = join(DIR, "events.jsonl");
mkdirSync(DIR, { recursive: true });

const errors: string[] = [];
let httpRequests = 0;
let created = 0;
let dupChecks = 0;
let cancelChecks = 0;
let killCount = 0;
let stopKilling = false;
let finishedSubmitting = false;
let latency: number[] = [];
let submitStart = 0;
let submitElapsedMs = 0;
const acceptedIds = new Map<string, string>();

function event(type: string, fields: Record<string, unknown> = {}) {
  appendFileSync(EVENT_PATH, JSON.stringify({ at: new Date().toISOString(), type, ...fields }) + "\n");
}
function problem(message: string) {
  errors.push(message);
  event("ERROR", { message });
  console.error("ERROR:", message);
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function command(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((done) => {
    const child = spawn("docker", ["compose", ...args], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.on("data", (b: Buffer) => { stdout += b.toString(); });
    child.stderr.on("data", (b: Buffer) => { stderr += b.toString(); });
    child.on("error", (e) => done({ code: -1, stdout, stderr: String(e) }));
    child.on("close", (code) => done({ code: code ?? -1, stdout, stderr }));
  });
}

async function request(path: string, init: RequestInit, meta: Record<string, unknown>) {
  const started = performance.now();
  httpRequests++;
  try {
    const response = await fetch(`${API}${path}`, { ...init, signal: AbortSignal.timeout(20_000) });
    const text = await response.text();
    let body: any = null;
    try { body = text ? JSON.parse(text) : null; }
    catch { body = { invalidJson: text.slice(0, 300) }; }
    const durationMs = performance.now() - started;
    latency.push(durationMs);
    event("HTTP", { ...meta, method: init.method ?? "GET", path, status: response.status, durationMs: Math.round(durationMs), body });
    return { status: response.status, body };
  } catch (e) {
    event("HTTP_ERROR", { ...meta, path, error: String(e) });
    throw e;
  }
}

function post(key: string, durationMs = 100, shouldFail = false) {
  return request("/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": key },
    body: JSON.stringify({ durationMs, shouldFail }),
  }, { key });
}

async function makeJob(i: number) {
  const key = `${PREFIX}${i}`;
  // Some duplicates are truly simultaneous, not merely sequential retries.
  const duplicate = i % 10 === 0;
  const shouldFail = i % 13 === 0;
  const results = await Promise.allSettled([
    post(key, 100, shouldFail),
    ...(duplicate ? [post(key, 100, shouldFail)] : []),
  ]);
  const good = results.filter((r): r is PromiseFulfilledResult<Awaited<ReturnType<typeof post>>> => r.status === "fulfilled");
  for (const r of results) if (r.status === "rejected") problem(`POST ${key}: ${String(r.reason)}`);
  if (good.length !== results.length) return;
  const ids = good.map((r) => r.value.body?.id);
  for (const r of good) if (![200, 201].includes(r.value.status) || typeof r.value.body?.id !== "string") problem(`Unexpected POST response for ${key}: HTTP ${r.value.status}`);
  if (new Set(ids).size !== 1) problem(`Duplicate POST produced different IDs for ${key}: ${JSON.stringify(ids)}`);
  if (typeof ids[0] === "string") { acceptedIds.set(key, ids[0]); created++; }
  if (duplicate) dupChecks++;

  // Submit cancellations while worker execution and other POSTs continue.
  if (i % 20 === 0 && typeof ids[0] === "string") {
    try {
      const r = await request(`/jobs/${ids[0]}/cancel`, { method: "POST" }, { jobId: ids[0] });
      if (![200, 409].includes(r.status)) problem(`Cancel ${key}: unexpected HTTP ${r.status}`);
      cancelChecks++;
    } catch (e) { problem(`Cancel ${key}: ${String(e)}`); }
  }
}

async function killLoop() {
  let turn = 0;
  while (!stopKilling) {
    await sleep(KILL_INTERVAL_MS);
    if (stopKilling) break;
    const name = WORKERS[Math.floor(Math.random() * WORKERS.length)];
    const killed = await command(["kill", "-s", "SIGKILL", name]);
    event("KILL", { worker: name, code: killed.code, stderr: killed.stderr.trim() });
    if (killed.code !== 0) problem(`Failed to SIGKILL ${name}: ${killed.stderr}`);
    else killCount++;
    // Compose restart policy varies; force service back into the desired state.
    const restarted = await command(["up", "-d", "--no-deps", name]);
    event("RESTART", { worker: name, code: restarted.code, stderr: restarted.stderr.trim() });
    if (restarted.code !== 0) problem(`Failed to restart ${name}: ${restarted.stderr}`);
    turn++;
  }
}

async function captureLogs() {
  const output = createWriteStream(join(DIR, "docker-workers.log"));
  const child = spawn("docker", ["compose", "logs", "--no-color", "--timestamps", ...WORKERS], { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.pipe(output, { end: false });
  child.stderr.pipe(output, { end: false });
  await new Promise<void>((done) => {
    child.on("error", (e) => { event("LOG_ERROR", { error: String(e) }); done(); });
    child.on("close", () => done());
  });
  await new Promise<void>((done) => output.end(done));
}

type DbSummary = {
  total: string; queued: string; running: string; succeeded: string; failed: string; cancelled: string;
  succeeded_missing_effect: string; non_success_with_effect: string; duplicate_effect_jobs: string;
};
async function dbSummary(): Promise<DbSummary> {
  const result = await pool.query<DbSummary>(`
    WITH scoped AS (
      SELECT id, status FROM jobs WHERE idempotency_key LIKE $1
    ), effects AS (
      SELECT s.id, s.status, COUNT(e.id)::int AS effects
      FROM scoped s LEFT JOIN execution_records e ON e.job_id = s.id
      GROUP BY s.id, s.status
    )
    SELECT
      COUNT(*)::text AS total,
      COUNT(*) FILTER (WHERE status = 'queued')::text AS queued,
      COUNT(*) FILTER (WHERE status = 'running')::text AS running,
      COUNT(*) FILTER (WHERE status = 'succeeded')::text AS succeeded,
      COUNT(*) FILTER (WHERE status = 'failed')::text AS failed,
      COUNT(*) FILTER (WHERE status = 'cancelled')::text AS cancelled,
      COUNT(*) FILTER (WHERE status = 'succeeded' AND effects <> 1)::text AS succeeded_missing_effect,
      COUNT(*) FILTER (WHERE status <> 'succeeded' AND effects <> 0)::text AS non_success_with_effect,
      COUNT(*) FILTER (WHERE effects > 1)::text AS duplicate_effect_jobs
    FROM effects`, [`${PREFIX}%`]);
  return result.rows[0];
}

function numeric(s: DbSummary, key: keyof DbSummary) { return Number(s[key]); }
function percentile(values: number[], pct: number) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return Math.round(sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * pct) - 1)]);
}

async function main() {
  const started = Date.now();
  let snapshot: DbSummary | null = null;
  
  event("START", { runId: RUN_ID, jobs: JOBS, createRps: CREATE_RPS, timeoutMs: TIMEOUT_MS, killIntervalMs: KILL_INTERVAL_MS, workers: WORKERS });
  console.log(`Run ${RUN_ID}: evidence -> ${DIR}`);
  try {
    assert.ok(Number.isSafeInteger(JOBS) && JOBS > 0);
    assert.ok(Number.isSafeInteger(CREATE_RPS) && CREATE_RPS > 0 && CREATE_RPS <= 1000);
    assert.ok(Number.isSafeInteger(KILL_INTERVAL_MS) && KILL_INTERVAL_MS >= 500);
    assert.ok(Number.isSafeInteger(TIMEOUT_MS) && TIMEOUT_MS > 0);
    await pool.query("SELECT 1");
    const health = await fetch(`${API}/jobs?page=1&limit=1`, { signal: AbortSignal.timeout(5000) });
    assert.equal(health.status, 200, "API must be running");
    for (const service of WORKERS) {
      const r = await command(["ps", "-q", service]);
      if (r.code !== 0 || !r.stdout.trim()) throw new Error(`Worker not running: ${service}. Add worker-5..8 to compose.yaml and start all 8.`);
    }
    console.log("Preflight passed: API, DB, and 8 worker containers visible.");
    const killing = killLoop();
    try {
      submitStart = performance.now();
      // 100 new jobs/s; duplicate and cancel traffic increases total HTTP RPS.
      for (let start = 0; start < JOBS; start += CREATE_RPS) {
        const batchStart = performance.now();
        const batchSize = Math.min(CREATE_RPS, JOBS - start);
        await Promise.all(Array.from({ length: batchSize }, (_, offset) => makeJob(start + offset)));
        if ((start + batchSize) % 1000 === 0 || start + batchSize === JOBS)
          console.log(`Submitted ${start + batchSize}/${JOBS}, errors=${errors.length}, kills=${killCount}`);
        const remaining = 1000 - (performance.now() - batchStart);
        if (remaining > 0) await sleep(remaining);
      }
      submitElapsedMs = performance.now() - submitStart;
      finishedSubmitting = true;
      event("SUBMITTED", { created, expected: JOBS, elapsedMs: Math.round(submitElapsedMs), kills: killCount });
      const deadline = Date.now() + TIMEOUT_MS;
      while (Date.now() < deadline) {
        snapshot = await dbSummary();
        event("PROGRESS", { db: snapshot });
        if (numeric(snapshot, "total") === JOBS && numeric(snapshot, "queued") === 0 && numeric(snapshot, "running") === 0) break;
        await sleep(2000);
      }
    } finally {
      stopKilling = true;
      await killing;
    }
    snapshot = await dbSummary();
    if (created !== JOBS) problem(`Only ${created}/${JOBS} POSTs returned IDs`);
    if (numeric(snapshot, "total") !== JOBS) problem(`Lost/extra jobs: DB total ${snapshot.total}, expected ${JOBS}`);
    for (const key of ["queued", "running", "succeeded_missing_effect", "non_success_with_effect", "duplicate_effect_jobs"] as const)
      if (numeric(snapshot, key) !== 0) problem(`${key}: ${snapshot[key]}`);
    if (killCount === 0) problem("No worker was killed during the run");
  } catch (e) {
    const details = e instanceof Error ? (e.stack ?? e.message) : String(e);
    problem(`Harness exception: ${details}`);
  } finally {
    stopKilling = true;
    try { snapshot ??= await dbSummary(); } catch (e) { problem(`Could not collect DB summary: ${String(e)}`); }
    try { await captureLogs(); } catch (e) { problem(`Could not capture Docker logs: ${String(e)}`); }
    try { await pool.end(); } catch (e) { problem(`Could not close DB pool: ${String(e)}`); }
    const result = {
      runId: RUN_ID, prefix: PREFIX, startedAt: new Date(started).toISOString(), finishedAt: new Date().toISOString(),
      configured: { jobs: JOBS, newJobsPerSecond: CREATE_RPS, timeoutMs: TIMEOUT_MS, killEveryMs: KILL_INTERVAL_MS, workers: 8 },
      measured: { created, duplicatePairs: dupChecks, cancellations: cancelChecks, kills: killCount,
        httpRequests, submitElapsedMs: Math.round(submitElapsedMs),
        newJobRate: submitElapsedMs > 0 ? +(created / (submitElapsedMs / 1000)).toFixed(1) : null,
        totalHttpRateDuringSubmitApprox: submitElapsedMs > 0 ? +(httpRequests / (submitElapsedMs / 1000)).toFixed(1) : null,
        httpLatencyP50Ms: percentile(latency, 0.50), httpLatencyP95Ms: percentile(latency, 0.95), httpLatencyP99Ms: percentile(latency, 0.99) },
      finalDatabase: snapshot,
      passed: finishedSubmitting && errors.length === 0,
      errors,
      limitations: [
        "One local run does not prove production reliability or sustained 100 RPS capacity.",
        "SIGKILL tests process crashes, not network partitions, power loss, disk corruption or database failover.",
        "A 30-second recovery deadline can leave jobs temporarily running; inspect timeouts and failures, do not hide them.",
        "The effects table proves only committed database effects, not external side effects.",
        "The harness samples races; it cannot prove all possible interleavings, clock-skew or isolation-level behavior.",
        "Observed POST rate includes concurrent retries/cancellations; measured create rate is reported separately.",
      ],
    };
    event("END", { passed: result.passed, errors: errors.length });
    writeFileSync(join(DIR, "summary.json"), JSON.stringify(result, null, 2) + "\n");
    console.log(`\n${result.passed ? "PASS" : "FAIL"} — ${DIR}/summary.json`);
    console.log(JSON.stringify({ measured: result.measured, db: snapshot, errors }, null, 2));
    if (!result.passed) process.exitCode = 1;
  }
}

void main();
