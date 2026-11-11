/**
 * Stage 2 — live worker integration test. ALL FOUR WORKERS MUST BE RUNNING.
 * API + DB must be running as well. Do not run concurrently with api.test.ts.
 * Run: npm run build && node --test dist/tests/recovery-live.test.js
 *
 * This test injects an expired lease into a running job, verifies the worker
 * fleet recovers and retries it, and checks the final committed effect.
 * It does NOT kill an OS process or verify the 10-second heartbeat schedule.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { pool } from "../db";

const BASE = (process.env.API_BASE_URL ?? "http://localhost:3000").replace(/\/$/, "");
type Job = { id: string; status: string; attempts: number; lease_token: string | null };
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function getJob(id: string): Promise<Job> {
  const r = await fetch(`${BASE}/jobs/${id}`);
  assert.equal(r.status, 200, `GET job HTTP ${r.status}`);
  return (await r.json()) as Job;
}

async function waitFor(id: string, predicate: (j: Job) => boolean, timeoutMs: number): Promise<Job> {
  const deadline = Date.now() + timeoutMs;
  let last: Job | undefined;
  while (Date.now() < deadline) {
    last = await getJob(id);
    if (predicate(last)) return last;
    await sleep(100);
  }
  throw new Error(`Timed out waiting for job ${id}; last=${JSON.stringify(last)}`);
}

test("Stage 2 — live recovery with running workers", { timeout: 30_000 }, async (t) => {
  let id: string | null = null;
  try {
    await t.test("01 - Create and observe a running job", async () => {
      const r = await fetch(`${BASE}/jobs`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": `stage2-live-${randomUUID()}`,
        },
        body: JSON.stringify({ durationMs: 10_000, shouldFail: false }),
      });
      assert.equal(r.status, 201);
      const newJob = (await r.json()) as Job;
      id = newJob.id;
      const running = await waitFor(id, (j) => j.status === "running", 5_000);
      assert.equal(running.attempts, 1);
      assert.ok(running.lease_token);
    });

    await t.test("02 - Force the lease to expire while attempt 1 is running", async () => {
      assert.ok(id);
      const updated = await pool.query(
        `UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE id = $1 AND status = 'running' AND attempts = 1 RETURNING id`,
        [id],
      );
      assert.equal(updated.rows.length, 1, "Job completed before expiry injection");
    });

    await t.test("03 - Recovery claims attempt 2 and reaches succeeded", async () => {
      assert.ok(id);
      const complete = await waitFor(id, (j) => j.status === "succeeded", 25_000);
      assert.ok(complete.attempts >= 2, `Recovery did not retry: attempts=${complete.attempts}`);
      assert.equal(complete.lease_token, null);
    });

    await t.test("04 - Exactly one committed successful side effect", async () => {
      assert.ok(id);
      const r = await pool.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM execution_records WHERE job_id = $1`,
        [id],
      );
      assert.equal(Number(r.rows[0].count), 1);
    });

    await t.test("05 - Job remains terminal after original worker resumes", async () => {
      assert.ok(id);
      // In this implementation the old worker sleeps up to 10 seconds.
      // Wait to make sure its stale completion attempt has happened.
      await sleep(11_000);
      const final = await getJob(id);
      assert.equal(final.status, "succeeded");
      assert.ok(final.attempts >= 2);
      const r = await pool.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM execution_records WHERE job_id = $1`,
        [id],
      );
      assert.equal(Number(r.rows[0].count), 1);
    });
  } finally {
    // Intentionally retain this test job as evidence for the assignment.
    // It can be identified by its 'stage2-live-*' idempotency key.
    await pool.end();
  }
});
