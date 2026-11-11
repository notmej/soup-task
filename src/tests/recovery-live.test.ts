/**
 * Stage 2 — live worker integration test. ALL FOUR WORKERS MUST BE RUNNING.
 * API + DB must be running as well. Do not run concurrently with api.test.ts.
 * Run: npm run build && node --test dist/tests/recovery-live.test.js
 *
 * This test injects an expired lease into a running job, verifies the worker
 * fleet recovers and retries it, and checks the final committed effect.
 * It does NOT kill an OS process or verify the 10-second heartbeat schedule.
 *
 * Test sequence:
 *  1. Create a long-running job and observe its first claim.
 *  2. Expire its lease directly in PostgreSQL while attempt 1 is active.
 *  3. Observe recovery, a subsequent claim, and successful completion.
 *  4. Verify exactly one committed execution record.
 *  5. Allow the original worker to finish and confirm the result stays final.
 *
 * CAUTION: This integration test changes a live job's lease in the database.
 * Use an isolated development database, never a production environment.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { pool } from "../db";

// Allow an alternative API base URL; remove a trailing slash to avoid //jobs.
const BASE = (process.env.API_BASE_URL ?? "http://localhost:3000").replace(
  /\/$/,
  "",
);
// Only fields needed by these recovery assertions are represented here.
type Job = {
  id: string;
  status: string;
  attempts: number;
  lease_token: string | null;
};
// Small polling delay used while waiting for asynchronous worker transitions.
const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Retrieve a job through the public HTTP API, checking that retrieval succeeds.
 * @param id - Identifier of the job to inspect.
 * @returns The status, attempt count, and lease token used by the test.
 */
async function getJob(id: string): Promise<Job> {
  const r = await fetch(`${BASE}/jobs/${id}`);
  assert.equal(r.status, 200, `GET job HTTP ${r.status}`);
  return (await r.json()) as Job;
}

/**
 * Poll job status until a given condition holds or the deadline is reached.
 * Failure includes the last observed job snapshot for debugging.
 * @param id - Identifier of the job to poll.
 * @param predicate - Condition that signals the expected state has been reached.
 * @param timeoutMs - Maximum polling duration in milliseconds.
 * @returns The first observed job satisfying the predicate.
 */
async function waitFor(
  id: string,
  predicate: (j: Job) => boolean,
  timeoutMs: number,
): Promise<Job> {
  const deadline = Date.now() + timeoutMs;
  let last: Job | undefined;
  while (Date.now() < deadline) {
    last = await getJob(id);
    if (predicate(last)) return last;
    await sleep(100);
  }
  throw new Error(
    `Timed out waiting for job ${id}; last=${JSON.stringify(last)}`,
  );
}

// The parent test runs dependent subtests in sequence and closes the database
// pool even if an assertion fails. Its timeout applies to the parent test.
test(
  "Stage 2 — live recovery with running workers",
  { timeout: 30_000 },
  async (t) => {
    let id: string | null = null;
    try {
      // Establish a running first attempt so lease expiry can be injected safely.
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

      // Simulate the loss of the first worker's lease without killing its process.
      // The conditional UPDATE ensures this only affects the expected first attempt.
      await t.test(
        "02 - Force the lease to expire while attempt 1 is running",
        async () => {
          assert.ok(id);
          const updated = await pool.query(
            `UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE id = $1 AND status = 'running' AND attempts = 1 RETURNING id`,
            [id],
          );
          assert.equal(
            updated.rows.length,
            1,
            "Job completed before expiry injection",
          );
        },
      );

      // A recovery worker should requeue and reclaim the expired job. The attempt
      // count proves that the eventual success was not simply the original claim.
      await t.test(
        "03 - Recovery claims attempt 2 and reaches succeeded",
        async () => {
          assert.ok(id);
          const complete = await waitFor(
            id,
            (j) => j.status === "succeeded",
            25_000,
          );
          assert.ok(
            complete.attempts >= 2,
            `Recovery did not retry: attempts=${complete.attempts}`,
          );
          assert.equal(complete.lease_token, null);
        },
      );

      // Check the database side effect separately from the job's terminal status.
      await t.test(
        "04 - Exactly one committed successful side effect",
        async () => {
          assert.ok(id);
          const r = await pool.query<{ count: string }>(
            `SELECT COUNT(*)::text AS count FROM execution_records WHERE job_id = $1`,
            [id],
          );
          assert.equal(Number(r.rows[0].count), 1);
        },
      );

      // The first worker may still wake up and attempt a stale completion; verify
      // that this does not change the terminal status or create another DB effect.
      await t.test(
        "05 - Job remains terminal after original worker resumes",
        async () => {
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
        },
      );
    } finally {
      // Intentionally retain this test job as evidence for the assignment.
      // It can be identified by its 'stage2-live-*' idempotency key.
      await pool.end();
    }
  },
);
