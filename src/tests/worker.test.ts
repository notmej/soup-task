/**
 * Stage 1 — Worker system integration tests.
 *
 * REQUIREMENTS:
 * - PostgreSQL running
 * - ALL worker processes STOPPED
 * - DATABASE_URL configured in .env
 *
 * Tests the queue/database functions directly.
 *
 * Run:
 * npm run build
 * node --test dist/tests/worker.test.js
 *
 * These tests do NOT prove actual worker-process execution
 * or that duration_ms is respected by worker.ts.
 */

/**
 * Load environment configuration and import the Node.js test/assertion tools,
 * PostgreSQL connection pool, and queue/job operations under test.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import { pool } from "../db";
import { createJob, cancelJob } from "../jobs";
import { claimJob, completeJob } from "../queue";

/** IDs of jobs created by this suite, retained for targeted cleanup. */
const createdJobs: string[] = [];

/** All valid persisted job lifecycle states used by these assertions. */
type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

/**
 * Database representation of the fields inspected by the worker tests.
 * Nullable ownership fields are cleared when a job becomes terminal.
 */
interface DbJob {
  id: string;
  status: JobStatus;
  attempts: number;
  worker_id: string | null;
  lease_token: string | null;
  finished_at: Date | null;
}

// Create a test job.
// Set its creation date far in the past so FIFO claiming
// selects it before ordinary queued jobs.
/**
 * Create an isolated test job and backdate it to make FIFO claiming predictable.
 *
 * @param shouldFail - Whether the job's configured execution should fail.
 * @returns ID of the created job, also tracked for cleanup.
 */
async function seedJob(shouldFail = false): Promise<string> {
  const { job } = await createJob(`stage1-${randomUUID()}`, {
    durationMs: 1000,
    shouldFail,
  });

  const id = job.id as string;
  createdJobs.push(id);

  await pool.query(
    `UPDATE jobs
     SET created_at = TIMESTAMPTZ '1800-01-01'
     WHERE id = $1`,
    [id],
  );

  return id;
}

// Read the current state directly from PostgreSQL.
/**
 * Query the persisted job state directly, avoiding reliance on API responses.
 *
 * @param id - UUID of the job to inspect.
 * @returns The stored status, attempt count, ownership, and completion fields.
 */
async function readJob(id: string): Promise<DbJob> {
  const result = await pool.query<DbJob>(
    `SELECT id, status, attempts, worker_id,
            lease_token, finished_at
     FROM jobs
     WHERE id = $1`,
    [id],
  );

  assert.equal(result.rows.length, 1);
  return result.rows[0];
}

// Count committed execution side effects.
/**
 * Count committed execution-record rows for a single job.
 * The count is used to verify the database side-effect invariant.
 *
 * @param id - UUID of the job to inspect.
 * @returns Number of matching committed execution records.
 */
async function effectCount(id: string): Promise<number> {
  const result = await pool.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
     FROM execution_records
     WHERE job_id = $1`,
    [id],
  );

  return Number(result.rows[0].count);
}

/**
 * Execute the worker-system checks sequentially as named subtests.
 * Workers must remain stopped so direct queue calls control job ownership.
 */
test("Stage 1 — Worker system and job execution", async (t) => {
  try {
    /**
     * Claiming a queued job should set running status, increment attempts,
     * and persist the claimant's worker ID and lease token.
     */
    // TEST 01
    await t.test("01 - Claim queued job: queued -> running", async () => {
      const id = await seedJob();

      const claimed = await claimJob("stage1-worker-1");

      assert.ok(claimed, "Expected a queued job");
      assert.equal(claimed.id, id);
      assert.equal(claimed.attempts, 1);
      assert.ok(claimed.lease_token);

      const job = await readJob(id);

      assert.equal(job.status, "running");
      assert.equal(job.worker_id, "stage1-worker-1");
      assert.equal(job.attempts, 1);
      assert.equal(job.lease_token, claimed.lease_token);

      // Cleanup state for this test.
      await cancelJob(id);
    });

    /**
     * Four concurrent claim requests must select four distinct queued jobs,
     * without assigning any single job to two claimants.
     */
    // TEST 02
    await t.test("02 - Four workers claim four different jobs", async () => {
      const ids = await Promise.all([
        seedJob(),
        seedJob(),
        seedJob(),
        seedJob(),
      ]);

      const workers = [
        "stage1-worker-A",
        "stage1-worker-B",
        "stage1-worker-C",
        "stage1-worker-D",
      ];

      const claims = await Promise.all(
        workers.map((worker) => claimJob(worker)),
      );

      for (const claim of claims) {
        assert.ok(claim, "Every worker must claim a job");
      }

      const claimedIds = claims.map((claim) => claim!.id);

      // Every worker must receive a different job.
      assert.equal(new Set(claimedIds).size, 4);

      // The four claimed jobs must be the four created jobs.
      assert.deepEqual(new Set(claimedIds), new Set(ids));

      // Confirm all four jobs are running.
      for (const id of ids) {
        const job = await readJob(id);
        assert.equal(job.status, "running");
        assert.equal(job.attempts, 1);
      }

      // Cancel them before the next test.
      for (const id of ids) {
        await cancelJob(id);
      }
    });

    /**
     * Completing with a valid lease transitions a running job to succeeded,
     * records the finish time, and invalidates its lease token.
     */
    // TEST 03
    await t.test(
      "03 - Successful completion: running -> succeeded",
      async () => {
        const id = await seedJob();

        const claimed = await claimJob("stage1-success");
        assert.ok(claimed);
        assert.equal(claimed.id, id);

        const completed = await completeJob(
          id,
          claimed.lease_token,
          "stage1-success",
          "succeeded",
        );

        assert.equal(completed, true);

        const job = await readJob(id);

        assert.equal(job.status, "succeeded");
        assert.equal(job.finished_at !== null, true);
        assert.equal(job.lease_token, null);
      },
    );

    /**
     * Verify that successful completion commits exactly one execution record,
     * with no record present before completion.
     */
    // TEST 04
    await t.test(
      "04 - Successful completion creates one side effect",
      async () => {
        const id = await seedJob();

        const claimed = await claimJob("stage1-effect");
        assert.ok(claimed);
        assert.equal(claimed.id, id);

        assert.equal(await effectCount(id), 0);

        const result = await completeJob(
          id,
          claimed.lease_token,
          "stage1-effect",
          "succeeded",
        );

        assert.equal(result, true);

        assert.equal((await readJob(id)).status, "succeeded");
        assert.equal(await effectCount(id), 1);
      },
    );

    /**
     * A failed result should produce the failed terminal state but no
     * successful-execution record under this implementation's semantics.
     */
    // TEST 05
    await t.test("05 - Failed completion: running -> failed", async () => {
      const id = await seedJob(true);

      const claimed = await claimJob("stage1-failure");
      assert.ok(claimed);
      assert.equal(claimed.id, id);

      const completed = await completeJob(
        id,
        claimed.lease_token,
        "stage1-failure",
        "failed",
      );

      assert.equal(completed, true);

      const job = await readJob(id);

      assert.equal(job.status, "failed");
      assert.ok(job.finished_at);
      assert.equal(job.lease_token, null);

      // Current design: only successful completion
      // writes a committed execution side effect.
      assert.equal(await effectCount(id), 0);
    });

    /**
     * Repeating completion with the original lease must be rejected,
     * preventing a second committed execution record.
     */
    // TEST 06
    await t.test(
      "06 - Repeated completion cannot duplicate side effects",
      async () => {
        const id = await seedJob();

        const claimed = await claimJob("stage1-once");
        assert.ok(claimed);
        assert.equal(claimed.id, id);

        const token = claimed.lease_token;

        assert.equal(
          await completeJob(id, token, "stage1-once", "succeeded"),
          true,
        );

        // The same completion must be rejected.
        assert.equal(
          await completeJob(id, token, "stage1-once", "succeeded"),
          false,
        );

        assert.equal((await readJob(id)).status, "succeeded");
        assert.equal(await effectCount(id), 1);
      },
    );

    /**
     * Cancellation revokes a running job's lease; a subsequent completion
     * attempt by its former worker must not change state or write an effect.
     */
    // TEST 07
    await t.test(
      "07 - Cancel running job prevents later completion",
      async () => {
        const id = await seedJob();

        const claimed = await claimJob("stage1-cancel");
        assert.ok(claimed);
        assert.equal(claimed.id, id);

        const result = await cancelJob(id);

        assert.ok(result);
        assert.equal(result.cancelled, true);

        const job = await readJob(id);

        assert.equal(job.status, "cancelled");
        assert.equal(job.lease_token, null);
        assert.ok(job.finished_at);

        // A worker that was running before cancellation
        // must not commit a stale successful result.
        const completed = await completeJob(
          id,
          claimed.lease_token,
          "stage1-cancel",
          "succeeded",
        );

        assert.equal(completed, false);
        assert.equal(await effectCount(id), 0);
        assert.equal((await readJob(id)).status, "cancelled");
      },
    );

    /**
     * Race cancellation against completion; either may win, but the final
     * job status and execution-record count must agree.
     */
    // TEST 08
    await t.test(
      "08 - Cancel and completion race stays consistent",
      async () => {
        const id = await seedJob();

        const claimed = await claimJob("stage1-race");
        assert.ok(claimed);
        assert.equal(claimed.id, id);

        // Both operations compete for the same job.
        await Promise.all([
          cancelJob(id),
          completeJob(id, claimed.lease_token, "stage1-race", "succeeded"),
        ]);

        const job = await readJob(id);
        const effects = await effectCount(id);

        // Either operation may win.
        // But the final database state must be consistent.
        assert.ok(job.status === "cancelled" || job.status === "succeeded");

        if (job.status === "cancelled") {
          assert.equal(effects, 0);
        } else {
          assert.equal(effects, 1);
        }

        assert.ok(job.finished_at);
      },
    );

    /**
     * Terminal jobs are ineligible for new claims: a later claimant should
     * receive a newly queued job instead of an already succeeded one.
     */
    // TEST 09
    await t.test("09 - Terminal jobs cannot be claimed again", async () => {
      const completedId = await seedJob();

      const claim = await claimJob("stage1-terminal");
      assert.ok(claim);
      assert.equal(claim.id, completedId);

      assert.equal(
        await completeJob(
          completedId,
          claim.lease_token,
          "stage1-terminal",
          "succeeded",
        ),
        true,
      );

      // Add another queued job.
      const queuedId = await seedJob();

      const nextClaim = await claimJob("stage1-next");
      assert.ok(nextClaim);

      // The succeeded job must not be reclaimed.
      assert.equal(nextClaim.id, queuedId);
      assert.notEqual(nextClaim.id, completedId);

      assert.equal((await readJob(completedId)).status, "succeeded");

      assert.equal(await effectCount(completedId), 1);

      await cancelJob(queuedId);
    });
  /**
   * Remove only this suite's jobs and associated execution records even if
   * a subtest fails, then close the PostgreSQL connection pool.
   */
  } finally {
    // Delete ONLY jobs created by this test suite.
    // Also runs if an assertion fails.
    if (createdJobs.length > 0) {
      await pool.query(
        `DELETE FROM execution_records
           WHERE job_id = ANY($1::uuid[])`,
        [createdJobs],
      );

      await pool.query(
        `DELETE FROM jobs
           WHERE id = ANY($1::uuid[])`,
        [createdJobs],
      );
    }

    await pool.end();
  }
});
