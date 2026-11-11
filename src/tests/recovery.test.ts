/**
 * Stage 2 — deterministic database recovery tests.
 * Requirements: PostgreSQL running; ALL workers STOPPED.
 * Run: npm run build && node --test dist/tests/recovery.test.js
 *
 * These tests verify queue functions, not background worker heartbeat timing.
 * Each test job is backdated so FIFO claimJob() chooses it even if there are
 * older unclaimed jobs from previous tests. Only test-created jobs are deleted.
 *
 * Test strategy:
 * - Call queue and job-management functions directly against PostgreSQL.
 * - Deliberately manipulate lease expiry to test recovery deterministically,
 *   without waiting for an actual 30-second lease to expire.
 * - Check persistent database state and execution-record counts, rather than
 *   assuming a successful function return proves the expected side effect.
 * - Keep background workers stopped so they do not claim these test jobs.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { pool } from "../db";
import { createJob, cancelJob } from "../jobs";
import {
  claimJob,
  renewLease,
  recoverExpiredJobs,
  completeJob,
} from "../queue";

// Tracks only the jobs created by this suite so the finally block can clean up
// without deleting application data or jobs belonging to another test suite.
const created: string[] = [];

/**
 * Create a unique test job and make it oldest in the FIFO claiming order.
 * Backdating provides predictable selection by claimJob() in the presence of
 * unrelated queued jobs; this helper does not execute or claim the job.
 *
 * @param durationMs Simulated execution duration in milliseconds.
 * @returns Database UUID of the newly created test job.
 */
async function seed(durationMs = 1000): Promise<string> {
  const { job } = await createJob(`stage2-${randomUUID()}`, {
    durationMs,
    shouldFail: false,
  });
  created.push(job.id);
  // Important: make our test jobs the oldest to ensure claimJob() picks them.
  await pool.query(
    `UPDATE jobs SET created_at = TIMESTAMPTZ '1900-01-01' WHERE id = $1`,
    [job.id],
  );
  return job.id as string;
}

/** Database fields inspected to verify lease ownership and job status. */
type DbJob = {
  status: string;
  attempts: number;
  worker_id: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
};

/**
 * Read the persisted state of a single job, failing if the job is missing.
 * @param id UUID of the job to inspect.
 * @returns The selected database status, attempt, and lease fields.
 */
async function readJob(id: string): Promise<DbJob> {
  const r = await pool.query<DbJob>(
    `SELECT status, attempts, worker_id, lease_token, lease_expires_at
     FROM jobs WHERE id = $1`,
    [id],
  );
  assert.equal(r.rows.length, 1);
  return r.rows[0];
}

/**
 * Count committed execution records for a job. This measures persisted
 * database effects, not how many times a worker performed simulated execution.
 * @param id UUID of the job to inspect.
 * @returns Number of execution_records rows for the job.
 */
async function effectCount(id: string): Promise<number> {
  const r = await pool.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM execution_records WHERE job_id = $1`,
    [id],
  );
  return Number(r.rows[0].n);
}

// A single parent test runs dependent checks in order, preserving the initial
// claim, expiry, retry, and final-completion sequence across subtests.
test("Stage 2 — lease, recovery, retry, and stale-result invariants (workers STOPPED)", async (t) => {
  try {
    const id = await seed();
    let token1 = "";
    let token2 = "";

    // 01: Initial claim must establish ownership, advance attempts, and issue
    // an approximately 30-second lease for the claimed job.
    await t.test(
      "01 - Claim gives 30-second lease, owner, token, attempt 1",
      async () => {
        const claimed = await claimJob("stage2-worker-A");
        assert.ok(claimed, "No job claimed; workers may still be running");
        assert.equal(
          claimed.id,
          id,
          "An older queued job was claimed; stop workers and verify test isolation",
        );
        token1 = claimed.lease_token;
        assert.ok(token1);
        assert.equal(claimed.attempts, 1);
        const row = await readJob(id);
        assert.equal(row.status, "running");
        assert.equal(row.worker_id, "stage2-worker-A");
        assert.equal(row.lease_token, token1);
        assert.ok(row.lease_expires_at instanceof Date);
        const remaining = row.lease_expires_at.getTime() - Date.now();
        assert.ok(
          remaining > 15_000 && remaining <= 35_000,
          `Lease not near 30 seconds: ${remaining}ms`,
        );
      },
    );

    // 02: Simulate a lease nearing expiry and verify that the legitimate
    // worker can renew it to a later database timestamp.
    await t.test(
      "02 - Correct owner can renew and extend the lease",
      async () => {
        await pool.query(
          `UPDATE jobs SET lease_expires_at = clock_timestamp() + INTERVAL '5 seconds' WHERE id = $1`,
          [id],
        );
        assert.equal(await renewLease(id, token1, "stage2-worker-A"), true);
        const row = await readJob(id);
        assert.ok(row.lease_expires_at);
        assert.ok(row.lease_expires_at.getTime() - Date.now() > 20_000);
      },
    );

    // 03: Both worker identity and lease token must match before renewal.
    await t.test("03 - Wrong worker ID / token cannot renew", async () => {
      assert.equal(await renewLease(id, token1, "intruder"), false);
      assert.equal(
        await renewLease(id, randomUUID(), "stage2-worker-A"),
        false,
      );
    });

    // 04: Once lease expiry is forced into the past, the original owner must
    // be unable either to renew ownership or commit a successful effect.
    await t.test("04 - Expired owner cannot renew or complete", async () => {
      await pool.query(
        `UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1`,
        [id],
      );
      assert.equal(await renewLease(id, token1, "stage2-worker-A"), false);
      assert.equal(
        await completeJob(id, token1, "stage2-worker-A", "succeeded"),
        false,
      );
      assert.equal(await effectCount(id), 0);
    });

    // 05: Recovery should move the expired job back into the queue, clear
    // stale ownership, and preserve its prior attempt count.
    await t.test(
      "05 - Expired running job requeues and keeps attempt count",
      async () => {
        const recovered = await recoverExpiredJobs();
        assert.ok(recovered.includes(id));
        const row = await readJob(id);
        assert.equal(row.status, "queued");
        assert.equal(row.attempts, 1);
        assert.equal(row.worker_id, null);
        assert.equal(row.lease_token, null);
        assert.equal(row.lease_expires_at, null);
      },
    );

    // 06: A different worker can reclaim the job, but must receive a new
    // fencing token so the first worker cannot act as the current owner.
    await t.test("06 - Retry gets NEW token and attempt 2", async () => {
      const claimed = await claimJob("stage2-worker-B");
      assert.ok(claimed);
      assert.equal(claimed.id, id);
      assert.equal(claimed.attempts, 2);
      assert.notEqual(claimed.lease_token, token1);
      token2 = claimed.lease_token;
    });

    // 07: The expired first worker acts as a zombie: even if it finishes its
    // work, its old token must not change the job or create an effect.
    await t.test(
      "07 - Zombie worker's stale completion cannot commit",
      async () => {
        assert.equal(
          await completeJob(id, token1, "stage2-worker-A", "succeeded"),
          false,
        );
        assert.equal((await readJob(id)).status, "running");
        assert.equal(await effectCount(id), 0);
      },
    );

    // 08: The valid second owner should commit success and exactly one
    // execution record within the completion transaction.
    await t.test(
      "08 - New owner completes with one committed side effect",
      async () => {
        assert.equal(
          await completeJob(id, token2, "stage2-worker-B", "succeeded"),
          true,
        );
        assert.equal((await readJob(id)).status, "succeeded");
        assert.equal(await effectCount(id), 1);
      },
    );

    // 09: Repeated completion requests, from either the current or stale
    // claimant, must not create additional committed effects.
    await t.test(
      "09 - Repeated completion is refused and adds no effect",
      async () => {
        assert.equal(
          await completeJob(id, token2, "stage2-worker-B", "succeeded"),
          false,
        );
        assert.equal(
          await completeJob(id, token1, "stage2-worker-A", "succeeded"),
          false,
        );
        assert.equal(await effectCount(id), 1);
      },
    );

    // 10: Recovery must not reopen terminal jobs, even when it scans for
    // abandoned running leases.
    await t.test(
      "10 - Completed job cannot be requeued by recovery",
      async () => {
        assert.ok(!(await recoverExpiredJobs()).includes(id));
        assert.equal((await readJob(id)).status, "succeeded");
      },
    );

    // 11: A currently valid lease must be left untouched by recovery.
    await t.test(
      "11 - Unexpired running lease must NOT be recovered",
      async () => {
        const activeId = await seed();
        const claim = await claimJob("stage2-worker-C");
        assert.ok(claim);
        assert.equal(claim.id, activeId);
        assert.ok(!(await recoverExpiredJobs()).includes(activeId));
        assert.equal((await readJob(activeId)).status, "running");
        await cancelJob(activeId);
      },
    );

    // 12: Cancellation must be terminal and invalidate subsequent stale
    // completion attempts, with no successful execution record inserted.
    await t.test(
      "12 - Cancelled running job cannot be recovered or completed",
      async () => {
        const cancelId = await seed();
        const claim = await claimJob("stage2-worker-D");
        assert.ok(claim);
        assert.equal(claim.id, cancelId);
        const response = await cancelJob(cancelId);
        assert.ok(response?.cancelled);
        assert.equal((await readJob(cancelId)).status, "cancelled");
        assert.equal(
          await completeJob(
            cancelId,
            claim.lease_token,
            "stage2-worker-D",
            "succeeded",
          ),
          false,
        );
        assert.ok(!(await recoverExpiredJobs()).includes(cancelId));
        assert.equal(await effectCount(cancelId), 0);
      },
    );

    // 13: Simultaneous claim attempts should select distinct queued rows,
    // demonstrating the intended behavior of SKIP LOCKED claiming.
    await t.test(
      "13 - Concurrent claims select different queued jobs",
      async () => {
        const first = await seed();
        const second = await seed();
        const [a, b] = await Promise.all([
          claimJob("stage2-parallel-1"),
          claimJob("stage2-parallel-2"),
        ]);
        assert.ok(a && b);
        assert.notEqual(a.id, b.id);
        assert.deepEqual(new Set([a.id, b.id]), new Set([first, second]));
        await cancelJob(first);
        await cancelJob(second);
      },
    );

    // 14: Directly challenge the database uniqueness constraint. PostgreSQL
    // error 23505 indicates that a second record for this job was rejected.
    await t.test(
      "14 - Database rejects a second execution record for the same job",
      async () => {
        await assert.rejects(
          pool.query(
            `INSERT INTO execution_records (id, job_id, worker_id, claim_attempt)
           VALUES ($1, $2, 'duplicate-worker', 99)`,
            [randomUUID(), id],
          ),
          (error: unknown) => {
            const pg = error as { code?: string };
            return pg.code === "23505";
          },
        );
        assert.equal(await effectCount(id), 1);
      },
    );
  } finally {
    // Cleanup only the rows created by THIS suite, even if a subtest fails.
    // Avoid leaving running or queued test jobs behind.
    if (created.length) {
      await pool.query(
        `DELETE FROM execution_records WHERE job_id = ANY($1::uuid[])`,
        [created],
      );
      await pool.query(`DELETE FROM jobs WHERE id = ANY($1::uuid[])`, [
        created,
      ]);
    }
    await pool.end();
  }
});
