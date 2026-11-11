/**
 * SOUP Stage 3 — silent-failure safety / concurrency integration tests.
 *
 * Precondition: PostgreSQL and HTTP API RUNNING, all worker containers STOPPED.
 * This suite creates temporary jobs and deletes only its own jobs afterward.
 * Run: npm run build && node --test dist/tests/silent-failures.test.js
 *
 * This is a controlled concurrency suite, NOT the 10,000-job chaos harness.
 */
/**
 * Loads environment configuration before the PostgreSQL pool and API helpers
 * are imported. The test runs against the configured local services.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { pool } from "../db";
import { createJob, cancelJob } from "../jobs";
import {
  claimJob,
  completeJob,
  recoverExpiredJobs,
  renewLease,
} from "../queue";

/** HTTP API address; removes a trailing slash for consistent URL joining. */
const BASE = (process.env.API_BASE_URL ?? "http://localhost:3000").replace(
  /\/$/,
  "",
);
/** IDs belonging to this suite, used for targeted cleanup after execution. */
const createdIds: string[] = [];
/** Run-specific prefix isolates idempotency keys from other test runs. */
const testPrefix = `stage3-${randomUUID()}`;

/** Selected persisted job fields used to verify state, ownership, and leases. */
interface JobRow {
  id: string;
  status: string;
  attempts: number;
  lease_token: string | null;
  worker_id: string | null;
  lease_expires_at: Date | null;
}

/** Non-null return type of claimJob(), used by the assertion helper. */
type Claim = NonNullable<Awaited<ReturnType<typeof claimJob>>>;

/**
 * Creates a uniquely keyed test job and makes it oldest in FIFO order.
 * Backdating prevents earlier queued development jobs from being claimed first.
 *
 * @param label - Human-readable identifier for this test scenario.
 * @returns The ID of the new job; it is also tracked for cleanup.
 */
async function seed(label: string): Promise<string> {
  const { job } = await createJob(`${testPrefix}-${label}-${randomUUID()}`, {
    durationMs: 1000,
    shouldFail: false,
  });
  const id = job.id as string;
  createdIds.push(id);
  // Guarantee test jobs are the oldest, independent of previous local jobs.
  await pool.query(
    "UPDATE jobs SET created_at = TIMESTAMPTZ '1800-01-01' WHERE id = $1",
    [id],
  );
  return id;
}

/**
 * Reads a job directly from PostgreSQL so assertions verify persisted state,
 * independently of the API's response representation.
 * @param id - Job UUID.
 * @returns The selected database columns for that job.
 */
async function row(id: string): Promise<JobRow> {
  const r = await pool.query<JobRow>(
    "SELECT id, status, attempts, lease_token, worker_id, lease_expires_at FROM jobs WHERE id = $1",
    [id],
  );
  assert.equal(r.rows.length, 1);
  return r.rows[0];
}

/**
 * Counts committed execution_records associated with a job.
 * Used to detect missing or duplicate database side effects.
 * @param id - Job UUID.
 * @returns Number of matching committed records.
 */
async function effects(id: string): Promise<number> {
  const result = await pool.query<{ total: string }>(
    "SELECT COUNT(*)::text AS total FROM execution_records WHERE job_id = $1",
    [id],
  );
  return Number(result.rows[0].total);
}

/**
 * Sends a job-creation HTTP request with the given idempotency key.
 * Enables simultaneous retries and conflicting-payload experiments.
 * @param key - Idempotency key reused across related requests.
 * @param durationMs - Requested simulated execution duration.
 * @returns HTTP status and parsed JSON response.
 */
async function post(key: string, durationMs: number) {
  const response = await fetch(`${BASE}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": key },
    body: JSON.stringify({ durationMs, shouldFail: false }),
  });
  const body: unknown = await response.json();
  return {
    status: response.status,
    body: body as {
      id?: string;
      status?: string;
      attempts?: number;
      error?: string;
    },
  };
}

/**
 * Asserts that a queued job was claimed and narrows the nullable TypeScript type.
 * A failure may indicate incorrect suite isolation or unexpected worker activity.
 */
function assertClaim(value: Claim | null): asserts value is Claim {
  assert.ok(value, "Expected a queued job; make sure all workers are stopped");
}

// Sequential named tests within a single suite so race experiments don't
// compete with each other's staged jobs. Multiple processes/connections are
// exercised *inside* each test, where concurrency is intentional.
test(
  "Stage 3 — silent-failure safety (workers STOPPED)",
  { timeout: 90_000 },
  async (t) => {
    try {
      /**
       * Scenario 01 — Launch 12 claims concurrently. Every seeded job must be
       * claimed exactly once, with one attempt and no committed side effects.
       */
      await t.test(
        "01 - Concurrent workers claim distinct jobs; no double claim",
        async () => {
          const ids = await Promise.all(
            Array.from({ length: 12 }, (_, i) => seed(`claim-${i}`)),
          );
          const claims = await Promise.all(
            Array.from({ length: 12 }, (_, i) =>
              claimJob(`parallel-worker-${i}`),
            ),
          );
          for (const c of claims) assertClaim(c);
          assert.deepEqual(new Set(claims.map((c) => c!.id)), new Set(ids));
          assert.equal(new Set(claims.map((c) => c!.id)).size, 12);
          for (const id of ids) {
            const r = await row(id);
            assert.equal(r.attempts, 1);
            assert.equal(r.status, "running");
          }
          await Promise.all(ids.map((id) => cancelJob(id)));
          for (const id of ids) assert.equal(await effects(id), 0);
        },
      );

      /**
       * Scenario 02 — Expire the original owner's lease, recover and reclaim the
       * job, then prove the old token cannot renew or complete it. Only the new
       * owner may commit a successful effect.
       */
      await t.test(
        "02 - Zombie worker returning after lease expiry is fenced off",
        async () => {
          const id = await seed("zombie");
          const oldClaim = await claimJob("old-worker");
          assertClaim(oldClaim);
          assert.equal(oldClaim.id, id);
          await pool.query(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
            [id],
          );
          assert.equal(
            await renewLease(id, oldClaim.lease_token, "old-worker"),
            false,
          );
          assert.ok((await recoverExpiredJobs()).includes(id));
          const newClaim = await claimJob("new-worker");
          assertClaim(newClaim);
          assert.equal(newClaim.id, id);
          assert.notEqual(oldClaim.lease_token, newClaim.lease_token);
          assert.equal(newClaim.attempts, 2);
          const staleResult = await completeJob(
            id,
            oldClaim.lease_token,
            "old-worker",
            "succeeded",
          );
          assert.equal(staleResult, false);
          assert.equal(await effects(id), 0);
          assert.equal(
            await completeJob(
              id,
              newClaim.lease_token,
              "new-worker",
              "succeeded",
            ),
            true,
          );
          assert.equal((await row(id)).status, "succeeded");
          assert.equal(await effects(id), 1);
        },
      );

      /**
       * Scenario 03 — Repeat the cancellation/completion race 20 times and verify
       * that the final state and effect count agree with whichever action wins.
       */
      await t.test(
        "03 - Concurrent cancellation and completion: exactly one final outcome",
        async () => {
          // Repeat this deliberately overlapping race to catch intermittent bugs.
          for (let i = 0; i < 20; i++) {
            const id = await seed(`race-${i}`);
            const claim = await claimJob(`race-worker-${i}`);
            assertClaim(claim);
            assert.equal(claim.id, id);
            const [cancel, finish] = await Promise.all([
              cancelJob(id),
              completeJob(
                id,
                claim.lease_token,
                `race-worker-${i}`,
                "succeeded",
              ),
            ]);
            assert.ok(cancel, "Cancellation must return a job");
            const final = await row(id);
            const n = await effects(id);
            assert.ok(
              final.status === "cancelled" || final.status === "succeeded",
            );
            if (final.status === "cancelled") {
              assert.equal(n, 0, `Cancelled job ${id} had an effect`);
              assert.equal(finish, false);
              assert.equal(cancel.cancelled, true);
            } else {
              assert.equal(
                n,
                1,
                `Succeeded job ${id} must have exactly one effect`,
              );
              assert.equal(finish, true);
              assert.equal(cancel.cancelled, false);
            }
            assert.equal(final.lease_token, null);
          }
        },
      );

      /**
       * Scenario 04 — Submit identical HTTP requests simultaneously; check that
       * only one job exists. After lease recovery, retry with both matching and
       * conflicting payloads to ensure idempotency cannot reset execution state.
       */
      await t.test(
        "04 - Concurrent POST retries: one job, one key, conflicting payload rejected",
        async () => {
          const key = `${testPrefix}-http-${randomUUID()}`;
          // Deliberately send requests at the same time.
          const responses = await Promise.all(
            Array.from({ length: 20 }, () => post(key, 1000)),
          );
          assert.equal(responses.filter((r) => r.status === 201).length, 1);
          assert.equal(responses.filter((r) => r.status === 200).length, 19);
          const ids = responses.map((r) => r.body.id);
          assert.ok(ids.every((id) => typeof id === "string"));
          assert.equal(new Set(ids).size, 1);
          const id = ids[0]!;
          createdIds.push(id);
          const count = await pool.query<{ total: string }>(
            "SELECT COUNT(*)::text AS total FROM jobs WHERE idempotency_key = $1",
            [key],
          );
          assert.equal(Number(count.rows[0].total), 1);

          // Also retry concurrently AFTER lease expiry and reclaim, to demonstrate
          // POST retries don't reset job ownership, status or attempts.
          await pool.query(
            "UPDATE jobs SET created_at = TIMESTAMPTZ '1800-01-01' WHERE id = $1",
            [id],
          );
          const first = await claimJob("retry-old");
          assertClaim(first);
          assert.equal(first.id, id);
          await pool.query(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
            [id],
          );
          assert.ok((await recoverExpiredJobs()).includes(id));
          const second = await claimJob("retry-new");
          assertClaim(second);
          assert.equal(second.id, id);
          assert.equal(second.attempts, 2);
          const repeated = await Promise.all([
            ...Array.from({ length: 10 }, () => post(key, 1000)),
            ...Array.from({ length: 10 }, () => post(key, 2000)),
          ]);
          assert.equal(repeated.filter((r) => r.status === 200).length, 10);
          assert.equal(repeated.filter((r) => r.status === 409).length, 10);
          assert.ok(
            repeated
              .filter((r) => r.status === 200)
              .every((r) => r.body.id === id),
          );
          assert.equal((await row(id)).attempts, 2);
          assert.equal((await row(id)).status, "running");
          assert.equal(
            await completeJob(id, second.lease_token, "retry-new", "succeeded"),
            true,
          );
          assert.equal(await effects(id), 1);
        },
      );

      /**
       * Scenario 05 — Verify the database's READ COMMITTED isolation setting and
       * visibility of another transaction's committed update. Then hold a row
       * lock to ensure cancellation waits until that lock is released.
       * This tests selected behaviors, not every possible isolation anomaly.
       */
      await t.test(
        "05 - READ COMMITTED behavior and row-lock serialization",
        async () => {
          const isolation = await pool.query<{ transaction_isolation: string }>(
            "SHOW transaction_isolation",
          );
          assert.equal(
            isolation.rows[0].transaction_isolation,
            "read committed",
          );
          const id = await seed("isolation");
          const a = await pool.connect();
          const b = await pool.connect();
          try {
            await a.query("BEGIN ISOLATION LEVEL READ COMMITTED");
            const before = await a.query<{ status: string }>(
              "SELECT status FROM jobs WHERE id = $1",
              [id],
            );
            assert.equal(before.rows[0].status, "queued");
            await b.query("BEGIN");
            await b.query(
              "UPDATE jobs SET status='cancelled', finished_at=clock_timestamp() WHERE id=$1",
              [id],
            );
            await b.query("COMMIT");
            // A READ COMMITTED transaction sees newer committed rows on its NEXT statement.
            const after = await a.query<{ status: string }>(
              "SELECT status FROM jobs WHERE id = $1",
              [id],
            );
            assert.equal(after.rows[0].status, "cancelled");
            await a.query("COMMIT");
          } finally {
            await a.query("ROLLBACK").catch(() => undefined);
            await b.query("ROLLBACK").catch(() => undefined);
            a.release();
            b.release();
          }

          // Holding a row lock must serialize completion/cancellation updates.
          const lockedId = await seed("locking");
          const c = await claimJob("lock-owner");
          assertClaim(c);
          assert.equal(c.id, lockedId);
          const lockClient = await pool.connect();
          try {
            await lockClient.query("BEGIN");
            await lockClient.query(
              "SELECT id FROM jobs WHERE id = $1 FOR UPDATE",
              [lockedId],
            );
            let settled = false;
            const cancelling = cancelJob(lockedId).then((value) => {
              settled = true;
              return value;
            });
            await new Promise((resolve) => setTimeout(resolve, 80));
            assert.equal(
              settled,
              false,
              "Cancellation finished while its row was locked",
            );
            await lockClient.query("COMMIT");
            const result = await cancelling;
            assert.ok(result?.cancelled);
            assert.equal((await row(lockedId)).status, "cancelled");
            assert.equal(
              await completeJob(
                lockedId,
                c.lease_token,
                "lock-owner",
                "succeeded",
              ),
              false,
            );
            assert.equal(await effects(lockedId), 0);
          } finally {
            await lockClient.query("ROLLBACK").catch(() => undefined);
            lockClient.release();
          }
        },
      );

      /**
       * Scenario 06 — Skew Date.now() in this test process by +24 hours and
       * verify that claim/renew still use PostgreSQL timestamps. Then expire the
       * lease in the database and confirm renewal is rejected and recovery works.
       * This does not simulate skew of PostgreSQL's own server clock.
       */
      await t.test(
        "06 - Clock skew: lease decisions use PostgreSQL clock",
        async () => {
          const id = await seed("clock");
          const actualDateNow = Date.now;
          let claim: Claim | null = null;
          try {
            // Artificially skew the *test application's clock* 24h ahead.
            // Database timestamp/expiration checks must be unaffected.
            Date.now = () => actualDateNow() + 24 * 60 * 60 * 1000;
            claim = await claimJob("clock-worker");
            assertClaim(claim);
            assert.equal(claim.id, id);
            assert.equal(
              await renewLease(id, claim.lease_token, "clock-worker"),
              true,
            );
          } finally {
            Date.now = actualDateNow;
          }
          assertClaim(claim);
          const remaining = await pool.query<{ seconds: number }>(
            `SELECT EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))::float8 AS seconds
         FROM jobs WHERE id = $1`,
            [id],
          );
          assert.ok(
            remaining.rows[0].seconds > 20 && remaining.rows[0].seconds <= 31,
          );
          await pool.query(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
            [id],
          );
          assert.equal(
            await renewLease(id, claim.lease_token, "clock-worker"),
            false,
          );
          assert.ok((await recoverExpiredJobs()).includes(id));
          assert.equal((await row(id)).status, "queued");
          await cancelJob(id);
        },
      );
    } finally {
      // Remove only jobs created by this suite, including their effect records.
      // Cleanup runs even if an assertion or subtest fails.
      if (createdIds.length) {
        try {
          await pool.query(
            "DELETE FROM execution_records WHERE job_id = ANY($1::uuid[])",
            [createdIds],
          );
          await pool.query("DELETE FROM jobs WHERE id = ANY($1::uuid[])", [
            createdIds,
          ]);
        } catch (error) {
          console.error(
            "Stage 3 test cleanup failed; inspect leftover stage3-* jobs:",
            error,
          );
        }
      }
      await pool.end();
    }
  },
);
