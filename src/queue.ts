/**
 * PostgreSQL-backed job queue operations.
 *
 * This module implements atomic claiming, lease-based ownership, completion,
 * expired-lease recovery, and lease renewal. Workers call these operations to
 * coordinate through PostgreSQL rather than communicating with one another.
 *
 * A lease token identifies an individual claim attempt. Ownership-sensitive
 * operations require the matching job, worker, and token, as well as a lease
 * that has not expired according to the database clock.
 */
import { randomUUID } from "node:crypto";
import { pool } from "./db";

/** The job details returned to a worker after a successful claim. */
export interface ClaimedJob {
  id: string;
  duration_ms: number;
  should_fail: boolean;
  attempts: number;
  lease_token: string;
}

/**
 * Atomically claim the oldest available queued job for a worker.
 *
 * `FOR UPDATE SKIP LOCKED` skips rows locked by concurrent claimers. The
 * selected row is updated in the same SQL statement, assigning ownership,
 * incrementing the attempt counter, and starting a 30-second lease.
 *
 * @param workerId Identifier of the worker requesting work.
 * @returns The claimed job, or `null` if no unlocked queued job is available.
 */
//claim one avail job atomcly
export async function claimJob(workerId: string): Promise<ClaimedJob | null> {
  const leaseToken = randomUUID();

  const result = await pool.query<ClaimedJob>(
    `
    WITH next_job AS (
      SELECT id
      FROM jobs
      WHERE status = 'queued'
      ORDER BY created_at, id
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE jobs AS j
    SET
      status = 'running',
      worker_id = $1,
      lease_token = $2,
      lease_expires_at = clock_timestamp() + INTERVAL '30 seconds',
      attempts = attempts + 1,
      updated_at = clock_timestamp()
    FROM next_job
    WHERE j.id = next_job.id
    RETURNING
      j.id,
      j.duration_ms,
      j.should_fail,
      j.attempts,
      j.lease_token
    `,
    [workerId, leaseToken],
  );

  return result.rows[0] ?? null;
}

/** Terminal execution outcomes reported by a worker. */
export type JobResult = "succeeded" | "failed";

/**
 * Commit a worker's result only while it still owns a valid lease.
 *
 * The transaction first locks and checks the running job. A successful
 * result inserts an execution record, then updates the job to its terminal
 * state and clears lease ownership. These changes commit together: a failed
 * transaction cannot leave only the job update or only the execution record.
 *
 * A failed result does not create an execution record. Database uniqueness
 * constraints provide an additional safeguard against duplicate committed
 * records; this operation does not guarantee exactly-once external effects.
 *
 * @param jobId Job to complete.
 * @param leaseToken Token issued for the worker's claim attempt.
 * @param workerId Identifier of the claiming worker.
 * @param result Terminal outcome (`succeeded` or `failed`).
 * @returns `true` if committed; `false` if ownership or lease validity fails.
 * @throws If a database operation fails; the transaction is rolled back.
 */
export async function completeJob(
  jobId: string,
  leaseToken: string,
  workerId: string,
  result: JobResult,
): Promise<boolean> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Lock the job and verify ownership.
    const ownership = await client.query<{
      id: string;
      attempts: number;
    }>(
      `
      SELECT id, attempts
      FROM jobs
      WHERE id = $1
        AND status = 'running'
        AND lease_token = $2
        AND worker_id = $3
        AND lease_expires_at > clock_timestamp()
      FOR UPDATE
      `,
      [jobId, leaseToken, workerId],
    );

    const job = ownership.rows[0];

    if (!job) {
      await client.query("ROLLBACK");
      return false;
    }

    // A successful execution creates one committed side effect.
    if (result === "succeeded") {
      await client.query(
        `
        INSERT INTO execution_records (
          id,
          job_id,
          worker_id,
          claim_attempt
        )
        VALUES ($1, $2, $3, $4)
        `,
        [randomUUID(), jobId, workerId, job.attempts],
      );
    }

    // Finish the job and invalidate the lease.
    await client.query(
      `
      UPDATE jobs
      SET
        status = $2,
        worker_id = NULL,
        lease_token = NULL,
        lease_expires_at = NULL,
        finished_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id = $1
      `,
      [jobId, result],
    );

    await client.query("COMMIT");
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Requeue running jobs whose leases have expired.
 *
 * Each invocation processes at most `limit` unlocked expired rows. The
 * recovery update clears the old owner and token so another worker can make
 * a fresh claim; it deliberately preserves the accumulated attempt count.
 * `SKIP LOCKED` allows recovery to coexist with concurrent job operations.
 *
 * @param limit Maximum number of expired jobs to recover (default: 100).
 * @returns IDs of jobs moved from `running` back to `queued`.
 */
export async function recoverExpiredJobs(
  limit: number = 100,
): Promise<string[]> {
  const result = await pool.query<{ id: string }>(
    `
    WITH expired_jobs AS (
      SELECT id
      FROM jobs
      WHERE status = 'running'
        AND lease_expires_at <= clock_timestamp()
      ORDER BY lease_expires_at, id
      FOR UPDATE SKIP LOCKED
      LIMIT $1
    )
    UPDATE jobs AS j
    SET
      status = 'queued',
      worker_id = NULL,
      lease_token = NULL,
      lease_expires_at = NULL,
      updated_at = clock_timestamp(),
      finished_at = NULL
    FROM expired_jobs
    WHERE j.id = expired_jobs.id
    RETURNING j.id
    `,
    [limit],
  );

  return result.rows.map((row) => row.id);
}

/**
 * Extend the current worker's lease for another 30 seconds.
 *
 * The conditional update succeeds only for a running job with the matching
 * worker ID and lease token whose lease has not already expired. PostgreSQL's
 * `clock_timestamp()` is used for both the validity check and new expiration.
 * A stale worker receives `false` instead of regaining ownership.
 *
 * @param jobId Job whose lease should be renewed.
 * @param leaseToken Token belonging to the current claim attempt.
 * @param workerId Worker that currently owns the claim.
 * @returns `true` if exactly one job was renewed; otherwise `false`.
 */
export async function renewLease(
  jobId: string,
  leaseToken: string,
  workerId: string,
): Promise<boolean> {
  const result = await pool.query(
    `
    UPDATE jobs
    SET
      lease_expires_at =
        clock_timestamp() + INTERVAL '30 seconds',
      updated_at = clock_timestamp()
    WHERE id = $1
      AND status = 'running'
      AND lease_token = $2
      AND worker_id = $3
      AND lease_expires_at > clock_timestamp()
    RETURNING id
    `,
    [jobId, leaseToken, workerId],
  );

  return result.rows.length === 1;
}
