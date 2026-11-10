import { randomUUID } from "node:crypto";
import { pool } from "./db";

export interface ClaimedJob {
  id: string;
  duration_ms: number;
  should_fail: boolean;
  attempts: number;
  lease_token: string;
}

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

export type JobResult = "succeeded" | "failed";

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
