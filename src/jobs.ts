import { createHash, randomUUID } from "node:crypto";
import { pool } from "./db";

/**
 * Defines the parameters required to create a job.
 *
 * @property durationMs - Simulated execution duration in milliseconds (100–10000).
 * @property shouldFail - Whether the worker should simulate a failure.
 *                        Defaults to false.
 */
export interface CreateJobInput {
  durationMs: number;
  shouldFail?: boolean;
}
// shouldFail used for worker implement.: test for successful & failed jobs

/**
 * Error thrown when an idempotency key is reused
 * with different job parameters.
 */
export class JobConflictError extends Error {}

/**
 * Creates a new job in the PostgreSQL database.
 *
 * Validates the input, generates a SHA-256 fingerprint of the
 * normalized job parameters, and attempts to insert a new job.
 *
 * Idempotency behavior:
 * - New key: creates and returns a new job.
 * - Existing key with identical parameters: returns the original job.
 * - Existing key with different parameters: throws JobConflictError.
 *
 * The UNIQUE constraint on idempotency_key prevents duplicate jobs
 * from being inserted by concurrent requests.
 *
 * @param key - Unique idempotency key supplied by the client (max 255 characters).
 * @param input - Job parameters containing durationMs and optional shouldFail.
 * @returns The job record and a created flag indicating whether it was newly inserted.
 * @throws Error If the input or idempotency key is invalid.
 * @throws JobConflictError If the key already exists with different parameters.
 */
export async function createJob(key: string, input: CreateJobInput) {
  // input validation
  if (
    !Number.isInteger(input.durationMs) ||
    input.durationMs < 100 ||
    input.durationMs > 10000 ||
    (input.shouldFail !== undefined && typeof input.shouldFail !== "boolean")
  ) {
    throw new Error("invalid job param");
  }

  if (!key || key.length > 255) {
    throw new Error("invalid idempotency key");
  }

  //normalizing req
  const shouldFail = input.shouldFail ?? false;

  //gen a fingerprint of job param
  const fingerprint = createHash("sha256")
    .update(
      JSON.stringify({
        durationMs: input.durationMs,
        shouldFail,
      }),
    )
    .digest("hex");

  // try insert new job
  // when same idemp key submitted again, postgreSQL != insert anthr job
  const inserted = await pool.query(
    `
    INSERT INTO jobs (id, idempotency_key, request_fingerprint, duration_ms, should_fail)
    VALUES ($1, $2, $3, $4, $5) ON CONFLICT (idempotency_key) DO NOTHING 
    RETURNING *
    `,
    [randomUUID(), key, fingerprint, input.durationMs, shouldFail],
  );

  if (inserted.rows.length > 0) {
    return {
      job: inserted.rows[0],
      created: true,
    };
  }

  //Key exists -> retrieve original job
  const existing = await pool.query(
    "SELECT * FROM jobs WHERE idempotency_key = $1",
    [key],
  );

  const job = existing.rows[0];

  if (!job) {
    throw new Error("couldn't retrieve existing job");
  }

  //rejct same key w/ diff param
  if (job.request_fingerprint !== fingerprint) {
    throw new JobConflictError(
      "idempotency key already used with different param",
    );
  }

  //retrn orig. job w/out making anthr
  return {
    job,
    created: false,
  };

  //
  //---- SAMPLE INPUT ----
  //curl -i -X POST http://localhost:3000/jobs \
  //   -H "Content-Type: application/json" \
  //   -H "Idempotency-Key: test-001" \
  //   -d '{"durationMs":5000}'
  //--- SAMPLE OUTPUT ----
  // {
  //
  // "id":"76a36a3b-a7ac-4c64-ba7b-7b1f0a35547e",
  // "idempotency_key":"test-001",
  // "request_fingerprint":"a2f1e2a7602cbab8401a65bedec9d504bdd3f8cc518bff1875adf5b3ea5512ab",
  // "duration_ms":5000,
  // "should_fail":false,
  //
  // "status":"queued",
  // "worker_id":null,
  // "lease_token":null,
  // "lease_expires_at":null,
  // "attempts":0,
  // "created_at":"2026-11-10T19:01:54.126Z",
  // "updated_at":"2026-11-10T19:01:54.126Z",
  // "finished_at":null
  // }
  // --------------------
}

/**
 * Retrieves a job from the PostgreSQL database using its UUID.
 *
 * Does not modify the job or its execution state.
 *
 * @param id - UUID of the job to retrieve.
 * @returns The job record if found, otherwise null.
 */
export async function getJob(id: string) {
  const result = await pool.query("SELECT * FROM jobs WHERE id = $1", [id]);

  return result.rows[0] ?? null;
}

/**
 * Retrieves a paginated list of jobs from the database.
 *
 * Jobs are ordered by creation time (newest first).
 * UUIDs are used as a secondary sorting criterion when multiple
 * jobs have the same creation timestamp.
 *
 * Uses LIMIT and OFFSET for pagination and a separate COUNT query
 * to determine the total number of jobs.
 *
 * @param page - Page number, starting at 1. Defaults to 1.
 * @param limit - Maximum number of jobs per page. Defaults to 10.
 * @returns An object containing:
 *          - data: Array of job records for the requested page.
 *          - pagination.page: Current page number.
 *          - pagination.limit: Maximum jobs per page.
 *          - pagination.total: Total number of jobs.
 *          - pagination.totalPages: Total number of available pages.
 */
export async function listJobs(page: number = 1, limit: number = 10) {
  const offset = (page - 1) * limit;

  //retrieve jobs for the req pg
  const result = await pool.query(
    `
    SELECT *
    FROM jobs
    ORDER BY created_at DESC, id DESC
    LIMIT $1 OFFSET $2
    `,
    [limit, offset],
  );

  //count tot jobs
  const countResult = await pool.query("SELECT COUNT(*) FROM jobs");

  const total = Number(countResult.rows[0].count);

  return {
    data: result.rows,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
    },
  };
}

/**
 * Attempts to cancel a job by its UUID.
 *
 * Only jobs with status 'queued' or 'running' can be cancelled.
 * Cancellation is performed using a conditional SQL UPDATE,
 * allowing PostgreSQL to handle concurrent status changes atomically.
 *
 * When cancellation succeeds:
 * - Changes status to 'cancelled'.
 * - Clears worker ownership and lease information.
 * - Updates updated_at and finished_at timestamps.
 *
 * Jobs that have already succeeded or failed are not modified.
 * Repeated cancellation of an already cancelled job is treated
 * as successful (idempotent cancellation).
 *
 * @param id - UUID of the job to cancel.
 * @returns An object containing the job and a cancelled flag:
 *          - cancelled: true if the job is now cancelled.
 *          - cancelled: false if the job has another terminal status.
 *          Returns null if the job does not exist.
 */
export async function cancelJob(id: string) {
  const result = await pool.query(
    `
    UPDATE jobs
    SET
      status = 'cancelled',
      worker_id = NULL,
      lease_token = NULL,
      lease_expires_at = NULL,
      updated_at = clock_timestamp(),
      finished_at = clock_timestamp()
    WHERE id = $1
      AND status IN ('queued', 'running')
    RETURNING *
    `,
    [id],
  );

  if (result.rows.length > 0) {
    return {
      job: result.rows[0],
      cancelled: true,
    };
  }

  //chck if job exists
  const existing = await getJob(id);

  if (!existing) {
    return null;
  }

  return {
    job: existing,
    cancelled: existing.status === "cancelled",
  };
}
