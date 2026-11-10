import { createHash, randomUUID } from "node:crypto";
import { pool } from "./db";

export interface CreateJobInput {
  durationMs: number;
  shouldFail?: boolean;
}
// shouldFail used for worker implement.: test for successful & failed jobs

export class JobConflictError extends Error {}

/**
 * Creates a job
 * @param key
 * @param input
 * @returns
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
 * Retrieves the job from the PostgreSQL database.
 * @param id
 * @returns
 */
export async function getJob(id: string) {
  const result = await pool.query("SELECT * FROM jobs WHERE id = $1", [id]);

  return result.rows[0] ?? null;
}

/**
 * Retrieves only a certail number of jobs at a time, using pagination
 * Jobs are sorted by creation time. The newest jobs have priority with the UUID order
 * determining priority if some jobs were created at the same time.
 * @param page
 * @param limit
 * @returns
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
 * 
 * @param id 
 * @returns 
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
    [id]
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