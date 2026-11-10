/**
 * ---AI GENERATED TESTS---
 * Soup API integration tests (22 checks from the manual test checklist).
 *
 * Requirements:
 *   - API running at http://localhost:3000 (or API_BASE_URL)
 *   - PostgreSQL running and DATABASE_URL set (or present in .env)
 *   - Workers STOPPED: these tests assume queued jobs stay queued
 *
 * Run from your project root:
 *   npm run build
 *   node --test dist/tests/api.test.js
 *
 * Place this file at src/tests/api.test.ts so it compiles with rootDir=src.
 */
import "dotenv/config";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { pool } from "../db";

const BASE_URL = (process.env.API_BASE_URL ?? "http://localhost:3000").replace(
  /\/$/,
  "",
);
const runId = randomUUID();
const key = `api-suite-${runId}`;
const secondKey = `api-suite-page-${runId}`;
const UUID_RE = /^[a-f\d]{8}-([a-f\d]{4}-){3}[a-f\d]{12}$/i;

interface Job {
  id: string;
  idempotency_key: string;
  duration_ms: number;
  should_fail: boolean;
  status: string;
  attempts: number;
  worker_id: string | null;
  lease_token: string | null;
  lease_expires_at: string | null;
  finished_at: string | null;
}

interface PaginationResponse {
  data: Job[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

async function request(path: string, init: RequestInit = {}) {
  const response = await fetch(`${BASE_URL}${path}`, init);
  const text = await response.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      throw new Error(`Non-JSON response for ${path}: ${text.slice(0, 300)}`);
    }
  }
  return { status: response.status, body };
}

function createRequest(idempotencyKey: string, body: string) {
  return request("/jobs", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body,
  });
}

function asJob(body: unknown): Job {
  assert.ok(body && typeof body === "object", "Expected a job object");
  return body as Job;
}

function asPage(body: unknown): PaginationResponse {
  assert.ok(body && typeof body === "object", "Expected pagination response");
  return body as PaginationResponse;
}

// One parent test ensures the dependent checks execute in the checklist order.
// Each subtest is separately reported as passed or failed.
test("Soup API integration checklist (workers stopped)", async (t) => {
  let jobId = "";
  let originalFinishedAt: string | null = null;

  try {
    await t.test("01 - Create a new job: 201 / queued / defaults", async () => {
      const { status, body } = await createRequest(key, '{"durationMs":5000}');
      assert.equal(status, 201);
      const job = asJob(body);
      assert.match(job.id, UUID_RE);
      assert.equal(job.idempotency_key, key);
      assert.equal(job.status, "queued");
      assert.equal(job.duration_ms, 5000);
      assert.equal(job.attempts, 0);
      assert.equal(job.should_fail, false);
      jobId = job.id;
    });

    await t.test("02 - Retrieve existing job by ID", async () => {
      assert.ok(jobId, "Test 1 must pass first");
      const { status, body } = await request(`/jobs/${jobId}`);
      assert.equal(status, 200);
      const job = asJob(body);
      assert.equal(job.id, jobId);
      assert.equal(job.status, "queued");
    });

    await t.test(
      "03 - List first page and verify pagination metadata",
      async () => {
        const { status, body } = await request("/jobs?page=1&limit=5");
        assert.equal(status, 200);
        const { data, pagination } = asPage(body);
        assert.ok(Array.isArray(data));
        assert.ok(data.length <= 5);
        assert.equal(pagination.page, 1);
        assert.equal(pagination.limit, 5);
        const dbCount = await pool.query<{ count: string }>(
          "SELECT COUNT(*)::text AS count FROM jobs",
        );
        const total = Number(dbCount.rows[0].count);
        assert.equal(pagination.total, total);
        assert.equal(pagination.totalPages, Math.ceil(total / 5));
      },
    );

    await t.test("04 - Two pages at limit=1 have different IDs", async () => {
      // Guarantee >=2 jobs without relying on prior test runs.
      const inserted = await createRequest(secondKey, '{"durationMs":5000}');
      assert.equal(inserted.status, 201);
      const [first, second] = await Promise.all([
        request("/jobs?page=1&limit=1"),
        request("/jobs?page=2&limit=1"),
      ]);
      assert.equal(first.status, 200);
      assert.equal(second.status, 200);
      const page1 = asPage(first.body);
      const page2 = asPage(second.body);
      assert.equal(page1.data.length, 1);
      assert.equal(page2.data.length, 1);
      assert.notEqual(page1.data[0].id, page2.data[0].id);
    });

    await t.test(
      "05 - Same key and payload: 200 / same UUID / no extra row",
      async () => {
        const { status, body } = await createRequest(
          key,
          '{"durationMs":5000}',
        );
        assert.equal(status, 200);
        assert.equal(asJob(body).id, jobId);
        const result = await pool.query<{ count: string }>(
          "SELECT COUNT(*)::text AS count FROM jobs WHERE idempotency_key = $1",
          [key],
        );
        assert.equal(Number(result.rows[0].count), 1);
      },
    );

    await t.test("06 - Same key, different payload: 409", async () => {
      const { status, body } = await createRequest(key, '{"durationMs":7000}');
      assert.equal(status, 409);
      assert.match(JSON.stringify(body), /idempotency|different param/i);
    });

    await t.test("07 - Cancel queued job and clear lease fields", async () => {
      const { status, body } = await request(`/jobs/${jobId}/cancel`, {
        method: "POST",
      });
      assert.equal(status, 200);
      const job = asJob(body);
      assert.equal(job.id, jobId);
      assert.equal(job.status, "cancelled");
      assert.notEqual(job.finished_at, null);
      assert.equal(job.worker_id, null);
      assert.equal(job.lease_token, null);
      assert.equal(job.lease_expires_at, null);
      assert.equal(job.attempts, 0);
      originalFinishedAt = job.finished_at;
    });

    await t.test(
      "08 - Repeated cancellation: still cancelled (200)",
      async () => {
        const { status, body } = await request(`/jobs/${jobId}/cancel`, {
          method: "POST",
        });
        assert.equal(status, 200);
        const job = asJob(body);
        assert.equal(job.status, "cancelled");
        assert.equal(job.finished_at, originalFinishedAt);
      },
    );

    await t.test("09 - GET after cancellation remains cancelled", async () => {
      const { status, body } = await request(`/jobs/${jobId}`);
      assert.equal(status, 200);
      assert.equal(asJob(body).status, "cancelled");
    });

    await t.test(
      "10 - Duplicate create does not reactivate cancelled job",
      async () => {
        const { status, body } = await createRequest(
          key,
          '{"durationMs":5000}',
        );
        assert.equal(status, 200);
        const job = asJob(body);
        assert.equal(job.id, jobId);
        assert.equal(job.status, "cancelled");
      },
    );

    await t.test("11 - Missing Idempotency-Key: 400", async () => {
      const { status } = await request("/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: '{"durationMs":5000}',
      });
      assert.equal(status, 400);
    });

    await t.test("12 - Malformed JSON: 400", async () => {
      const { status } = await createRequest(
        `bad-json-${runId}`,
        '{"durationMs":',
      );
      assert.equal(status, 400);
    });

    await t.test("13 - Out-of-range duration: 400", async () => {
      const { status } = await createRequest(
        `bad-duration-${runId}`,
        '{"durationMs":50}',
      );
      assert.equal(status, 400);
    });

    await t.test("14 - Invalid shouldFail type: 400", async () => {
      const { status } = await createRequest(
        `bad-shouldfail-${runId}`,
        '{"durationMs":5000,"shouldFail":"yes"}',
      );
      assert.equal(status, 400);
    });

    await t.test("15 - Invalid page and limit values: 400 each", async () => {
      const [badPage, badLimit] = await Promise.all([
        request("/jobs?page=-1&limit=5"),
        request("/jobs?page=1&limit=101"),
      ]);
      assert.equal(badPage.status, 400);
      assert.equal(badLimit.status, 400);
    });

    await t.test("16 - Malformed UUID: 400", async () => {
      const { status } = await request("/jobs/not-a-uuid");
      assert.equal(status, 400);
    });

    await t.test("17 - Missing job with valid UUID: 404", async () => {
      const { status } = await request(
        "/jobs/00000000-0000-0000-0000-000000000000",
      );
      assert.equal(status, 404);
    });

    await t.test("18 - Cancel nonexistent job: 404", async () => {
      const { status } = await request(
        "/jobs/00000000-0000-0000-0000-000000000000/cancel",
        {
          method: "POST",
        },
      );
      assert.equal(status, 404);
    });

    await t.test("19 - Unknown endpoint: 404", async () => {
      const { status } = await request("/unknown");
      assert.equal(status, 404);
    });

    await t.test("20 - Database persists the cancelled status", async () => {
      const result = await pool.query<{
        id: string;
        status: string;
        attempts: number;
      }>("SELECT id, status, attempts FROM jobs WHERE id = $1", [jobId]);
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].status, "cancelled");
      assert.equal(result.rows[0].attempts, 0);
    });

    await t.test(
      "21 - Database has exactly one row for idempotency key",
      async () => {
        const result = await pool.query<{ count: string }>(
          "SELECT COUNT(*)::text AS count FROM jobs WHERE idempotency_key = $1",
          [key],
        );
        assert.equal(Number(result.rows[0].count), 1);
      },
    );

    await t.test("22 - Database defaults for status and attempts", async () => {
      const result = await pool.query<{
        column_name: string;
        column_default: string | null;
      }>(
        `SELECT column_name, column_default
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'jobs'
           AND column_name IN ('status', 'attempts')`,
      );
      const defaults = Object.fromEntries(
        result.rows.map((row) => [row.column_name, row.column_default]),
      );
      assert.ok(defaults.status, "Expected status default");
      assert.match(defaults.status, /queued/i);
      assert.ok(defaults.attempts, "Expected attempts default");
      assert.match(defaults.attempts, /\b0\b/);
    });
  } finally {
    await pool.end();
  }
});
