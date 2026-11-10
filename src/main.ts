import "dotenv/config";
import { createServer } from "node:http";
import {
  getJob,
  listJobs,
  cancelJob,
  createJob,
  JobConflictError,
} from "./jobs";

/**
 * HTTP port used by the API server.
 * Reads PORT from environment variables, defaulting to 3000.
 */
const PORT = Number(process.env.PORT ?? 3000);

/**
 * Regex used to validate the format of job UUIDs.
 * Accepts hexadecimal UUIDs in the standard 8-4-4-4-12 format.
 */
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Creates the HTTP server for the training-job queue API.
 *
 * Routes incoming requests to the corresponding job service functions.
 * Handles request validation, JSON serialization, pagination, and errors.
 *
 * Supported endpoints:
 * - POST /jobs             - Create a new job.
 * - GET /jobs              - List jobs with pagination.
 * - GET /jobs/:id          - Retrieve a job by UUID.
 * - POST /jobs/:id/cancel  - Cancel a queued or running job.
 *
 * @param req - Incoming HTTP request.
 * @param res - Outgoing HTTP response.
 */
const server = createServer(async (req, res) => {
  const url = new URL(
    req.url ?? "/",
    `http://${req.headers.host ?? "localhost"}`,
  );

  /**
   * Sends a JSON response to the client.
   *
   * Sets the HTTP status code and Content-Type header,
   * then serializes the response data as JSON.
   *
   * @param status - HTTP status code.
   * @param data - Data to serialize and return to the client.
   * @returns void
   */
  function respond(status: number, data: unknown) {
    res.writeHead(status, {
      "Content-Type": "application/json",
    });
    res.end(JSON.stringify(data));
  }
  try {
    /**
     * POST /jobs
     *
     * Creates a new training job using an idempotency key.
     *
     * Required header:
     * - Idempotency-Key: Unique client-generated key.
     *
     * Request body:
     * - durationMs: Simulated execution duration (100–10000 ms).
     * - shouldFail: Optional boolean indicating simulated failure.
     *
     * Validates the header, JSON body, and job parameters.
     * Limits the request body to 8192 bytes.
     *
     * Responses:
     * - 201: New job created.
     * - 200: Existing job returned for the same key and parameters.
     * - 400: Invalid header, JSON, or job parameters.
     * - 409: Idempotency key reused with different parameters.
     * - 413: Request body exceeds the size limit.
     * - 500: Unexpected server error.
     */

    // POST / jobs -> Create a job
    if (req.method === "POST" && url.pathname === "/jobs") {
      const key = req.headers["idempotency-key"];

      if (typeof key !== "string" || !key.trim()) {
        respond(400, {
          error: "Idempotency-Key header is required",
        });
        return;
      }

      let body = "";

      for await (const chunk of req) {
        body += chunk;

        if (Buffer.byteLength(body) > 8192) {
          respond(413, { error: "Request body too large" });
          return;
        }
      }

      let data: unknown;

      try {
        data = JSON.parse(body);
      } catch {
        respond(400, { error: "Invalid JSON" });
        return;
      }

      if (typeof data !== "object" || data === null || Array.isArray(data)) {
        respond(400, { error: "Expected a JSON object" });
        return;
      }

      const input = data as Record<string, unknown>;

      if (
        typeof input.durationMs !== "number" ||
        (input.shouldFail !== undefined &&
          typeof input.shouldFail !== "boolean")
      ) {
        respond(400, { error: "Invalid job parameters" });
        return;
      }

      const result = await createJob(key, {
        durationMs: input.durationMs,
        shouldFail: input.shouldFail as boolean | undefined,
      });

      respond(result.created ? 201 : 200, result.job);
      return;
    }

    /**
     * GET /jobs
     *
     * Retrieves a paginated list of jobs.
     *
     * Optional query parameters:
     * - page: Page number, starting at 1 (default: 1).
     * - limit: Number of jobs per page (default: 10, maximum: 100).
     *
     * Ensures pagination values are positive safe integers and
     * that the calculated OFFSET fits within PostgreSQL's limit.
     *
     * Jobs are returned in descending creation-time order.
     *
     * Responses:
     * - 200: Paginated job list and pagination metadata.
     * - 400: Invalid pagination parameters.
     * - 500: Unexpected server error.
     */

    // GET / jobs -> lists with pagination
    if (req.method === "GET" && url.pathname === "/jobs") {
      const pageText = url.searchParams.get("page") ?? "1";
      const limitText = url.searchParams.get("limit") ?? "10";

      const page = Number(pageText);
      const limit = Number(limitText);

      if (
        !Number.isSafeInteger(page) ||
        !Number.isSafeInteger(limit) ||
        page < 1 ||
        limit < 1 ||
        limit > 100 ||
        (page - 1) * limit > 2147483647
      ) {
        respond(400, {
          error: "Invalid pagination parameters",
        });
        return;
      }

      const result = await listJobs(page, limit);
      respond(200, result);
      return;
    }

    /**
     * GET /jobs/:id
     *
     * Retrieves a single job and its current execution status.
     *
     * Path parameters:
     * - id: UUID of the requested job.
     *
     * Validates the UUID format before querying PostgreSQL.
     *
     * Responses:
     * - 200: Job record including its status and metadata.
     * - 400: Invalid UUID format.
     * - 404: Job does not exist.
     * - 500: Unexpected server error.
     */

    // GET /jobs/:id -> gtes status
    const jobMatch = url.pathname.match(/^\/jobs\/([^/]+)$/);
    if (req.method === "GET" && jobMatch) {
      const id = jobMatch[1];

      if (!UUID_REGEX.test(id)) {
        respond(400, { error: "Invalid job ID" });
        return;
      }

      const job = await getJob(id);

      if (!job) {
        respond(404, { error: "Job not found" });
        return;
      }

      respond(200, job);
      return;
    }

    /**
     * POST /jobs/:id/cancel
     *
     * Attempts to cancel an existing queued or running job.
     *
     * Path parameters:
     * - id: UUID of the job to cancel.
     *
     * Cancellation is performed through a conditional database update.
     * Already cancelled jobs return successfully without modification.
     * Jobs that have succeeded or failed cannot be cancelled.
     *
     * Responses:
     * - 200: Job is cancelled.
     * - 400: Invalid UUID format.
     * - 404: Job does not exist.
     * - 409: Job has already succeeded or failed.
     * - 500: Unexpected server error.
     */

    //POST /jobs/:id/cancel - cancels job
    const cancelMatch = url.pathname.match(/^\/jobs\/([^/]+)\/cancel$/);
    if (req.method === "POST" && cancelMatch) {
      const id = cancelMatch[1];

      if (!UUID_REGEX.test(id)) {
        respond(400, { error: "Invalid job ID" });
        return;
      }

      const result = await cancelJob(id);

      if (!result) {
        respond(404, { error: "Job not found" });
        return;
      }

      if (!result.cancelled) {
        respond(409, {
          error: "Job is already in a terminal state",
          status: result.job.status,
        });
        return;
      }

      respond(200, result.job);
      return;
    }
    /**
     * Returns 404 when no registered endpoint matches the request.
     */
    respond(404, { error: "Route not found" });
  } catch (e) {
    /**
     * Handles errors raised during API request processing.
     *
     * Error mapping:
     * - JobConflictError: HTTP 409 Conflict.
     * - Invalid job parameters or idempotency key: HTTP 400 Bad Request.
     * - All other errors: HTTP 500 Internal Server Error.
     *
     * Unexpected errors are logged without exposing their
     * internal details to the client.
     */
    if (e instanceof JobConflictError) {
      respond(409, { error: e.message });
    } else if (
      e instanceof Error &&
      ["invalid job param", "invalid idempotency key"].includes(e.message)
    ) {
      respond(400, { error: e.message });
    } else {
      console.error(e);
      respond(500, { error: "Internal server error" });
    }
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Soup API listening on port ${PORT}`);
});
