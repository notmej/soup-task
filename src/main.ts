import "dotenv/config";
import { createServer } from "node:http";
import {
  getJob,
  listJobs,
  cancelJob,
  createJob,
  JobConflictError,
} from "./jobs";

const PORT = Number(process.env.PORT ?? 3000);

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const server = createServer(async (req, res) => {
  const url = new URL(
    req.url ?? "/",
    `http://${req.headers.host ?? "localhost"}`,
  );

  function respond(status: number, data: unknown) {
    res.writeHead(status, {
      "Content-Type": "application/json",
    });
    res.end(JSON.stringify(data));
  }
  try {
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
    respond(404, { error: "Route not found" });
  } catch (e) {
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
