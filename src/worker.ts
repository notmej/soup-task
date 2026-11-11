import "dotenv/config";
import {
  claimJob,
  completeJob,
  recoverExpiredJobs,
  renewLease,
  type ClaimedJob,
} from "./queue";
import { setTimeout as delay } from "node:timers/promises";

const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;

const POLL_INTERVAL_MS = 250;
const HEARTBEAT_INTERVAL_MS = Number(
  process.env.HEARTBEAT_INTERVAL_MS ?? 10_000,
);

if (!Number.isInteger(HEARTBEAT_INTERVAL_MS) || HEARTBEAT_INTERVAL_MS < 100) {
  throw new Error("Invalid HEARTBEAT_INTERVAL_MS");
}
const RECOVERY_INTERVAL_MS = 2_000;

// A reusable asynchronous delay.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function heartbeatLoop(
  job: ClaimedJob,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    try {
      await delay(HEARTBEAT_INTERVAL_MS, undefined, {
        signal,
      });
    } catch (error) {
      if (signal.aborted) return;
      throw error;
    }

    if (signal.aborted) return;

    try {
      const renewed = await renewLease(job.id, job.lease_token, workerId);
      if (!renewed) {
        console.log(`[${workerId}] Lost lease for ${job.id}`);
        return;
      }
      console.log(`[${workerId}] Renewed lease for ${job.id}`);
    } catch (error) {
      console.error(`[${workerId}] Heartbeat error:`, error);
    }
  }
}

async function recoveryLoop(): Promise<void> {
  while (true) {
    try {
      const recovered = await recoverExpiredJobs();
      for (const id of recovered) {
        console.log(`[${workerId}] Requeued expired job ${id}`);
      }
    } catch (error) {
      console.error(`[${workerId}] Recovery error:`, error);
    }

    await sleep(RECOVERY_INTERVAL_MS);
  }
}

async function runWorker(): Promise<void> {
  console.log(`[${workerId}] Worker started`);
  void recoveryLoop();

  while (true) {
    try {
      // Ask PostgreSQL for one available job.
      const job = await claimJob(workerId);

      if (!job) {
        // No jobs currently available.
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      console.log(
        `[${workerId}] Claimed job ${job.id}, attempt ${job.attempts}`,
      );

      const controller = new AbortController();
      const heartbeatTask = heartbeatLoop(job, controller.signal);

      try {
        // Simulate executing the job.
        await sleep(job.duration_ms);
        const result = job.should_fail ? "failed" : "succeeded";
        const completed = await completeJob(
          job.id,
          job.lease_token,
          workerId,
          result,
        );
        if (completed) {
          console.log(`[${workerId}] Job ${job.id}: ${result}`);
        } else {
          console.log(`[${workerId}] Job ${job.id}: stale result rejected`);
        }
      } finally {
        //stop heartbeat aftr job finish
        controller.abort();
        await heartbeatTask;
      }
    } catch (error) {
      console.error(`[${workerId}] Worker error:`, error);
      await sleep(1000);
    }
  }
}

runWorker().catch((error) => {
  console.error(`[${workerId}] Fatal error:`, error);
  process.exit(1);
});
