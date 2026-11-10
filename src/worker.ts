import "dotenv/config";
import { claimJob, completeJob } from "./queue";

const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;

const POLL_INTERVAL_MS = 250;

// A reusable asynchronous delay.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runWorker(): Promise<void> {
  console.log(`[${workerId}] Worker started`);

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

      // Simulate doing work.
      await sleep(job.duration_ms);

      const result = job.should_fail ? "failed" : "succeeded";

      // Commit the result only if we still own the job.
      const completed = await completeJob(
        job.id,
        job.lease_token,
        workerId,
        result,
      );

      if (completed) {
        console.log(`[${workerId}] Job ${job.id}: ${result}`);
      } else {
        console.log(`[${workerId}] Job ${job.id}: result rejected`);
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
