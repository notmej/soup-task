/**
 * Background worker process for the PostgreSQL-backed job queue.
 *
 * Each process has its own worker ID and continuously claims queued jobs,
 * simulates their execution, and attempts to commit their final state.
 * A separate recovery loop requeues jobs whose leases have expired.
 *
 * Lease ownership and terminal-state correctness are enforced by the queue
 * functions, not by this process's in-memory state. Run multiple independent
 * worker processes to process jobs concurrently.
 */
import "dotenv/config";
import {
  claimJob,
  completeJob,
  recoverExpiredJobs,
  renewLease,
  type ClaimedJob,
} from "./queue";
import { setTimeout as delay } from "node:timers/promises";

/**
 * Identifies this process when claiming, renewing, and completing jobs.
 * WORKER_ID may be supplied by the container configuration; otherwise the
 * process ID provides a default identifier.
 */
const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;

/** Time between claim attempts when the queue has no available jobs. */
const POLL_INTERVAL_MS = 250;
/**
 * Time between lease-renewal attempts while processing a claimed job.
 * The default is 10 seconds, shorter than the 30-second lease in queue.ts.
 */
const HEARTBEAT_INTERVAL_MS = Number(
  process.env.HEARTBEAT_INTERVAL_MS ?? 10_000,
);

/** Reject heartbeat intervals that are invalid or excessively short. */
if (!Number.isInteger(HEARTBEAT_INTERVAL_MS) || HEARTBEAT_INTERVAL_MS < 100) {
  throw new Error("Invalid HEARTBEAT_INTERVAL_MS");
}
/** Time between scans for running jobs with expired leases. */
const RECOVERY_INTERVAL_MS = 2_000;

/**
 * Waits for a specified duration without blocking the event loop.
 * @param ms - Delay in milliseconds.
 * @returns A promise that resolves after the delay.
 */
// A reusable asynchronous delay.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Periodically renews the lease for an active job until stopped.
 *
 * The AbortSignal allows execution cleanup to interrupt the timer when the
 * job finishes. If renewal is rejected, this worker no longer owns the job
 * and stops sending heartbeats. Transient renewal errors are logged; later
 * iterations may retry unless the signal is aborted. A separate ownership
 * check in completeJob() prevents stale results from being committed.
 *
 * @param job - The claimed job, including its unique lease token.
 * @param signal - Cancellation signal for the heartbeat timer.
 */
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

/**
 * Continuously scans for expired leases and requeues recoverable jobs.
 *
 * This loop operates independently of job execution. Any worker can perform
 * recovery, and the database-level claim/locking logic coordinates concurrent
 * recovery scans. Errors are logged so the next scan can still proceed.
 * The loop intentionally runs for the lifetime of the worker process.
 */
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

/**
 * Starts the worker's main polling and execution loop.
 *
 * For each claimed job, a heartbeat loop runs alongside simulated execution.
 * The job's configured duration determines how long execution takes, and
 * should_fail determines its intended terminal result. completeJob() checks
 * the lease token and worker ID before accepting the result, so a worker
 * that lost ownership cannot overwrite a newer attempt.
 *
 * The heartbeat is aborted and awaited in a finally block, including when
 * job processing fails. Unexpected errors are logged and followed by a short
 * delay before polling again. This function normally runs indefinitely.
 */
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

      // Start periodic lease renewal while this attempt is in progress.
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

/** Log an unexpected top-level failure and terminate with an error status. */
runWorker().catch((error) => {
  console.error(`[${workerId}] Fatal error:`, error);
  process.exit(1);
});
