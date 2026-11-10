# Job Queue - Design
---

## Database choice (PostgreSQL):
PostgreSQL was selected over SQLite as it supports concurrent writes, row-level locking, and “FOR UPDATE SKIP LOCKED” which is the mechanism that will allow us to have multiple workers claiming different jobs without accidentally having job-claiming conflicts, which allows concurrency to occur smoothly.  

SQLite could simplify write concurrency (but not eliminate it as application-level race conditions are still possible) as it allows only one writer at a time through serialized transactions, while PostgreSQL supports multiple concurrent writers. However, SQLite's single-writer limitation could become a performance bottleneck under high write concurrency. Hence, PostgreSQL is preferred for its ability to handle concurrent transactions.

## How a worker claims a job:
When an available job is found, PostgreSQL locks its row using  “FOR UPDATE SKIP LOCKED”, thus preventing other workers from claiming the same job simultaneously. While the row is locked, its status updates from “queued”  to “running”. Then, the worker commits the database transaction and releases the row lock. Other workers cannot claim the job through the normal claiming process as its status is now “running”. Then the worker can process the job without holding the database lock.

## How you detect a dead worker:
We can detect dead workers using leases and heartbeats. We check this by seeing if the worker is still renewing its permission to process a job. For example: a worker starts on a job and, with that, gets a 30-second lease. Every 10 seconds, it is able to renew the lease to continue working on a job. When the lease expires, a recovery process would detect the expired lease and return the incomplete job to the queue; this allows another worker to claim that job. Lease tokens would prevent the original worker from committing results if it resumes after losing ownership of the job. 

## What happens when a running job is cancelled:
When a running job is cancelled, the operation is committed first, determining the status outcome. If the worker has already completed the job and committed its result to PostgreSQL, cancellation cannot occur, as the job has reached a terminal state. However, if cancellation is committed before completion, the job's status changes from “running”  to “cancelled”, and the worker's lease is invalidated, preventing it from committing a successful result. The worker should then stop processing the job once it detects the cancellation.

## which guarantee you give (at-least-once, at-most-once or exactly-once effect) and why:
My traing-job queue approach aims to provide an at-least-once execution guarantee. This approach permits jobs to be retried in the case that a worker crashes or fails during job processing. This prevents unfinished jobs from being permanently lost. Even though duplicate execution attempts may happen, safeguards such as lease based worker ownership, atomic PostgreSQL transactions, and unique constraints on execution records are used to prevent repeatedly committed database side effects. The at-least-once approach puts emphasis on job recovery while ensuring consistency.

