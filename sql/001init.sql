-- init run only when PostgreSQL volume first created.
-- request fingerprint to supprt checking idempodency-key reuse. lease fields
-- for recovering crashed jobs, unique job_id in execution_recodes so a job isnt completed by multiple workers
CREATE TABLE
    jobs (
        id UUID PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        request_fingerprint TEXT NOT NULL,
        duration_ms INTEGER NOT NULL CHECK (duration_ms BETWEEN 100 AND 10000),
        should_fail BOOLEAN NOT NULL DEFAULT FALSE,
        status TEXT NOT NULL DEFAULT 'queued' CHECK (
            status IN (
                'queued',
                'running',
                'succeeded',
                'failed',
                'cancelled'
            )
        ),
        worker_id TEXT,
        lease_token UUID,
        lease_expires_at TIMESTAMPTZ,
        attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp (),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp (),
        finished_at TIMESTAMPTZ,
        CHECK (
            (
                status = 'running'
                AND lease_token IS NOT NULL
                AND lease_expires_at IS NOT NULL
            )
            OR (
                status <> 'running'
                AND lease_token IS NULL
                AND lease_expires_at IS NULL
            )
        )
    );

CREATE INDEX jobs_queued_idx ON jobs (created_at, id)
WHERE
    status = 'queued';

CREATE INDEX jobs_expired_idx ON jobs (lease_expires_at)
WHERE
    status = 'running';

CREATE INDEX jobs_recent_idx ON jobs (created_at DESC, id DESC);

-- only committd successful exec writes effct
CREATE TABLE
    execution_records (
        id UUID PRIMARY KEY,
        job_id UUID NOT NULL UNIQUE REFERENCES jobs (id),
        worker_id TEXT NOT NULL,
        claim_attempt INTEGER NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp ()
    );