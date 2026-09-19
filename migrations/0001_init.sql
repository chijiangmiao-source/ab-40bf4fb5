-- Switch forwarding-table migration service: persistent schema.
-- All concurrency control (idempotency dedup, coordinator leases, epochs,
-- device generations, command dedup) lives in the database so that any number
-- of API containers sharing this database stay correct. No in-process locks,
-- in-memory queues or local clocks participate in correctness.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Immutable plans
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plans (
    id                 UUID PRIMARY KEY,
    idempotency_key    TEXT NOT NULL UNIQUE,
    -- Exact hash of the canonical normalized request: reused key with
    -- different parameters is detected against this value.
    input_fingerprint  TEXT NOT NULL,
    -- Canonical JSON of normalized topology + ingresses.
    input_canonical    JSONB NOT NULL,
    switches           JSONB NOT NULL,  -- ["id", ...] in raw UTF-8 byte order
    ingresses          JSONB NOT NULL,
    old_next           JSONB NOT NULL,
    new_next           JSONB NOT NULL,
    changed_switches   JSONB NOT NULL,
    step_count         INTEGER NOT NULL CHECK (step_count >= 0),
    status             TEXT NOT NULL
                       CHECK (status IN ('feasible', 'proven_impossible')),
    -- Null when proven_impossible.
    update_order       JSONB,
    plan_digest        TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Rollouts (executions of an immutable plan)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollouts (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    plan_id         UUID NOT NULL REFERENCES plans(id),
    plan_digest     TEXT NOT NULL,
    total_steps     INTEGER NOT NULL CHECK (total_steps >= 0),
    status          TEXT NOT NULL
                    CHECK (status IN ('PENDING', 'ACTIVE', 'COMPLETED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-rollout coordinator lease; all deadlines come from the database clock.
CREATE TABLE IF NOT EXISTS coordinator_leases (
    rollout_id       UUID PRIMARY KEY REFERENCES rollouts(id),
    epoch            BIGINT NOT NULL,
    holder_id        TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Global epoch pool: every takeover gets nextval, so per rollout epochs are
-- strictly increasing and a value is never reused.
CREATE SEQUENCE IF NOT EXISTS coordinator_epoch_seq AS BIGINT;

-- Idempotency records for lease/advance operations.
CREATE TABLE IF NOT EXISTS coordinator_ops (
    rollout_id    UUID NOT NULL REFERENCES rollouts(id),
    operation_id  TEXT NOT NULL,
    op_kind       TEXT NOT NULL
                  CHECK (op_kind IN ('acquire', 'renew', 'advance')),
    epoch         BIGINT NOT NULL,
    holder_id     TEXT NOT NULL,
    result        JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rollout_id, operation_id)
);

-- ---------------------------------------------------------------------------
-- Device commands and acknowledgements
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS device_commands (
    id                UUID PRIMARY KEY,            -- command_id
    rollout_id        UUID NOT NULL REFERENCES rollouts(id),
    step              INTEGER NOT NULL CHECK (step >= 0),
    switch_id         TEXT NOT NULL,
    plan_digest       TEXT NOT NULL,
    device_generation BIGINT NOT NULL CHECK (device_generation >= 1),
    status            TEXT NOT NULL
                      CHECK (status IN ('PENDING', 'APPLIED')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at        TIMESTAMPTZ,
    UNIQUE (rollout_id, step),
    UNIQUE (rollout_id, switch_id, device_generation)
);

-- Defense in depth: at most one outstanding command per rollout at any time.
CREATE UNIQUE INDEX IF NOT EXISTS device_commands_one_pending
    ON device_commands (rollout_id)
    WHERE status = 'PENDING';

-- Accepted device generations, globally per switch across all rollouts:
-- a device's generation strictly increases every time it is commanded, and
-- a late acknowledgement below the device's accepted generation is rejected.
CREATE TABLE IF NOT EXISTS device_generations (
    switch_id           TEXT PRIMARY KEY,
    current_generation  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS acknowledgements (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rollout_id        UUID NOT NULL REFERENCES rollouts(id),
    command_id        UUID NOT NULL REFERENCES device_commands(id) UNIQUE,
    switch_id         TEXT NOT NULL,
    step              INTEGER NOT NULL,
    plan_digest       TEXT NOT NULL,
    device_generation BIGINT NOT NULL,
    result            TEXT NOT NULL CHECK (result IN ('APPLIED')),
    duplicate_count   INTEGER NOT NULL DEFAULT 0,
    first_created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Audit trail: one row per state-changing event.
CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rollout_id  UUID NOT NULL REFERENCES rollouts(id),
    event_type  TEXT NOT NULL,
    epoch       BIGINT,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_events_rollout_idx
    ON audit_events (rollout_id, id);

INSERT INTO schema_migrations(version) VALUES (1)
ON CONFLICT (version) DO NOTHING;
