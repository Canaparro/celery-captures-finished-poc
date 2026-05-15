CREATE TABLE IF NOT EXISTS workflows (
    workflow_id  TEXT PRIMARY KEY,
    status       TEXT NOT NULL CHECK (status IN ('pending','complete')),
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    workflow_id     TEXT NOT NULL,
    parent_task_id  TEXT,
    record_id       INTEGER NOT NULL,
    label           TEXT,
    final_status    TEXT NOT NULL
                    CHECK (final_status IN ('success','permanent_failure','transient_failure_exhausted')),
    last_message    TEXT,
    metrics         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tasks_parent   ON tasks (parent_task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow ON tasks (workflow_id);

CREATE INDEX IF NOT EXISTS idx_tasks_workflow_duration
    ON tasks (workflow_id, ((metrics->>'total_duration_ms')::int) DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_retries
    ON tasks (workflow_id, ((metrics->>'retry_count')::int) DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_status
    ON tasks (workflow_id, final_status);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tasks_updated_at ON tasks;
CREATE TRIGGER trg_tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
