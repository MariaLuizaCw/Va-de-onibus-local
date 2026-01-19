-- =============================================================================
-- job_executions - Tabela de logs de execução de jobs
-- =============================================================================

CREATE TABLE IF NOT EXISTS job_executions (
    id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    subtask BOOLEAN DEFAULT FALSE,
    cron_expression TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms INTEGER,
    status TEXT CHECK (status IN ('success', 'error')),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions(job_name);
CREATE INDEX IF NOT EXISTS idx_job_executions_started_at ON job_executions(started_at);
CREATE INDEX IF NOT EXISTS idx_job_executions_status ON job_executions(status);
