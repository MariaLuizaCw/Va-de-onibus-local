-- =============================================================================
-- jobLogs - Database Functions para logs de execução de jobs
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo scheduler

-- -----------------------------------------------------------------------------
-- fn_insert_job_execution
-- Insere um registro de execução de job
-- Usado por: jobLogs.js -> logJobExecution
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_job_execution(
    p_job_name TEXT,
    p_parent_job TEXT,
    p_subtask BOOLEAN,
    p_cron_expression TEXT,
    p_started_at TIMESTAMPTZ,
    p_finished_at TIMESTAMPTZ,
    p_duration_ms INTEGER,
    p_status TEXT,
    p_error_message TEXT,
    p_info_message TEXT DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO job_executions (
        job_name,
        parent_job,
        subtask,
        cron_expression,
        started_at,
        finished_at,
        duration_ms,
        status,
        error_message,
        info_message
    )
    VALUES (
        p_job_name,
        p_parent_job,
        COALESCE(p_subtask, FALSE),
        p_cron_expression,
        p_started_at,
        p_finished_at,
        p_duration_ms,
        p_status,
        LEFT(p_error_message, 500),
        LEFT(p_info_message, 500)
    );
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_delete_old_job_executions
-- Deleta registros de execução mais antigos que o período de retenção
-- Retorna quantidade de registros deletados
-- Usado por: jobLogs.js -> deleteOldJobExecutions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_delete_old_job_executions(p_retention_days INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM job_executions 
    WHERE started_at < NOW() - (p_retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
