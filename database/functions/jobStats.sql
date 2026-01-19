-- =============================================================================
-- Job Statistics Functions
-- Execute este script para criar/atualizar as functions de estatísticas de jobs
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fn_get_job_stats
-- Retorna estatísticas agregadas dos jobs para uma data específica
-- Usado por: jobStats.js -> getJobStats
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_job_stats(p_date DATE)
RETURNS TABLE (
    type TEXT,
    job_name TEXT,
    parent_job TEXT,
    execution_count BIGINT,
    avg_duration_ms NUMERIC,
    stddev_duration_ms NUMERIC,
    min_duration_ms BIGINT,
    max_duration_ms BIGINT,
    success_count BIGINT,
    error_count BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH job_data AS (
        SELECT 
            je.job_name,
            je.parent_job,
            je.subtask,
            je.duration_ms,
            je.status,
            je.started_at,
            je.finished_at
        FROM job_executions je
        WHERE DATE(je.started_at) = p_date
    ),
    parent_stats AS (
        SELECT 
            je.job_name,
            COUNT(*) as execution_count,
            AVG(je.duration_ms)::numeric(10,2) as avg_duration_ms,
            STDDEV(je.duration_ms)::numeric(10,2) as stddev_duration_ms,
            MIN(je.duration_ms) as min_duration_ms,
            MAX(je.duration_ms) as max_duration_ms,
            SUM(CASE WHEN je.status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN je.status = 'error' THEN 1 ELSE 0 END) as error_count
        FROM job_data je
        WHERE je.subtask = false OR je.subtask IS NULL
        GROUP BY je.job_name
    ),
    child_stats AS (
        SELECT 
            je.job_name,
            je.parent_job,
            COUNT(*) as execution_count,
            AVG(je.duration_ms)::numeric(10,2) as avg_duration_ms,
            STDDEV(je.duration_ms)::numeric(10,2) as stddev_duration_ms,
            MIN(je.duration_ms) as min_duration_ms,
            MAX(je.duration_ms) as max_duration_ms,
            SUM(CASE WHEN je.status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN je.status = 'error' THEN 1 ELSE 0 END) as error_count
        FROM job_data je
        WHERE je.subtask = true
        GROUP BY je.job_name, je.parent_job
    )
    SELECT 
        'parent'::TEXT as type,
        ps.job_name::TEXT,
        NULL::TEXT as parent_job,
        ps.execution_count::BIGINT,
        ps.avg_duration_ms::NUMERIC,
        ps.stddev_duration_ms::NUMERIC,
        ps.min_duration_ms::BIGINT,
        ps.max_duration_ms::BIGINT,
        ps.success_count::BIGINT,
        ps.error_count::BIGINT
    FROM parent_stats ps
    UNION ALL
    SELECT 
        'child'::TEXT as type,
        cs.job_name::TEXT,
        cs.parent_job::TEXT,
        cs.execution_count::BIGINT,
        cs.avg_duration_ms::NUMERIC,
        cs.stddev_duration_ms::NUMERIC,
        cs.min_duration_ms::BIGINT,
        cs.max_duration_ms::BIGINT,
        cs.success_count::BIGINT,
        cs.error_count::BIGINT
    FROM child_stats cs
    ORDER BY type DESC, job_name;
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_get_job_timeline
-- Retorna timeline de execuções de um job específico
-- Usado por: jobStats.js -> getJobTimeline
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_job_timeline(
    p_job_name TEXT,
    p_date DATE,
    p_include_children BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    job_name TEXT,
    parent_job TEXT,
    subtask BOOLEAN,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    status TEXT,
    info_message TEXT,
    error_message TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        je.job_name::TEXT,
        je.parent_job::TEXT,
        je.subtask::BOOLEAN,
        je.started_at::TIMESTAMPTZ,
        je.finished_at::TIMESTAMPTZ,
        je.duration_ms::BIGINT,
        je.status::TEXT,
        je.info_message::TEXT,
        je.error_message::TEXT
    FROM job_executions je
    WHERE DATE(je.started_at) = p_date
      AND (je.job_name = p_job_name OR (p_include_children = true AND je.parent_job = p_job_name))
    ORDER BY je.started_at ASC;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_get_job_hourly_distribution
-- Retorna distribuição de status por hora para um job
-- Usado por: jobStats.js -> getJobHourlyDistribution
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_job_hourly_distribution(
    p_job_name TEXT,
    p_date DATE
)
RETURNS TABLE (
    hour INTEGER,
    total BIGINT,
    success_count BIGINT,
    error_count BIGINT,
    avg_duration_ms NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        EXTRACT(HOUR FROM je.started_at)::INTEGER as hour,
        COUNT(*)::BIGINT as total,
        SUM(CASE WHEN je.status = 'success' THEN 1 ELSE 0 END)::BIGINT as success_count,
        SUM(CASE WHEN je.status = 'error' THEN 1 ELSE 0 END)::BIGINT as error_count,
        AVG(je.duration_ms)::NUMERIC(10,2) as avg_duration_ms
    FROM job_executions je
    WHERE DATE(je.started_at) = p_date
      AND (je.job_name = p_job_name OR je.parent_job = p_job_name)
    GROUP BY EXTRACT(HOUR FROM je.started_at)
    ORDER BY hour;
END;
$$;
