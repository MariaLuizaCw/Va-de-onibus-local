-- =============================================================================
-- reports.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo reports

-- -----------------------------------------------------------------------------
-- fn_generate_sentido_coverage_report
-- Gera relatório de cobertura de sentido para Rio
-- Usado por: reports.js -> generateSentidoCoverageReport
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_generate_sentido_coverage_report(
    p_report_date date,
    p_timezone text,
    p_max_snap_distance_meters numeric
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count integer;
BEGIN
    WITH day_bounds AS (
        SELECT
            EXTRACT(EPOCH FROM (p_report_date AT TIME ZONE p_timezone))::bigint * 1000 AS start_ms,
            EXTRACT(EPOCH FROM ((p_report_date + interval '1 day') AT TIME ZONE p_timezone))::bigint * 1000 AS end_ms
    ),
    ranked_pontos AS (
        SELECT 
            linha, 
            latitude, 
            longitude,
            ROW_NUMBER() OVER (PARTITION BY linha ORDER BY RANDOM()) AS rn
        FROM public.gps_posicoes_rio, day_bounds
        WHERE datahoraenvio >= day_bounds.start_ms AND datahoraenvio < day_bounds.end_ms
    ),
    sample_pontos AS (
        SELECT linha, latitude, longitude
        FROM ranked_pontos
        WHERE rn <= 100
    ),
    pontos_with_dist AS (
        SELECT
            p.linha,
            (
                SELECT MIN(ST_Distance(
                    i.the_geom::geography,
                    ST_SetSRID(ST_MakePoint(p.longitude::double precision, p.latitude::double precision), 4326)::geography
                ))
                FROM public.itinerario i
                WHERE i.numero_linha = p.linha AND i.habilitado = true
            ) AS min_dist
        FROM sample_pontos p
    ),
    coverage AS (
        SELECT
            linha,
            COUNT(*) AS sample_count,
            COUNT(*) FILTER (WHERE min_dist IS NOT NULL AND min_dist <= p_max_snap_distance_meters) AS covered_count
        FROM pontos_with_dist
        GROUP BY linha
    )
    INSERT INTO public.sentido_coverage_report (report_date, city, linha, total_pontos, pontos_sem_sentido, pct_sem_sentido)
    SELECT
        p_report_date AS report_date,
        'rio' AS city,
        c.linha,
        c.sample_count AS total_pontos,
        c.sample_count - c.covered_count AS pontos_sem_sentido,
        ROUND(100.0 * (c.sample_count - c.covered_count) / NULLIF(c.sample_count, 0), 2) AS pct_sem_sentido
    FROM coverage c
    ON CONFLICT (report_date, city, linha) DO UPDATE SET
        total_pontos = EXCLUDED.total_pontos,
        pontos_sem_sentido = EXCLUDED.pontos_sem_sentido,
        pct_sem_sentido = EXCLUDED.pct_sem_sentido;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    
    -- Cleanup reports older than 1 month
    DELETE FROM public.sentido_coverage_report WHERE report_date < CURRENT_DATE - interval '1 month';
    
    RETURN v_row_count;
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_generate_angra_route_type_report
-- Gera relatório de route_type indefinido para Angra
-- Usado por: reports.js -> generateAngraRouteTypeReport
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_generate_angra_route_type_report(
    p_report_date date,
    p_timezone text
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count integer;
BEGIN
    WITH day_bounds AS (
        SELECT
            (p_report_date AT TIME ZONE p_timezone) AS start_ts,
            ((p_report_date + interval '1 day') AT TIME ZONE p_timezone) AS end_ts
    )
    INSERT INTO public.sentido_coverage_report (report_date, city, linha, total_pontos, pontos_sem_sentido, pct_sem_sentido)
    SELECT
        p_report_date AS report_date,
        'angra' AS city,
        line_number AS linha,
        COUNT(*) AS total_pontos,
        COUNT(*) FILTER (WHERE route_type = 'indefinido') AS pontos_sem_sentido,
        ROUND(100.0 * COUNT(*) FILTER (WHERE route_type = 'indefinido') / NULLIF(COUNT(*), 0), 2) AS pct_sem_sentido
    FROM public.gps_posicoes_angra, day_bounds
    WHERE event_date >= day_bounds.start_ts AND event_date < day_bounds.end_ts
    GROUP BY line_number
    ON CONFLICT (report_date, city, linha) DO UPDATE SET
        total_pontos = EXCLUDED.total_pontos,
        pontos_sem_sentido = EXCLUDED.pontos_sem_sentido,
        pct_sem_sentido = EXCLUDED.pct_sem_sentido;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RETURN v_row_count;
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
