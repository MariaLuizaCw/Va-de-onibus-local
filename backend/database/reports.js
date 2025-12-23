const { dbPool } = require('./pool');
const { API_TIMEZONE, formatDateYYYYMMDDInTimeZone } = require('../utils');

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function generateSentidoCoverageReport() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const reportDate = formatDateYYYYMMDDInTimeZone(yesterday, API_TIMEZONE);

    const query = `
        WITH day_bounds AS (
            SELECT
                EXTRACT(EPOCH FROM (($1::date) AT TIME ZONE $2))::bigint * 1000 AS start_ms,
                EXTRACT(EPOCH FROM (($1::date + interval '1 day') AT TIME ZONE $2))::bigint * 1000 AS end_ms
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
                COUNT(*) FILTER (WHERE min_dist IS NOT NULL AND min_dist <= $3) AS covered_count
            FROM pontos_with_dist
            GROUP BY linha
        )
        INSERT INTO public.sentido_coverage_report (report_date, linha, total_pontos, pontos_sem_sentido, pct_sem_sentido)
        SELECT
            $1::date AS report_date,
            c.linha,
            c.sample_count AS total_pontos,
            c.sample_count - c.covered_count AS pontos_sem_sentido,
            ROUND(100.0 * (c.sample_count - c.covered_count) / NULLIF(c.sample_count, 0), 2) AS pct_sem_sentido
        FROM coverage c
        ON CONFLICT (report_date, linha) DO UPDATE SET
            total_pontos = EXCLUDED.total_pontos,
            pontos_sem_sentido = EXCLUDED.pontos_sem_sentido,
            pct_sem_sentido = EXCLUDED.pct_sem_sentido;
    `;

    try {
        const result = await dbPool.query(query, [reportDate, API_TIMEZONE, MAX_SNAP_DISTANCE_METERS]);
        console.log(`[coverage] Generated sentido coverage report for ${reportDate}: ${result.rowCount} lines`);

        // Cleanup reports older than 1 month
        await dbPool.query(`DELETE FROM public.sentido_coverage_report WHERE report_date < CURRENT_DATE - interval '1 month'`);
    } catch (err) {
        console.error('[coverage] Error generating sentido coverage report:', err);
    }
}

module.exports = {
    generateSentidoCoverageReport,
};
