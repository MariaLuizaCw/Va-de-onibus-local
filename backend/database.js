const { Pool } = require('pg');
const { API_TIMEZONE, formatDateYYYYMMDDInTimeZone } = require('./utils');

const dbPool = new Pool({
    host: process.env.DATABASE_HOST,
    port: Number(process.env.DATABASE_PORT) || 5432,
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME
});

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function enrichRecordsWithSentido(records) {
    if (!Array.isArray(records) || records.length === 0) return records;

    const BATCH_SIZE = Number(process.env.SENTIDO_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 4;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const values = [MAX_SNAP_DISTANCE_METERS];
        const placeholders = [];

        for (let index = 0; index < batch.length; index++) {
            const record = batch[index];

            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            values.push(String(record.linha), lon, lat, String(record.ordem));

            const baseIndex = 1 + index * PARAMS_PER_ROW;
            placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4})`);
        }

        const text = `
            WITH pts(linha, lon, lat, ordem) AS (
                SELECT
                    v.linha::text,
                    v.lon::double precision,
                    v.lat::double precision,
                    v.ordem::text
                FROM (VALUES ${placeholders.join(',\n')}) AS v(linha, lon, lat, ordem)
            )
            SELECT
                pts.linha,
                pts.ordem,
                best.sentido,
                best.dist_m
            FROM pts
            LEFT JOIN LATERAL (
                SELECT
                    i.sentido,
                    ST_Distance(
                        i.the_geom::geography,
                        ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography
                    ) AS dist_m
                FROM public.itinerario i
                WHERE i.habilitado = true
                  AND i.numero_linha::text = pts.linha
                  AND ST_DWithin(
                        i.the_geom::geography,
                        ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography,
                        $1
                  )
                ORDER BY dist_m ASC
                LIMIT 1
            ) best ON true;
        `;

        let result;
        try {
            result = await dbPool.query(text, values);
        } catch (err) {
            console.error('[sentido] Error computing sentido via PostGIS:', err);
            continue;
        }

        const byKey = new Map();
        for (const row of result.rows) {
            const key = `${String(row.linha)}|${String(row.ordem)}`;
            byKey.set(key, row);
        }

        for (const record of batch) {
            const key = `${String(record.linha)}|${String(record.ordem)}`;
            const row = byKey.get(key);
            if (!row) continue;
            record.sentido = row.sentido != null ? String(row.sentido) : null;
            record.distancia_metros = row.dist_m != null ? Number(row.dist_m) : null;
        }
    }

    return records;
}

async function saveRecordsToDb(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 8;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const values = [];
        const placeholders = [];

        batch.forEach((record, index) => {
            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            values.push(
                record.ordem,
                lat,
                lon,
                Number(record.datahora),
                Number(record.velocidade),
                record.linha,
                Number(record.datahoraenvio),
                Number(record.datahoraservidor)
            );

            const baseIndex = index * PARAMS_PER_ROW;
            placeholders.push(
                `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8})`
            );
        });

        const text = `
            INSERT INTO gps_posicoes (
                ordem,
                latitude,
                longitude,
                datahora,
                velocidade,
                linha,
                datahoraenvio,
                datahoraservidor
            ) VALUES
                ${placeholders.join(',\n')}
            ON CONFLICT ON CONSTRAINT gps_posicoes_unique_ponto DO NOTHING;
        `;

        try {
            await dbPool.query(text, values);
        } catch (err) {
            console.error('Error inserting GPS records into database:', err);
        }
    }
}

async function loadLatestRioOnibusSnapshot() {
    const text = `
        SELECT data
        FROM public.rio_onibus_snapshots
        LIMIT 1;
    `;

    try {
        const result = await dbPool.query(text);
        if (!result.rows || result.rows.length === 0) {
            console.log('[snapshot] No snapshot found in database');
            return null;
        }
        console.log('[snapshot] Loaded snapshot from database');
        return result.rows[0].data || null;
    } catch (err) {
        console.error('[snapshot] Error loading latest rio_onibus snapshot from database:', err);
        return null;
    }
}

async function saveRioOnibusSnapshot(snapshot) {
    if (!snapshot) return;

    try {
        await dbPool.query('TRUNCATE public.rio_onibus_snapshots');
        await dbPool.query('INSERT INTO public.rio_onibus_snapshots (data) VALUES ($1::jsonb)', [snapshot]);
        console.log('[snapshot] Saved snapshot to database');
    } catch (err) {
        console.error('[snapshot] Error inserting rio_onibus snapshot into database:', err);
    }
}

function formatDateYYYYMMDD(date) {
    const pad = (n) => n.toString().padStart(2, '0');
    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    return `${year}-${month}-${day}`;
}

async function cleanupOldPartitions(retentionDays = 7) {
    const todaySpStr = formatDateYYYYMMDDInTimeZone(new Date(), API_TIMEZONE);

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
    const cutoffSpStr = formatDateYYYYMMDDInTimeZone(cutoffDate, API_TIMEZONE);

    console.log(`[partitions] cleanup start tz=${API_TIMEZONE} today=${todaySpStr} retentionDays=${retentionDays} cutoff=${cutoffSpStr}`);

    let result;
    try {
        result = await dbPool.query(
            `
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE 'gps_posicoes_%'
            `
        );
    } catch (err) {
        console.error('Error listing GPS partition tables', err);
        return;
    }

    console.log(`[partitions] found ${result.rows.length} tables matching gps_posicoes_%`);

    const tablesToDrop = result.rows
        .map((r) => r.tablename)
        .filter((name) => /^gps_posicoes_\d{8}$/.test(name))
        .map((name) => {
            const y = name.slice('gps_posicoes_'.length, 'gps_posicoes_'.length + 4);
            const m = name.slice('gps_posicoes_'.length + 4, 'gps_posicoes_'.length + 6);
            const d = name.slice('gps_posicoes_'.length + 6, 'gps_posicoes_'.length + 8);
            return { name, dateStr: `${y}-${m}-${d}` };
        })
        .filter(({ dateStr }) => dateStr < cutoffSpStr)
        .sort((a, b) => a.dateStr.localeCompare(b.dateStr));

    if (tablesToDrop.length === 0) {
        console.log(`[partitions] nothing to drop (cutoff=${cutoffSpStr})`);
        return;
    }

    console.log(
        `[partitions] will drop ${tablesToDrop.length} partitions older than ${cutoffSpStr}: ${tablesToDrop
            .map((t) => `${t.name}(${t.dateStr})`)
            .join(', ')}`
    );

    for (const { name, dateStr } of tablesToDrop) {
        try {
            console.log(`[partitions] dropping ${name} (date=${dateStr})`);
            await dbPool.query(`DROP TABLE IF EXISTS public.${name};`);
        } catch (err) {
            console.error('Error dropping old GPS partition', name, dateStr, { cutoffSpStr, todaySpStr }, err);
        }
    }

    console.log(`[partitions] cleanup done dropped=${tablesToDrop.length}`);
}

async function createPartitionForDate(dateStr) {
    const tableSuffix = dateStr.replace(/-/g, '');
    const tableName = `gps_posicoes_${tableSuffix}`;

    const text = `
        CREATE TABLE IF NOT EXISTS public.${tableName}
        PARTITION OF public.gps_posicoes
        FOR VALUES FROM (
            (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE 'America/Sao_Paulo') * 1000)::bigint
        ) TO (
            (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE 'America/Sao_Paulo' + interval '1 day') * 1000)::bigint
        );
    `;

    try {
        await dbPool.query(text);
    } catch (err) {
        console.error('Error creating partition for date', dateStr, err);
    }
}

async function ensureFuturePartitions() {
    const now = new Date();
    const offsets = [2];

    for (const offset of offsets) {
        const targetDate = new Date(now.getTime() + offset * 24 * 60 * 60 * 1000);
        const dateStr = formatDateYYYYMMDD(targetDate);
        await createPartitionForDate(dateStr);
    }

    const retentionDays = Number(process.env.PARTITION_RETENTION_DAYS) || 7;
    await cleanupOldPartitions(retentionDays);
}

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
            FROM public.gps_posicoes, day_bounds
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
    dbPool,
    enrichRecordsWithSentido,
    saveRecordsToDb,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    ensureFuturePartitions,
    generateSentidoCoverageReport,
};
