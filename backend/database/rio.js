const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');

const retention_days = Number(process.env.PARTITION_RETENTION_DAYS) || 7;
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
                best.dist_m,
                best.itinerario_id,
                best.route_name
            FROM pts
            LEFT JOIN LATERAL (
                SELECT
                    i.sentido,
                    i.id AS itinerario_id,
                    i.route_name,
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
            record.sentido_itinerario_id = row.itinerario_id != null ? Number(row.itinerario_id) : null;
            record.route_name = row.route_name != null ? String(row.route_name) : null;
        }
    }

    return records;
}

async function saveRioRecordsToDb(records) {
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
            INSERT INTO gps_posicoes_rio (
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
            ON CONFLICT ON CONSTRAINT gps_posicoes_rio_unique_ponto DO NOTHING;
        `;

        try {
            await dbPool.query(text, values);
        } catch (err) {
            console.error('[Rio] Error inserting GPS records into database:', err);
        }
    }
}

async function saveRioToGpsSentido(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 10;

    const now = new Date();
    const minDate = new Date(now.getTime() - retention_days * 24 * 60 * 60 * 1000);

    const filteredRecords = records.filter((record) => {
        const datahoraMs = Number(record.datahora);
        if (!Number.isFinite(datahoraMs)) return false;
        const dt = new Date(datahoraMs);
        if (isNaN(dt.getTime())) return false;
        return dt >= minDate && dt <= now;
    });

    const skippedCount = records.length - filteredRecords.length;
    if (filteredRecords.length === 0) {
        console.log(
            `[Rio][gps_sentido] All ${records.length} records filtered out (outside ${retention_days} day window: ${formatDateInTimeZone(minDate)} to ${formatDateInTimeZone(now)})`
        );
        return;
    }

    if (skippedCount > 0) {
        console.log(
            `[Rio][gps_sentido] Filtered ${skippedCount} records outside ${retention_days} day window: ${formatDateInTimeZone(minDate)} to ${formatDateInTimeZone(now)}`
        );
    }

    for (let i = 0; i < filteredRecords.length; i += BATCH_SIZE) {
        const batch = filteredRecords.slice(i, i + BATCH_SIZE);

        const values = [];
        const placeholders = [];

        batch.forEach((record, index) => {
            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            // Converter datahora (ms) para timestamp
            const datahoraMs = Number(record.datahora);
            const datahoraTimestamp = Number.isFinite(datahoraMs)
                ? formatDateInTimeZone(new Date(datahoraMs))
                : null;

            values.push(
                record.ordem,
                datahoraTimestamp,
                record.linha,
                lat,
                lon,
                Number(record.velocidade),
                record.sentido || null,
                record.sentido_itinerario_id || null,
                record.route_name || null,
                'PMRJ'
            );

            const baseIndex = index * PARAMS_PER_ROW;
            placeholders.push(
                `($${baseIndex + 1}, $${baseIndex + 2}::timestamp, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9}, $${baseIndex + 10})`
            );
        });

        const text = `
            INSERT INTO gps_sentido (
                ordem,
                datahora,
                linha,
                latitude,
                longitude,
                velocidade,
                sentido,
                sentido_itinerario_id,
                route_name,
                token
            ) VALUES
                ${placeholders.join(',\n')}
            ON CONFLICT (ordem, datahora) DO NOTHING;
        `;

        try {
            await dbPool.query(text, values);
        } catch (err) {
            console.error('[Rio][gps_sentido] Error inserting records:', err.message);
        }
    }
}

const TERMINAL_VISIT_DISTANCE_METERS = Number(process.env.TERMINAL_VISIT_DISTANCE_METERS) || 20;
const TERMINAL_PROXIMITY_DISTANCE_METERS = Number(process.env.TERMINAL_PROXIMITY_DISTANCE_METERS) || 100;

async function saveRioToGpsOnibusEstado(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.SENTIDO_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 4;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const values = [TERMINAL_VISIT_DISTANCE_METERS, TERMINAL_PROXIMITY_DISTANCE_METERS];
        const placeholders = [];

        for (let index = 0; index < batch.length; index++) {
            const record = batch[index];

            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            const datahoraMs = Number(record.datahora);
            const datahoraTimestamp = Number.isFinite(datahoraMs)
                ? formatDateInTimeZone(new Date(datahoraMs))
                : null;

            values.push(
                String(record.ordem),
                String(record.linha),
                lon,
                lat,
                datahoraTimestamp
            );

            const baseIndex = 2 + index * 5;
            placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}::timestamp)`);
        }

        const text = `
            WITH pts(ordem, linha, lon, lat, datahora) AS (
                SELECT
                    v.ordem::text,
                    v.linha::text,
                    v.lon::double precision,
                    v.lat::double precision,
                    v.datahora::timestamp
                FROM (VALUES ${placeholders.join(',\n')}) AS v(ordem, linha, lon, lat, datahora)
            ),
            terminal_distances AS (
                SELECT
                    pts.ordem,
                    pts.linha,
                    pts.lon,
                    pts.lat,
                    pts.datahora,
                    i.sentido,
                    ST_Distance(
                        ST_StartPoint(i.the_geom)::geography,
                        ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography
                    ) AS dist_m
                FROM pts
                INNER JOIN public.itinerario i
                    ON i.habilitado = true
                    AND i.numero_linha::text = pts.linha
            ),
            best_terminal AS (
                SELECT DISTINCT ON (ordem)
                    ordem,
                    linha,
                    datahora,
                    sentido,
                    dist_m
                FROM terminal_distances
                ORDER BY ordem, dist_m ASC
            ),
            upsert_data AS (
                SELECT
                    bt.ordem,
                    bt.linha,
                    bt.datahora,
                    bt.sentido,
                    bt.dist_m,
                    CASE
                        WHEN bt.dist_m <= $1 THEN bt.sentido
                        ELSE NULL
                    END AS new_ultimo_terminal,
                    CASE
                        WHEN bt.dist_m <= $1 THEN bt.datahora
                        ELSE NULL
                    END AS new_ultima_passagem,
                    CASE
                        WHEN bt.dist_m > $1 AND bt.dist_m <= $2 THEN bt.sentido
                        ELSE NULL
                    END AS new_terminal_proximo,
                    CASE
                        WHEN bt.dist_m > $1 AND bt.dist_m <= $2 THEN bt.dist_m
                        ELSE NULL
                    END AS new_distancia_terminal,
                    CASE
                        WHEN bt.dist_m > $1 AND bt.dist_m <= $2 THEN bt.datahora
                        ELSE NULL
                    END AS new_desde_terminal_proximo
                FROM best_terminal bt
            )
            INSERT INTO gps_onibus_estado (
                ordem,
                linha,
                token,
                ultimo_terminal,
                ultima_passagem_terminal,
                terminal_proximo,
                distancia_terminal_metros,
                desde_terminal_proximo,
                atualizado_em
            )
            SELECT
                ud.ordem,
                ud.linha,
                'PMRJ',
                COALESCE(ud.new_ultimo_terminal, ''),
                ud.new_ultima_passagem,
                ud.new_terminal_proximo,
                ud.new_distancia_terminal,
                ud.new_desde_terminal_proximo,
                now()
            FROM upsert_data ud
            ON CONFLICT (ordem) DO UPDATE SET
                linha = EXCLUDED.linha,
                token = EXCLUDED.token,
                ultimo_terminal = CASE
                    WHEN EXCLUDED.ultimo_terminal != '' THEN EXCLUDED.ultimo_terminal
                    ELSE gps_onibus_estado.ultimo_terminal
                END,
                ultima_passagem_terminal = CASE
                    WHEN EXCLUDED.ultimo_terminal != '' THEN EXCLUDED.ultima_passagem_terminal
                    ELSE gps_onibus_estado.ultima_passagem_terminal
                END,
                terminal_proximo = CASE
                    WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
                    ELSE EXCLUDED.terminal_proximo
                END,
                distancia_terminal_metros = CASE
                    WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
                    ELSE EXCLUDED.distancia_terminal_metros
                END,
                desde_terminal_proximo = CASE
                    WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
                    WHEN EXCLUDED.terminal_proximo IS NOT NULL
                        AND gps_onibus_estado.terminal_proximo = EXCLUDED.terminal_proximo
                        AND gps_onibus_estado.desde_terminal_proximo IS NOT NULL
                    THEN gps_onibus_estado.desde_terminal_proximo
                    ELSE EXCLUDED.desde_terminal_proximo
                END,
                atualizado_em = now();
        `;

        try {
            await dbPool.query(text, values);
        } catch (err) {
            console.error('[Rio][gps_onibus_estado] Error upserting records:', err.message);
        }
    }
}

module.exports = {
    enrichRecordsWithSentido,
    saveRioRecordsToDb,
    saveRioToGpsSentido,
    saveRioToGpsOnibusEstado,
};
