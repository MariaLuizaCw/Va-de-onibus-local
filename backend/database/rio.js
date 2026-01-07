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

module.exports = {
    enrichRecordsWithSentido,
    saveRioRecordsToDb,
    saveRioToGpsSentido,
};
