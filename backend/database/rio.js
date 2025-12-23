const { dbPool } = require('./pool');

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

module.exports = {
    enrichRecordsWithSentido,
    saveRioRecordsToDb,
};
