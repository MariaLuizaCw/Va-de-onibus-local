const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');

const retention_days = Number(process.env.PARTITION_RETENTION_DAYS) || 7;
const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function enrichRecordsWithSentido(records) {
    if (!Array.isArray(records) || records.length === 0) return records;

    const BATCH_SIZE = Number(process.env.SENTIDO_BATCH_SIZE) || 2000;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // Prepara JSON array para a function
        const pointsJson = batch.map(record => {
            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);
            
            return {
                linha: String(record.linha),
                lon: lon,
                lat: lat,
                ordem: String(record.ordem)
            };
        });

        let result;
        try {
            result = await dbPool.query(
                'SELECT * FROM fn_enrich_gps_batch_with_sentido_json($1::jsonb, $2)',
                [JSON.stringify(pointsJson), MAX_SNAP_DISTANCE_METERS]
            );
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

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const recordsJson = batch.map(record => {
            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            return {
                ordem: record.ordem,
                latitude: lat,
                longitude: lon,
                datahora: Number(record.datahora),
                velocidade: Number(record.velocidade),
                linha: record.linha,
                datahoraenvio: Number(record.datahoraenvio),
                datahoraservidor: Number(record.datahoraservidor)
            };
        });

        try {
            await dbPool.query(
                'SELECT fn_insert_gps_posicoes_rio_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
        } catch (err) {
            console.error('[Rio] Error inserting GPS records into database:', err);
        }
    }
}

async function saveRioToGpsSentido(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;

    const now = new Date();
    const minDate = new Date(now.getTime() - retention_days * 24 * 60 * 60 * 1000);

    const filteredRecords = records.filter((record) => {
        const datahoraMs = Number(record.datahora);
        if (!Number.isFinite(datahoraMs)) return false;
        const dt = new Date(datahoraMs);
        if (isNaN(dt.getTime())) return false;
        return dt >= minDate && dt <= now;
    });


    for (let i = 0; i < filteredRecords.length; i += BATCH_SIZE) {
        const batch = filteredRecords.slice(i, i + BATCH_SIZE);

        const recordsJson = batch.map(record => {
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

            return {
                ordem: record.ordem,
                datahora: datahoraTimestamp,
                linha: record.linha,
                latitude: lat,
                longitude: lon,
                velocidade: Number(record.velocidade),
                sentido: record.sentido || null,
                sentido_itinerario_id: record.sentido_itinerario_id || null,
                route_name: record.route_name || null,
                token: 'PMRJ'
            };
        });

        try {
            await dbPool.query(
                'SELECT fn_insert_gps_sentido_rio_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
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

        // Prepara JSON array para a function
        const pointsJson = batch.map(record => {
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

            return {
                ordem: String(record.ordem),
                linha: String(record.linha),
                lon: lon,
                lat: lat,
                datahora: datahoraTimestamp
            };
        });

        try {
            await dbPool.query(
                'SELECT fn_upsert_gps_onibus_estado_batch_json($1::jsonb, $2, $3)',
                [JSON.stringify(pointsJson), TERMINAL_VISIT_DISTANCE_METERS, TERMINAL_PROXIMITY_DISTANCE_METERS]
            );
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
