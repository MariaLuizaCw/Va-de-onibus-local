const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');
const { getRioOnibus } = require('../stores/rioOnibusStore');

const retention_days = Number(process.env.PARTITION_RETENTION_DAYS) || 7;
const INACTIVITY_THRESHOLD_MINUTES = Number(process.env.INACTIVITY_THRESHOLD_MINUTES) || 15;
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


async function saveRioToGpsSentido(records) {
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
                'SELECT fn_upsert_gps_sentido_rio_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
        } catch (err) {
            console.error('[Rio][gps_sentido] Error upserting records:', err.message);
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


async function deactivateInactiveOnibusEstado() {
    const now = Date.now();
    const thresholdMs = INACTIVITY_THRESHOLD_MINUTES * 60 * 1000;
    const inactiveOrdens = new Set();

    const rioOnibus = getRioOnibus();

    for (const linhaKey of Object.keys(rioOnibus)) {
        const positions = rioOnibus[linhaKey];
        if (!Array.isArray(positions) || positions.length === 0) continue;

        const pos = positions[0];
        if (!pos || pos.ordem == null || !pos.datahora) continue;

        let datahoraMs = Number(pos.datahora);

        if (!Number.isFinite(datahoraMs)) {
            const parsed = Date.parse(pos.datahora);
            if (!Number.isFinite(parsed)) continue;
            datahoraMs = parsed;
        }

        if (now - datahoraMs > thresholdMs) {
            inactiveOrdens.add(String(pos.ordem));
        }
    }

    if (inactiveOrdens.size === 0) {
        return 0;
    }

    const result = await dbPool.query(
        'SELECT fn_deactivate_gps_onibus_estado_by_ordens($1::jsonb)',
        [JSON.stringify([...inactiveOrdens])]
    );

    const deactivatedCount =
        result.rows[0]?.fn_deactivate_gps_onibus_estado_by_ordens || 0;

    if (deactivatedCount > 0) {
        console.log(
            `[Rio][gps_onibus_estado] Registros desativados: ${deactivatedCount}`
        );
    }

    return deactivatedCount;
}

module.exports = {
    enrichRecordsWithSentido,
    saveRioToGpsSentido,
    saveRioToGpsOnibusEstado,
    deactivateInactiveOnibusEstado,
};
