const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');
const { logJobExecution } = require('./jobLogs');

const PROXIMITY_EVENT_RETENTION_HOURS = Number(process.env.PROXIMITY_EVENT_RETENTION_HOURS) || 8;



const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;
const TERMINAL_PASSAGE_DISTANCE_METERS = Number(process.env.TERMINAL_PASSAGE_DISTANCE_METERS) || 20;
const TERMINAL_PROXIMITY_DISTANCE_METERS = Number(process.env.TERMINAL_PROXIMITY_DISTANCE_METERS) || 100;
const PROXIMITY_WINDOW_MINUTES = Number(process.env.PROXIMITY_WINDOW_MINUTES) || 15;
const PROXIMITY_MIN_DURATION_MINUTES = Number(process.env.PROXIMITY_MIN_DURATION_MINUTES) || 10;

async function enrichRecordsWithSentido(records) {
    if (!records || records.length === 0) return [];
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;
    let totalEnriched = 0;
    const allEnrichedRecords = [];
    
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
                'SELECT * FROM fn_enrich_gps_batch_with_sentido_json($1::jsonb, $2, $3, $4, $5, $6)',
                [
                    JSON.stringify(pointsJson), 
                    MAX_SNAP_DISTANCE_METERS,
                    TERMINAL_PASSAGE_DISTANCE_METERS,
                    TERMINAL_PROXIMITY_DISTANCE_METERS,
                    PROXIMITY_WINDOW_MINUTES,
                    PROXIMITY_MIN_DURATION_MINUTES
                ]
            );
        } catch (err) {
            console.error('[sentido] Error computing sentido via PostGIS:', err);
            continue;
        }

        // Usar diretamente result.rows para gps_sentido - já está no formato correto
        const enrichedRecords = result.rows.map(row => ({
            ordem: String(row.ordem),
            linha: String(row.linha),
            sentido: row.sentido,
            sentido_itinerario_id: row.itinerario_id,
            route_name: row.route_name,
            distancia_metros: row.dist_m,
            longitude: batch.find(r => r.ordem === row.ordem && r.linha === row.linha)?.longitude,
            latitude: batch.find(r => r.ordem === row.ordem && r.linha === row.linha)?.latitude,
            datahora: batch.find(r => r.ordem === row.ordem && r.linha === row.linha)?.datahora,
            velocidade: batch.find(r => r.ordem === row.ordem && r.linha === row.linha)?.velocidade
        })); // Incluir todos, inclusive null (garagem)
        allEnrichedRecords.push(...enrichedRecords);
        totalEnriched += enrichedRecords.length;
        totalProcessed += batch.length;
    }

    // Log execution metrics
    const finishedAt = new Date();
    await logJobExecution({
        jobName: 'enrich-rio-sentido',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados, ${totalEnriched} registros enriquecidos`
    });

    return allEnrichedRecords;
}


async function saveRioToGpsSentido(records) {
    if (!records || records.length === 0) return;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;

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
            totalProcessed += batch.length;
        } catch (err) {
            console.error('[Rio][gps_sentido] Error upserting records:', err.message);
        }
    }

    // Log da subtask
    const finishedAt = new Date();
    const durationMs = finishedAt - startedAt;
    
    await logJobExecution({
        jobName: 'saveRioToGpsSentido',
        parentJob: 'angra-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados`
    });
}



async function saveRioToGpsProximidadeTerminalEvento(records) {
    if (!records || records.length === 0) return;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

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
                datahora: datahoraTimestamp,
                linha: record.linha,
                lon: lon,
                lat: lat
            };
        });

        try {
            await dbPool.query(
                'SELECT fn_insert_gps_proximidade_terminal_evento_json($1::jsonb, $2)',
                [JSON.stringify(pointsJson), MAX_SNAP_DISTANCE_METERS]
            );
            totalProcessed += batch.length;
        } catch (err) {
            console.error('[Rio][gps_proximidade_terminal_evento] Error inserting records:', err.message);
        }
    }

    // Log da subtask
    const finishedAt = new Date();
    const durationMs = finishedAt - startedAt;
    
    await logJobExecution({
        jobName: 'saveRioToGpsProximidadeTerminalEvento',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados`
    });
}

async function cleanupProximityEvents() {
    // Apenas executa o cleanup - logging é feito pelo scheduler
    try {
        // Usar função SQL em vez de DELETE direto
        const result = await dbPool.query(
            'SELECT * FROM fn_cleanup_gps_proximidade_terminal_evento($1)',
            [PROXIMITY_EVENT_RETENTION_HOURS]
        );
        
        const deletedCount = result.rows.length > 0 ? (result.rows[0].deleted_count || 0) : 0;
        
        console.log(`[Cleanup] Proximity events: ${deletedCount} registros removidos`);
        
    } catch (error) {
        console.error('[Cleanup] Error cleaning proximity events:', error.message);
        throw error;
    }
}

module.exports = {
    enrichRecordsWithSentido,
    saveRioToGpsSentido,
    saveRioToGpsProximidadeTerminalEvento,
    cleanupProximityEvents,
};
