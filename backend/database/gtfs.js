/**
 * Funções de database para GTFS-RT
 * Inspirado em angra.js e rio.js
 */

const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');
const { logJobExecution } = require('./jobLogs');
const { getRouteShortName, getShapeByDirection } = require('../stores/gtfsRoutesStore');

/**
 * Identifica o sentido correto usando function do banco
 * @param {string} numeroLinha - Número da linha (route_short_name)
 * @param {Object} startCoord - Coordenada inicial { lat, lon }
 * @param {Object} endCoord - Coordenada final { lat, lon }
 * @param {number} maxDistance - Distância máxima em metros (default 300)
 * @returns {Object|null} { sentido, itinerario_id } ou null
 */
async function identificarSentido(numeroLinha, startCoord, endCoord, maxDistance = 300) {
    if (!numeroLinha || !startCoord || !endCoord) return null;

    try {
        // Especificar apenas as colunas que precisamos
        const query = 'SELECT itinerario_id, sentido, route_name FROM fn_identificar_sentido_gtfs($1, $2, $3, $4, $5, $6)';

        const result = await dbPool.query(query, [
            numeroLinha,
            startCoord.lon,
            startCoord.lat,
            endCoord.lon,
            endCoord.lat,
            maxDistance
        ]);

        if (result.rows.length > 0) {
            return {
                sentido: result.rows[0].sentido,
                itinerario_id: result.rows[0].itinerario_id,
                route_name: result.rows[0].route_name
            };
        }

        return null;
    } catch (error) {
        console.error(`[GTFS-RT][database] Erro ao identificar sentido para linha ${numeroLinha}:`, error.message);
        return null;
    }
}

/**
 * Enriquece múltiplos registros GTFS-RT com sentido
 * Usa function batch com JOINs (sem loops no banco)
 * @param {Array} records - Array de registros enriquecidos com _enriched
 * @param {Object} options - Opções de processamento
 * @returns {Array} Array de registros com sentido adicionado
 */
async function enrichGtfsRecordsWithSentido(records, options = {}) {
    const enrichStartedAt = new Date();
    
    if (!Array.isArray(records) || records.length === 0) return records;

    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;
    let totalEnriched = 0;
    const maxDistance = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

    console.log(`[GTFS-RT][database] Iniciando enriquecimento de ${records.length} registros`);

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);
        
        // Prepara registros para a function batch
        const recordsForDb = [];
        const recordsMap = new Map(); // Mapeia index -> record original

        for (let j = 0; j < batch.length; j++) {
            const record = batch[j];
            totalProcessed++;
            const enriched = record._enriched;
            
            if (!enriched) {
                record._enriched = { sentido: null, itinerario_id: null, route_name: null };
                continue;
            }

            const { numeroLinha, directionId, routeId, company } = enriched;
            
            // Pular se não tiver dados necessários
            if (!numeroLinha || directionId === undefined || !routeId) {
                record._enriched.sentido = null;
                record._enriched.itinerario_id = null;
                record._enriched.route_name = null;
                continue;
            }

            // Obter shape correspondente ao directionId
            const shape = getShapeByDirection(company, routeId, directionId);
            
            if (!shape?.start_coord || !shape?.end_coord) {
                record._enriched.sentido = null;
                record._enriched.itinerario_id = null;
                record._enriched.route_name = null;
                continue;
            }

            // Adiciona ao array para processar em batch
            const dbIndex = recordsForDb.length + 1; // 1-indexed para o banco
            recordsForDb.push({
                numeroLinha,
                startCoord: shape.start_coord,
                endCoord: shape.end_coord
            });
            recordsMap.set(dbIndex, record);
        }

        // Se não tem registros para processar, continua
        if (recordsForDb.length === 0) continue;

        try {
            // Chama function batch (usa JOINs, sem loop no banco)
            const result = await dbPool.query(
                'SELECT * FROM fn_identificar_sentido_gtfs_batch($1::jsonb, $2)',
                [JSON.stringify(recordsForDb), maxDistance]
            );

            // Aplica resultados aos registros originais
            for (const row of result.rows) {
                const record = recordsMap.get(row.record_index);
                if (record) {
                    record._enriched.sentido = row.sentido;
                    record._enriched.itinerario_id = row.itinerario_id;
                    record._enriched.route_name = row.route_name;
                    totalEnriched++;
                }
            }

            // Marca registros sem match como null
            for (const [idx, record] of recordsMap) {
                if (record._enriched.sentido === undefined) {
                    record._enriched.sentido = null;
                    record._enriched.itinerario_id = null;
                    record._enriched.route_name = null;
                }
            }
        } catch (error) {
            console.error(`[GTFS-RT][database] Erro no batch enrich:`, error.message);
            // Em caso de erro, marca todos como null
            for (const record of recordsMap.values()) {
                record._enriched.sentido = null;
                record._enriched.itinerario_id = null;
                record._enriched.route_name = null;
            }
        }
    }

    const enrichFinishedAt = new Date();
    
    // Log do enriquecimento
    await logJobExecution({
        jobName: 'gtfs-enrich-sentido',
        parentJob: 'gtfs-gps-fetch',
        subtask: true,
        startedAt: enrichStartedAt,
        finishedAt: enrichFinishedAt,
        durationMs: enrichFinishedAt - enrichStartedAt,
        status: 'success',
        infoMessage: `${totalEnriched}/${totalProcessed} registros enriquecidos com sentido`
    });

    console.log(`[GTFS-RT][database] Enriquecimento concluído: ${totalEnriched}/${totalProcessed} registros com sentido`);
    
    return records;
}

/**
 * Salva registros GTFS-RT na tabela gps_sentido usando function do banco
 * @param {Array} records - Array de registros GTFS-RT
 * @returns {Promise<void>}
 */
async function saveGtfsToGpsSentido(records) {
    if (!Array.isArray(records) || records.length === 0) return;

    const saveStartedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalSaved = 0;

    console.log(`[GTFS-RT][database] Salvando ${records.length} registros em gps_sentido`);

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // Prepara registros para a function
        const recordsJson = batch.map(record => {
            const vehicle = record.vehicle;
            const enriched = record._enriched || {};
            
            // Extrair dados do veículo
            const position = vehicle?.position;
            const timestamp = parseInt(vehicle?.timestamp || '0', 10);
            const vehicleId = vehicle?.vehicle?.id;
            
            if (!position || !timestamp || !vehicleId) return null;

            // Converter timestamp para datetime com timezone correto
            const datetime = formatDateInTimeZone(new Date(timestamp * 1000));

            return {
                token: enriched.company || 'GTFS',  // token = empresa (nome da companhia)
                linha: enriched.numeroLinha,
                ordem: vehicleId,
                latitude: position.latitude,
                longitude: position.longitude,
                datahora: datetime,
                sentido: enriched.sentido || null,  // sentido é text na tabela
                sentido_itinerario_id: enriched.itinerario_id || null,  // id da tabela itinerario
                route_name: enriched.route_name || null,  // nome da rota da tabela itinerario
                velocidade: position.speed || null   // velocidade do GTFS-RT
            };
        }).filter(record => record !== null); // Remove registros nulos

        if (recordsJson.length === 0) continue;

        // Usa a function do banco para salvar em batch
        const result = await dbPool.query('SELECT fn_save_gtfs_gps_batch($1::jsonb) as saved_count', [
            JSON.stringify(recordsJson)
        ]);

        totalSaved += parseInt(result.rows[0].saved_count || '0', 10);
    }

    const saveFinishedAt = new Date();
    
    // Log do salvamento
    await logJobExecution({
        jobName: 'gtfs-save-gps-sentido',
        parentJob: 'gtfs-gps-fetch',
        subtask: true,
        startedAt: saveStartedAt,
        finishedAt: saveFinishedAt,
        durationMs: saveFinishedAt - saveStartedAt,
        status: 'success',
        infoMessage: `${totalSaved} registros salvos em gps_sentido`
    });

    console.log(`[GTFS-RT][database] Salvamento concluído: ${totalSaved} registros em gps_sentido`);
}

/**
 * Enriquece veículos com número da linha
 * O enriquecimento com sentido é feito pela função enrichGtfsRecordsWithSentido
 */
async function enrichVehicles(company, entities, options = {}) {
    const enriched = [];
    
    for (const entity of entities) {
        const vehicle = entity.vehicle;
        if (!vehicle) continue;

        const routeId = vehicle.trip?.routeId;
        const directionId = vehicle.trip?.directionId;
        
        // Obter número da linha
        const numeroLinha = routeId ? getRouteShortName(company, routeId) : null;
        
        // Dados básicos enriquecidos
        const enrichedVehicle = {
            ...entity,
            _enriched: {
                company,
                numeroLinha,
                routeId,
                directionId
            }
        };

        enriched.push(enrichedVehicle);
    }

    return enriched;
}

module.exports = {
    identificarSentido,
    enrichVehicles,
    enrichGtfsRecordsWithSentido,
    saveGtfsToGpsSentido
};
