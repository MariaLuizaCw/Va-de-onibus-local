const { dbPool } = require('./pool');
const { formatDateInTimeZone } = require('../utils');
const { logJobExecution } = require('./jobLogs');

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 200;

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
            metodo_inferencia: row.metodo_inferencia,
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



async function cleanupHistoricoViagens() {
    const retentionDays = Number(process.env.HISTORICO_VIAGENS_RETENTION_DAYS) || 30;
    
    try {
        const result = await dbPool.query(
            'SELECT fn_cleanup_historico_viagens($1::integer)',
            [retentionDays]
        );
        
        const deletedCount = result.rows.length > 0 ? (result.rows[0].deleted_count || 0) : 0;
        
        console.log(`[Cleanup] Histórico de viagens: ${deletedCount} registros removidos`);
        
    } catch (error) {
        console.error('[Cleanup] Error cleaning historico viagens:', error.message);
        throw error;
    }
}

async function saveRioGpsApiHistory(records) {
    if (!records || records.length === 0) return;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalInserted = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // Salvar dados brutos exatamente como chegam da API
        const recordsJson = batch.map(record => ({
            ordem: record.ordem,
            latitude: record.latitude,
            longitude: record.longitude,
            datahora: record.datahora,
            velocidade: record.velocidade,
            linha: record.linha,
            datahoraenvio: record.datahoraenvio,
            datahoraservidor: record.datahoraservidor
        }));

        try {
            const result = await dbPool.query(
                'SELECT * FROM fn_insert_rio_gps_api_history($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            const insertedCount = result.rows.length > 0 ? (result.rows[0].inserted_count || 0) : 0;
            totalInserted += insertedCount;
        } catch (err) {
            console.error('[Rio][gps_api_history] Error inserting raw history:', err.message);
        }
    }

    // Log da subtask
    const finishedAt = new Date();
    const durationMs = finishedAt - startedAt;
    
    await logJobExecution({
        jobName: 'saveRioGpsApiHistory',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs,
        status: 'success',
        infoMessage: `${totalInserted} registros brutos salvos no histórico`
    });

    return totalInserted;
}

async function cleanupRioGpsApiHistory() {
    const retentionDays = Number(process.env.RIO_GPS_API_HISTORY_RETENTION_DAYS) || 7;
    
    try {
        const result = await dbPool.query(
            'SELECT * FROM fn_cleanup_rio_gps_api_history($1::integer)',
            [retentionDays]
        );
        
        const deletedCount = result.rows.length > 0 ? (result.rows[0].deleted_count || 0) : 0;
        
        console.log(`[Cleanup] Rio GPS API History: ${deletedCount} registros removidos (retenção: ${retentionDays} dias)`);
        
        return deletedCount;
    } catch (error) {
        console.error('[Cleanup] Error cleaning Rio GPS API history:', error.message);
        throw error;
    }
}

async function processarViagensRio(records) {
    if (!records || records.length === 0) return;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const recordsJson = batch.map(record => {
            // datahora pode vir como string (já formatado) ou como milissegundos
            let datahoraTimestamp = record.datahora;
            if (typeof record.datahora === 'number' || !isNaN(Number(record.datahora))) {
                const datahoraMs = Number(record.datahora);
                if (Number.isFinite(datahoraMs) && datahoraMs > 1000000000000) {
                    datahoraTimestamp = formatDateInTimeZone(new Date(datahoraMs));
                }
            }

            return {
                ordem: record.ordem,
                datahora: datahoraTimestamp,
                linha: record.linha,
                sentido: record.sentido || null,
                sentido_itinerario_id: record.sentido_itinerario_id || null,
                metodo_inferencia: record.metodo_inferencia || null,
                metadados: record.metadados || null,
                token: record.token || 'PMRJ'
            };
        });
        
        try {
            await dbPool.query(
                'SELECT fn_processar_viagens_rio($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            totalProcessed += batch.length;
        } catch (err) {
            console.error('[Rio][viagens] Error processing trips:', err.message);
        }
    }

    // Log da subtask
    const finishedAt = new Date();
    const durationMs = finishedAt - startedAt;
    
    await logJobExecution({
        jobName: 'processarViagensRio',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados para viagens`
    });

    return totalProcessed;
}

const ULTIMA_PASSAGEM_PROXIMITY_METERS = Number(process.env.ULTIMA_PASSAGEM_PROXIMITY_METERS) || 150;
const ULTIMA_PASSAGEM_WINDOW_MINUTES = Number(process.env.ULTIMA_PASSAGEM_WINDOW_MINUTES) || 30;
const ULTIMA_PASSAGEM_MIN_DURATION_MINUTES = Number(process.env.ULTIMA_PASSAGEM_MIN_DURATION_MINUTES) || 8;

// Configurações para nova lógica de detecção de sentido
const SENTIDO_MAX_IDADE_ULTIMA_PASSAGEM_MIN = Number(process.env.SENTIDO_MAX_IDADE_ULTIMA_PASSAGEM_MIN) || 15;
const SENTIDO_MAX_DISTANCIA_ROTA_METROS = Number(process.env.SENTIDO_MAX_DISTANCIA_ROTA_METROS) || 300;
const SENTIDO_MAX_DISTANCIA_FALLBACK_METROS = Number(process.env.SENTIDO_MAX_DISTANCIA_FALLBACK_METROS) || 1000;

async function saveRioToGpsUltimaPassagem(records) {
    if (!records || records.length === 0) return 0;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const pointsJson = batch.map(record => {
            const datahoraMs = Number(record.datahora);
            const datahoraTimestamp = Number.isFinite(datahoraMs)
                ? formatDateInTimeZone(new Date(datahoraMs))
                : null;

            return {
                ordem: String(record.ordem),
                linha: String(record.linha),
                lat: parseFloat(String(record.latitude).replace(',', '.')),
                lon: parseFloat(String(record.longitude).replace(',', '.')),
                datahora: datahoraTimestamp
            };
        });

        try {
            await dbPool.query(
                'SELECT fn_atualizar_ultima_passagem($1::jsonb)',
                [JSON.stringify(pointsJson)]
            );
            totalProcessed += batch.length;
        } catch (err) {
            console.error('[Rio][gps_ultima_passagem] Error updating records:', err.message);
        }
    }

    // Log da subtask
    const finishedAt = new Date();
    const durationMs = finishedAt - startedAt;
    
    await logJobExecution({
        jobName: 'saveRioToGpsUltimaPassagem',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados para última passagem`
    });

    return totalProcessed;
}

// =============================================================================
// NOVA LÓGICA DE DETECÇÃO DE SENTIDO (2 etapas: ultima_passagem + fallback)
// =============================================================================

/**
 * Atualiza a tabela auxiliar de últimas 5 posições por ônibus/linha
 * Essa tabela é usada pela nova lógica de detecção de sentido
 */
async function atualizarUltimasPosicoes(records) {
    if (!records || records.length === 0) return { inseridos: 0, removidos: 0 };
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalInseridos = 0;
    let totalRemovidos = 0;

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
                ordem: String(record.ordem),
                linha: String(record.linha),
                datahora: datahoraTimestamp,
                latitude: lat,
                longitude: lon,
                velocidade: Number(record.velocidade) || null
            };
        });

        try {
            const result = await dbPool.query(
                'SELECT * FROM fn_atualizar_ultimas_posicoes($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            if (result.rows.length > 0) {
                totalInseridos += Number(result.rows[0].registros_inseridos) || 0;
                totalRemovidos += Number(result.rows[0].registros_removidos) || 0;
            }
        } catch (err) {
            console.error('[Rio][ultimas_posicoes] Error updating records:', err.message);
        }
    }

    const finishedAt = new Date();
    await logJobExecution({
        jobName: 'atualizarUltimasPosicoes',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
        status: 'success',
        infoMessage: `${totalInseridos} inseridos, ${totalRemovidos} removidos`
    });

    return { inseridos: totalInseridos, removidos: totalRemovidos };
}

/**
 * Processa detecção de sentido usando nova lógica (2 etapas)
 * Retorna registros enriquecidos com sentido, metodo_detecao, etc.
 * NÃO faz upsert - apenas enriquece os dados
 * em_terminal é consultado diretamente da tabela gps_ultima_passagem pela função SQL
 * @param {Array} records - Registros GPS
 * @param {string} token - Token de identificação
 */
async function processarSentidoNovaLogica(records, token = 'PMRJ') {
    if (!records || records.length === 0) return { processados: 0, comSentido: 0, garagem: 0, registros: [] };
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    const allEnrichedRecords = [];

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
                ordem: String(record.ordem),
                linha: String(record.linha),
                datahora: datahoraTimestamp,
                latitude: lat,
                longitude: lon,
                velocidade: Number(record.velocidade) || null
            };
        });

        try {
            // Detecta sentido - já retorna route_name e consulta em_terminal da tabela
            const result = await dbPool.query(
                'SELECT * FROM fn_processar_sentido_batch($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            
            // Mapear sentido por ordem|linha (já inclui route_name e em_terminal)
            const sentidoMap = new Map();
            for (const row of result.rows) {
                sentidoMap.set(`${row.ordem}|${row.linha}`, row);
            }

            // Enriquecer registros
            for (const rec of recordsJson) {
                const key = `${rec.ordem}|${rec.linha}`;
                const sentidoInfo = sentidoMap.get(key);

                // Metadados já incluem em_terminal da função SQL
                let metadados = sentidoInfo?.json_pontos_avaliados || {};
                if (typeof metadados === 'string') {
                    try { metadados = JSON.parse(metadados); } catch (e) { metadados = {}; }
                }

                allEnrichedRecords.push({
                    ordem: rec.ordem,
                    linha: rec.linha,
                    datahora: rec.datahora,
                    latitude: rec.latitude,
                    longitude: rec.longitude,
                    velocidade: rec.velocidade,
                    sentido: sentidoInfo?.sentido || null,
                    sentido_itinerario_id: sentidoInfo?.itinerario_id || null,
                    route_name: sentidoInfo?.route_name || rec.linha,
                    token: token,
                    metodo_inferencia: sentidoInfo?.metodo_detecao || null,
                    metadados: metadados
                });
            }
        } catch (err) {
            console.error('[Rio][sentido_nova_logica] Error processing records:', err.message);
        }
    }

    // Calcular métricas
    const totalProcessados = allEnrichedRecords.length;
    const totalComSentido = allEnrichedRecords.filter(r => r.sentido && r.sentido !== 'GARAGEM').length;
    const totalGaragem = allEnrichedRecords.filter(r => r.sentido === 'GARAGEM').length;

    const finishedAt = new Date();
    await logJobExecution({
        jobName: 'processarSentidoNovaLogica',
        parentJob: 'rio-gps-fetch',
        subtask: true,
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
        status: 'success',
        infoMessage: `${totalProcessados} processados, ${totalComSentido} com sentido, ${totalGaragem} garagem`
    });

    return { 
        processados: totalProcessados, 
        comSentido: totalComSentido, 
        garagem: totalGaragem,
        registros: allEnrichedRecords
    };
}

/**
 * Faz upsert em gps_sentido com registros já enriquecidos
 */
async function upsertGpsSentidoBatch(enrichedRecords) {
    if (!enrichedRecords || enrichedRecords.length === 0) return 0;
    
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalUpserted = 0;

    for (let i = 0; i < enrichedRecords.length; i += BATCH_SIZE) {
        const batch = enrichedRecords.slice(i, i + BATCH_SIZE);
        
        try {
            const result = await dbPool.query(
                'SELECT * FROM fn_upsert_gps_sentido_rio_batch_json($1::jsonb)',
                [JSON.stringify(batch)]
            );
            if (result.rows.length > 0) {
                totalUpserted += Number(result.rows[0].registros_processados) || 0;
            }
        } catch (err) {
            console.error('[Rio][upsert_gps_sentido] Error:', err.message);
        }
    }

    return totalUpserted;
}

/**
 * Cleanup da tabela auxiliar de últimas posições
 */
async function cleanupUltimasPosicoes() {
    const retentionHours = Number(process.env.ULTIMAS_POSICOES_RETENTION_HOURS) || 2;
    
    try {
        const result = await dbPool.query(
            'SELECT * FROM fn_cleanup_ultimas_posicoes($1::integer)',
            [retentionHours]
        );
        
        const deletedCount = result.rows.length > 0 ? (result.rows[0].deleted_count || 0) : 0;
        
        console.log(`[Cleanup] Últimas posições: ${deletedCount} registros removidos (retenção: ${retentionHours}h)`);
        
        return deletedCount;
    } catch (error) {
        console.error('[Cleanup] Error cleaning ultimas posicoes:', error.message);
        throw error;
    }
}

module.exports = {
    enrichRecordsWithSentido,
    saveRioToGpsSentido,
    processarViagensRio,
    cleanupHistoricoViagens,
    saveRioGpsApiHistory,
    cleanupRioGpsApiHistory,
    saveRioToGpsUltimaPassagem,
    // Nova lógica de detecção de sentido
    atualizarUltimasPosicoes,
    processarSentidoNovaLogica,
    upsertGpsSentidoBatch,
    cleanupUltimasPosicoes,
};
