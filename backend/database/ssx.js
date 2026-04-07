/**
 * Funções de database para integrações SSX (SystemsATX)
 * Suporta múltiplas viações: Angra (Bonfim), Barra do Piraí, Pedro Antônio, Resendense
 */

const { dbPool } = require('./pool');
const { getItinerariosByLinha, isLoaded } = require('../stores/itinerarioStore');
const { logJobExecution } = require('./jobLogs');

// Normaliza string para comparação (lowercase, trim, remove acentos)
function normalizeString(str) {
    if (!str) return '';
    return String(str)
        .toLowerCase()
        .trim()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[\/\(\)\-,.:;]/g, ' ');
}

function tokenize(normalizedStr) {
    return normalizedStr.split(/\s+/).filter(t => t.length > 0);
}

function tokenOverlapRatio(tokensA, tokensB) {
    const setB = new Set(tokensB);
    let overlap = 0;
    for (const t of tokensA) {
        if (setB.has(t)) overlap++;
    }
    const maxLen = Math.max(tokensA.length, tokensB.length);
    return maxLen === 0 ? 0 : overlap / maxLen;
}

/**
 * Enriquece registros SSX com sentido usando cache de itinerários
 * @param {Array} records - Registros da API SSX
 * @param {string} jobPrefix - Prefixo do job para logging (ex: 'angra', 'barrapirai')
 */
async function enrichSsxRecordsWithSentido(records, jobPrefix = 'ssx') {
    const enrichStartedAt = new Date();
    
    if (!Array.isArray(records) || records.length === 0) return records;

    if (!isLoaded()) {
        console.warn(`[${jobPrefix}][sentido] itinerario cache not loaded yet; skipping enrichment`);
        for (const record of records) {
            record.sentido_enriched = null;
            record.sentido_itinerario_id = null;
            record.route_name = null;
        }
        
        const enrichFinishedAt = new Date();
        await logJobExecution({
            jobName: `${jobPrefix}-enrich-sentido`,
            parentJob: `${jobPrefix}-gps-fetch`,
            subtask: true,
            startedAt: enrichStartedAt,
            finishedAt: enrichFinishedAt,
            durationMs: enrichFinishedAt - enrichStartedAt,
            status: 'success',
            infoMessage: 'Cache não carregado, 0 registros enriquecidos'
        });
        
        return records;
    }

    for (const record of records) {
        const lineNumber = String(record.LineNumber || '').trim();
        const routeDesc = normalizeString(record.RouteDescription);
        const routeTokens = tokenize(routeDesc);
        const candidates = getItinerariosByLinha(lineNumber);

        let bestMatch = null;
        let bestRatio = 0;

        for (const candidate of candidates) {
            const sentidoNormalized = normalizeString(candidate.sentido);
            if (!sentidoNormalized) continue;
            const sentidoTokens = tokenize(sentidoNormalized);
            const ratio = tokenOverlapRatio(routeTokens, sentidoTokens);
            if (ratio > bestRatio) {
                bestRatio = ratio;
                bestMatch = candidate;
            }
        }

        if (bestMatch && bestRatio > 0) {
            record.sentido_enriched = bestMatch.sentido;
            record.sentido_itinerario_id = bestMatch.itinerario_id || null;
            record.route_name = bestMatch.route_name || null;
        } else {
            record.sentido_enriched = null;
            record.sentido_itinerario_id = null;
            record.route_name = null;
        }
    }

    const enrichFinishedAt = new Date();
    await logJobExecution({
        jobName: `${jobPrefix}-enrich-sentido`,
        parentJob: `${jobPrefix}-gps-fetch`,
        subtask: true,
        startedAt: enrichStartedAt,
        finishedAt: enrichFinishedAt,
        durationMs: enrichFinishedAt - enrichStartedAt,
        status: 'success',
        infoMessage: `${records.length} registros enriquecidos`
    });

    return records;
}

/**
 * Salva registros SSX na tabela gps_sentido
 * @param {Array} records - Registros enriquecidos
 * @param {string} token - Token da viação (ex: 'Bonfim', 'BarraPirai')
 * @param {string} jobPrefix - Prefixo do job para logging
 */
async function saveSsxToGpsSentido(records, token, jobPrefix = 'ssx') {
    if (!records || records.length === 0) return;
    
    const startedAt = new Date();
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 400;
    let totalProcessed = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const recordsJson = batch.map(record => ({
            ordem: record.VehicleIntegrationCode,
            datahora: record.EventDate,
            linha: record.LineNumber,
            latitude: Number(record.Latitude),
            longitude: Number(record.Longitude),
            velocidade: Number(record.Speed),
            sentido: record.sentido_enriched || null,
            sentido_itinerario_id: record.sentido_itinerario_id || null,
            route_name: record.route_name || null,
            token: token
        }));
        
        try {
            await dbPool.query(
                'SELECT fn_upsert_gps_sentido_ssx_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            totalProcessed += batch.length;
        } catch (err) {
            console.error(`[${jobPrefix}][gps_sentido] Error inserting records:`, err.message);
        }

        try {
            await dbPool.query('SELECT * FROM gps.ftdbgps_atualiza_gps_sentido()');
        } catch (err) {
            console.error(`[${jobPrefix}] Erro executing atualiza_gps_sentido query`, err.message);
        }
    }

    const finishedAt = new Date();
    
    await logJobExecution({
        jobName: `${jobPrefix}-save-gps-sentido`,
        parentJob: `${jobPrefix}-gps-fetch`,
        subtask: true,
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
        status: 'success',
        infoMessage: `${totalProcessed} registros processados`
    });
}

// Aliases para compatibilidade com código existente (Angra)
async function enrichAngraRecordsWithSentido(records) {
    return enrichSsxRecordsWithSentido(records, 'angra');
}

async function saveAngraToGpsSentido(records) {
    return saveSsxToGpsSentido(records, 'Bonfim', 'angra');
}

module.exports = {
    enrichSsxRecordsWithSentido,
    saveSsxToGpsSentido,
    // Aliases para compatibilidade
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
};
