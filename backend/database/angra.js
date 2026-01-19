const { dbPool } = require('./pool');
const { API_TIMEZONE, formatDateInTimeZone } = require('../utils');
const { getItinerariosByLinha, isLoaded } = require('../stores/itinerarioStore');

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

async function enrichAngraRecordsWithSentido(records) {
    if (!Array.isArray(records) || records.length === 0) return records;

    if (!isLoaded()) {
        console.warn('[Angra][sentido] itinerario cache not loaded yet; skipping enrichment');
        for (const record of records) {
            record.sentido_enriched = null;
            record.sentido_itinerario_id = null;
            record.route_name = null;
        }
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

    return records;
}

async function saveAngraToGpsSentido(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;

    console.log(`[Angra][gps_sentido] Processing ${records.length} records`);

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
            token: 'Bonfim'
        }));
        console.log(recordsJson.length)
        try {
            await dbPool.query(
                'SELECT fn_upsert_gps_sentido_angra_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
            console.log(`[Angra][gps_sentido] Successfully processed batch of ${recordsJson.length} records`);
        } catch (err) {
            console.error('[Angra][gps_sentido] Error inserting records:', err.message);
        }

        
        try {
            await dbPool.query(
                'select * from gps.ftdbgps_atualiza_gps_sentido()'
            );
        } catch (err) {
            console.error('[Rio] Erro executing atualiza_gps_sentido query', err);
        }
    }
}

module.exports = {
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
};
