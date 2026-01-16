const { dbPool } = require('./pool');
const { API_TIMEZONE, formatDateInTimeZone } = require('../utils');
const { getItinerariosByLinha, isLoaded } = require('../itinerarioStore');
const RETENTION_DAYS = Number(process.env.PARTITION_RETENTION_DAYS) || 7;

async function saveAngraRecordsToDb(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 27;
    

    // Filtra registros que estão dentro do período de retenção (partições existentes)
    const now = new Date(new Date().toLocaleString('en-US', { timeZone: API_TIMEZONE }));
    const minDate = new Date(now.getTime() - RETENTION_DAYS * 24 * 60 * 60 * 1000);
    const filteredRecords = records.filter(record => {
        const updateDate = new Date(record.UpdateDate);
        return updateDate >= minDate && updateDate <= now;
    });

    const skippedCount = records.length - filteredRecords.length;
    if (filteredRecords.length === 0) {
        return;
    }

    if (skippedCount > 0) {
    }

    for (let i = 0; i < filteredRecords.length; i += BATCH_SIZE) {
        const batch = filteredRecords.slice(i, i + BATCH_SIZE);

        const recordsJson = batch.map(record => ({
            vehicle_integration_code: record.VehicleIntegrationCode,
            vehicle_description: record.VehicleDescription,
            line_integration_code: record.LineIntegrationCode,
            line_number: record.LineNumber,
            line_description: record.LineDescription,
            route_integration_code: record.RouteIntegrationCode,
            route_direction: record.RouteDirection,
            route_description: record.RouteDescription,
            estimated_departure_date: record.EstimatedDepartureDate,
            estimated_arrival_date: record.EstimatedArrivalDate,
            real_departure_date: record.RealDepartureDate,
            real_arrival_date: record.RealArrivalDate,
            shift: record.Shift,
            latitude: record.Latitude,
            longitude: record.Longitude,
            event_date: record.EventDate,
            update_date: record.UpdateDate,
            speed: record.Speed,
            direction: record.Direction,
            event_code: record.EventCode,
            event_name: record.EventName,
            is_route_start_point: record.IsRouteStartPoint,
            is_route_end_point: record.IsRouteEndPoint,
            is_garage: record.IsGarage,
            license_plate: record.LicensePlate,
            client_bus_integration_code: record.ClientBusIntegrationCode,
            route_type: record.RouteType
        }));

        try {
            await dbPool.query(
                'SELECT fn_insert_gps_posicoes_angra_batch_json($1::jsonb)',
                [JSON.stringify(recordsJson)]
            );
        } catch (err) {
            console.error('[Angra] Error inserting GPS records into database:', err);
        }
    }
}

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
    saveAngraRecordsToDb,
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
};
