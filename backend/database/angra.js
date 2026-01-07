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
        console.log(`[Angra] All ${records.length} records filtered out (outside ${RETENTION_DAYS} day window: ${minDate.toISOString()} to ${now.toISOString()})`);
        return;
    }

    if (skippedCount > 0) {
        console.log(`[Angra] Filtered ${skippedCount} records outside ${RETENTION_DAYS} day window: ${minDate.toISOString()} to ${now.toISOString()}`);
    }

    for (let i = 0; i < filteredRecords.length; i += BATCH_SIZE) {
        const batch = filteredRecords.slice(i, i + BATCH_SIZE);

        const values = [];
        const placeholders = [];

        batch.forEach((record, index) => {
            values.push(
                record.VehicleIntegrationCode,
                record.VehicleDescription,
                record.LineIntegrationCode,
                record.LineNumber,
                record.LineDescription,
                record.RouteIntegrationCode,
                record.RouteDirection,
                record.RouteDescription,
                record.EstimatedDepartureDate,
                record.EstimatedArrivalDate,
                record.RealDepartureDate,
                record.RealArrivalDate,
                record.Shift,
                record.Latitude,
                record.Longitude,
                record.EventDate,
                record.UpdateDate,
                record.Speed,
                record.Direction,
                record.EventCode,
                record.EventName,
                record.IsRouteStartPoint,
                record.IsRouteEndPoint,
                record.IsGarage,
                record.LicensePlate,
                record.ClientBusIntegrationCode,
                record.RouteType
            );

            const baseIndex = index * PARAMS_PER_ROW;
            const params = [];
            for (let p = 1; p <= PARAMS_PER_ROW; p++) {
                params.push(`$${baseIndex + p}`);
            }
            placeholders.push(`(${params.join(', ')})`);
        });

        const text = `
            INSERT INTO gps_posicoes_angra (
                vehicle_integration_code,
                vehicle_description,
                line_integration_code,
                line_number,
                line_description,
                route_integration_code,
                route_direction,
                route_description,
                estimated_departure_date,
                estimated_arrival_date,
                real_departure_date,
                real_arrival_date,
                shift,
                latitude,
                longitude,
                event_date,
                update_date,
                speed,
                direction,
                event_code,
                event_name,
                is_route_start_point,
                is_route_end_point,
                is_garage,
                license_plate,
                client_bus_integration_code,
                route_type
            ) VALUES
                ${placeholders.join(',\n')}
            ON CONFLICT ON CONSTRAINT gps_posicoes_angra_unique_ponto DO NOTHING;
        `;

        try {
            await dbPool.query(text, values);
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
    const PARAMS_PER_ROW = 10;


    // Filtra registros que estão dentro do período de retenção (partições existentes)
    const now = new Date(new Date().toLocaleString('en-US', { timeZone: API_TIMEZONE }));
    const minDate = new Date(now.getTime() - RETENTION_DAYS * 24 * 60 * 60 * 1000);
    const filteredRecords = records.filter(record => {
        const EventDate = new Date(record.EventDate);
        return EventDate >= minDate && EventDate <= now;
    });

    const skippedCount = records.length - filteredRecords.length;
    if (filteredRecords.length === 0) {
        console.log(`[Angra] All ${records.length} records filtered out (outside ${RETENTION_DAYS} day window: ${minDate.toISOString()} to ${now.toISOString()})`);
        return;
    }

    if (skippedCount > 0) {
        console.log(`[Angra] Filtered ${skippedCount} records outside ${RETENTION_DAYS} day window: ${minDate.toISOString()} to ${now.toISOString()}`);
    }


    for (let i = 0; i < filteredRecords.length; i += BATCH_SIZE) {
        const batch = filteredRecords.slice(i, i + BATCH_SIZE);

        const values = [];
        const placeholders = [];

        batch.forEach((record, index) => {
            const datahoraTimestamp = record.EventDate;

            values.push(
                record.VehicleIntegrationCode,
                datahoraTimestamp,
                record.LineNumber,
                Number(record.Latitude),
                Number(record.Longitude),
                Number(record.Speed),
                record.sentido_enriched || null,
                record.sentido_itinerario_id || null,
                record.route_name || null,
                'Bonfim'
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
            console.error('[Angra][gps_sentido] Error inserting records:', err.message);
        }
    }
}

module.exports = {
    saveAngraRecordsToDb,
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
};
