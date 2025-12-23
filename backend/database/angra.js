const { dbPool } = require('./pool');
const { API_TIMEZONE } = require('../utils');

async function saveAngraRecordsToDb(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 2000;
    const PARAMS_PER_ROW = 26;
    const RETENTION_DAYS = Number(process.env.PARTITION_RETENTION_DAYS) || 7;

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
                record.ClientBusIntegrationCode
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
                client_bus_integration_code
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

module.exports = {
    saveAngraRecordsToDb,
};
