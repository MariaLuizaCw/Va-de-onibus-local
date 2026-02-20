const { dbPool } = require('./pool');
const { enrichRecordsWithSentido, saveRioToGpsSentido, saveRioToGpsProximidadeTerminalEvento, processarViagensRio, cleanupProximityEvents, cleanupHistoricoViagens } = require('./rio');
const { enrichAngraRecordsWithSentido, saveAngraToGpsSentido } = require('./angra');
const { enrichGtfsRecordsWithSentido, saveGtfsToGpsSentido, identificarSentido, enrichVehicles } = require('./gtfs');
const { 
    loadOnibusSnapshot, 
    saveOnibusSnapshot, 
    loadLatestRioOnibusSnapshot, 
    saveRioOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    saveAngraOnibusSnapshot,
    syncRioSnapshot,
    syncAngraSnapshot,
    syncRioItaSnapshot
} = require('./snapshots');
const { generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('./reports');

module.exports = {
    dbPool,
    enrichRecordsWithSentido,
    saveRioToGpsSentido,
    saveRioToGpsProximidadeTerminalEvento,
    processarViagensRio,
    cleanupProximityEvents,
    cleanupHistoricoViagens,
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
    enrichGtfsRecordsWithSentido,
    saveGtfsToGpsSentido,
    identificarSentido,
    enrichVehicles,
    saveOnibusSnapshot,
    loadOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    saveAngraOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    syncRioSnapshot,
    syncAngraSnapshot,
    syncRioItaSnapshot,
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
};
