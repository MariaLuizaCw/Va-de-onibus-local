const { dbPool } = require('./pool');
const { enrichRecordsWithSentido, saveRioToGpsSentido, saveRioToGpsProximidadeTerminalEvento, processarViagensRio, cleanupProximityEvents, cleanupHistoricoViagens } = require('./rio');
const { enrichAngraRecordsWithSentido, saveAngraToGpsSentido } = require('./angra');
const { 
    loadOnibusSnapshot, 
    saveOnibusSnapshot, 
    loadLatestRioOnibusSnapshot, 
    saveRioOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    saveAngraOnibusSnapshot,
    syncRioSnapshot,
    syncAngraSnapshot
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
    saveOnibusSnapshot,
    loadOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    saveAngraOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    syncRioSnapshot,
    syncAngraSnapshot,
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
};
