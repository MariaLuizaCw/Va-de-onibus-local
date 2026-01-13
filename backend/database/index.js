const { dbPool } = require('./pool');
const { enrichRecordsWithSentido, saveRioRecordsToDb, saveRioToGpsSentido, saveRioToGpsOnibusEstado, deactivateInactiveOnibusEstado } = require('./rio');
const { saveAngraRecordsToDb, enrichAngraRecordsWithSentido, saveAngraToGpsSentido } = require('./angra');
const { 
    loadOnibusSnapshot, 
    saveOnibusSnapshot, 
    loadLatestRioOnibusSnapshot, 
    saveRioOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    saveAngraOnibusSnapshot 
} = require('./snapshots');
const { ensureFuturePartitions } = require('./partitions');
const { generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('./reports');

module.exports = {
    dbPool,
    enrichRecordsWithSentido,
    saveRioRecordsToDb,
    saveRioToGpsSentido,
    saveRioToGpsOnibusEstado,
    deactivateInactiveOnibusEstado,
    saveAngraRecordsToDb,
    enrichAngraRecordsWithSentido,
    saveAngraToGpsSentido,
    saveOnibusSnapshot,
    loadOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    saveAngraOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    ensureFuturePartitions,
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
};
