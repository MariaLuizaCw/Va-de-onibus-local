const { dbPool } = require('./pool');
const { enrichRecordsWithSentido, saveRioRecordsToDb } = require('./rio');
const { saveAngraRecordsToDb } = require('./angra');
const { 
    loadOnibusSnapshot, 
    saveOnibusSnapshot, 
    loadLatestRioOnibusSnapshot, 
    saveRioOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    saveAngraOnibusSnapshot 
} = require('./snapshots');
const { ensureFuturePartitions } = require('./partitions');
const { generateSentidoCoverageReport } = require('./reports');

module.exports = {
    dbPool,
    enrichRecordsWithSentido,
    saveRioRecordsToDb,
    saveAngraRecordsToDb,
    saveOnibusSnapshot,
    loadOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    saveAngraOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    ensureFuturePartitions,
    generateSentidoCoverageReport,
};
