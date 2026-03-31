const { dbPool } = require('./pool');
const { 
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
} = require('./rio');
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
    // Angra
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
