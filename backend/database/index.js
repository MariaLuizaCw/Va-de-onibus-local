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
const { enrichSsxRecordsWithSentido, saveSsxToGpsSentido } = require('./ssx');
const { enrichGtfsRecordsWithSentido, saveGtfsToGpsSentido, identificarSentido, enrichVehicles } = require('./gtfs');
const { 
    loadOnibusSnapshot, 
    saveOnibusSnapshot, 
    loadLatestRioOnibusSnapshot, 
    saveRioOnibusSnapshot,
    syncRioSnapshot
} = require('./snapshots');

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
    // SSX (Angra, Barra do Piraí, Pedro Antônio, Resendense)
    enrichSsxRecordsWithSentido,
    saveSsxToGpsSentido,
    // GTFS
    enrichGtfsRecordsWithSentido,
    saveGtfsToGpsSentido,
    identificarSentido,
    enrichVehicles,
    // Snapshots (apenas Rio)
    saveOnibusSnapshot,
    loadOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    syncRioSnapshot,
};
