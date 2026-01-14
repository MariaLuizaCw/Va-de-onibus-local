const axios = require('axios');
const { enrichRecordsWithSentido, saveRioRecordsToDb, saveRioToGpsSentido, saveRioToGpsOnibusEstado, deactivateInactiveOnibusEstado } = require('../database/index');
const { API_TIMEZONE, formatDateInTimeZone } = require('../utils');
const { addPositions } = require('../stores/rioOnibusStore');

// Deduplicar registros por ordem, mantendo apenas o mais recente de cada
function deduplicateByOrdem(records) {
    const byOrdem = new Map();
    for (const record of records) {
        const key = String(record.ordem);
        const existing = byOrdem.get(key);
        if (!existing || Number(record.datahora) > Number(existing.datahora)) {
            byOrdem.set(key, record);
        }
    }
    return Array.from(byOrdem.values());
}

async function fetchRioGPSData(windowInMinutes = null, options = {}) {
    if (windowInMinutes === null) {
        windowInMinutes = Number(process.env.RIO_POLLING_WINDOW_MINUTES) || Number(process.env.POLLING_WINDOW_MINUTES) || 3;
    }
    const { 
        updateInMemoryStore = true, 
        skipEnrich = false,
        saveToDb = true,
        saveToGpsSentido = true,
        saveToGpsOnibusEstado = true
    } = options;
    const now = new Date();

    // overlap window configurável em minutos; default 3
    const startWindow = new Date(now.getTime() - windowInMinutes * 60 * 1000);

    const dataInicial = formatDateInTimeZone(startWindow, API_TIMEZONE);
    const dataFinal = formatDateInTimeZone(now, API_TIMEZONE);

    // Log full URL used for fetchGPSData
    const urlBase = 'https://dados.mobilidade.rio/gps/sppo';
    const queryString = `?dataInicial=${encodeURIComponent(dataInicial)}&dataFinal=${encodeURIComponent(dataFinal)}`;
    const fullRequestUrl = `${urlBase}${queryString}`;

    try {
        const response = await axios.get(urlBase, {
            params: {
                dataInicial,
                dataFinal
            }
        });

        const records = response.data;
        
        // Deduplicar: manter apenas o registro mais recente de cada ordem
        // 22k registros → ~3k registros únicos
        const latestRecords = deduplicateByOrdem(records);
        console.log(`[Rio] ${records.length} registros → ${latestRecords.length} únicos por ordem`);

        // Enrich apenas os registros únicos (7x mais rápido)
        if (!skipEnrich) {
            try {
                await enrichRecordsWithSentido(latestRecords);
            } catch (err) {
                console.error('[Rio][sentido] enrichRecordsWithSentido failed; continuing without sentido', err);
            }
        }

        // Store em memória recebe apenas os mais recentes (já enriquecidos)


        const dbPromises = [];
        
        // gps_posicoes: recebe TODOS os registros (histórico completo)
        if (saveToDb) {
            dbPromises.push(
                saveRioRecordsToDb(records)
                    .then(() => console.log(`[Rio][gps_posicoes] Sucesso: ${records.length} registros`))
                    .catch(err => console.error('[Rio][gps_posicoes] Falha:', err.message))
            );
        }


           // gps_onibus_estado: recebe apenas os mais recentes (com sentido)
        if (saveToGpsOnibusEstado) {
            dbPromises.push(
                saveRioToGpsOnibusEstado(records)
                    .then(() => console.log(`[Rio][gps_onibus_estado] Sucesso: ${latestRecords.length} registros`))
                    .catch(err => console.error('[Rio][gps_onibus_estado] Falha:', err.message))
            );
        }

        // gps_sentido: recebe apenas os mais recentes (com sentido)
        if (saveToGpsSentido) {
            dbPromises.push(
                saveRioToGpsSentido(latestRecords)
                    .then(() => console.log(`[Rio][gps_sentido] Sucesso: ${latestRecords.length} registros`))
                    .catch(err => console.error('[Rio][gps_sentido] Falha:', err.message))
            );
        }

     

        // Executar em paralelo
        await Promise.allSettled(dbPromises);


        if (updateInMemoryStore) await addPositions(latestRecords);


        // Deactivate só depois que todos salvaram
        if (saveToGpsOnibusEstado) {
            deactivateInactiveOnibusEstado()
                .then(count => {
                    if (count > 0) console.log(`[Rio][gps_onibus_estado] Desativados ${count} ônibus inativos`);
                })
                .catch(err => console.error('[Rio][gps_onibus_estado] Falha ao desativar inativos:', err.message));
        }

    } catch (error) {
        const errorMsg = `[Rio] Error fetching data: ${error.message}`;
        console.error(errorMsg);
    }
}

module.exports = {
    fetchRioGPSData,
};
