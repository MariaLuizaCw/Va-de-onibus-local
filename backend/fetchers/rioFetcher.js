const axios = require('axios');
const { enrichRecordsWithSentido, saveRioRecordsToDb, saveRioToGpsSentido, saveRioToGpsOnibusEstado } = require('../database/index');
const { API_TIMEZONE, formatDateInTimeZone } = require('../utils');
const { addPositions } = require('../stores/rioOnibusStore');

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
        if (!skipEnrich) {
            try {
                await enrichRecordsWithSentido(records);
            } catch (err) {
                console.error('[Rio][sentido] enrichRecordsWithSentido failed; continuing without sentido', err);
            }
        }
        if (updateInMemoryStore) addPositions(records);
        
        if (saveToDb) {
            saveRioRecordsToDb(records).catch(err => {
                console.error('[Rio][gps_posicoes] Async insert failed:', err.message);
            });
        }

        if (saveToGpsSentido) {
            saveRioToGpsSentido(records).catch(err => {
                console.error('[Rio][gps_sentido] Async insert failed:', err.message);
            });
        }

        if (saveToGpsOnibusEstado) {
            saveRioToGpsOnibusEstado(records).catch(err => {
                console.error('[Rio][gps_onibus_estado] Async upsert failed:', err.message);
            });
        }
    } catch (error) {
        const errorMsg = `[Rio] Error fetching data: ${error.message}`;
        console.error(errorMsg);
    }
}

module.exports = {
    fetchRioGPSData,
};
