const axios = require('axios');
const { enrichRecordsWithSentido, saveRioRecordsToDb } = require('../database/index');
const { API_TIMEZONE, formatDateInTimeZone } = require('../utils');
const { addPositions } = require('../stores/rioOnibusStore');

async function fetchRioGPSData(windowInMinutes = 3, options = {}) {
    const { updateInMemoryStore = true, skipEnrich = false } = options;
    const now = new Date();

    // overlap window configurável em minutos; default 3
    const startWindow = new Date(now.getTime() - windowInMinutes * 60 * 1000);

    const dataInicial = formatDateInTimeZone(startWindow, API_TIMEZONE);
    const dataFinal = formatDateInTimeZone(now, API_TIMEZONE);

    const startMsg = `[Rio] Polling GPS data: ${dataInicial} to ${dataFinal}`;
    console.log(startMsg);

    // Log full URL used for fetchGPSData
    const urlBase = 'https://dados.mobilidade.rio/gps/sppo';
    const queryString = `?dataInicial=${encodeURIComponent(dataInicial)}&dataFinal=${encodeURIComponent(dataFinal)}`;
    const fullRequestUrl = `${urlBase}${queryString}`;
    console.log(`[Rio] fetchGPSData URL: ${fullRequestUrl}`);

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
        await saveRioRecordsToDb(records);

        const successMsg = `[Rio] Success! Fetched ${records.length} records.`;
        console.log(successMsg);
    } catch (error) {
        const errorMsg = `[Rio] Error fetching data: ${error.message}`;
        console.error(errorMsg);
    }
}

module.exports = {
    fetchRioGPSData,
};
