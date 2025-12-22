const axios = require('axios');
const { saveRecordsToDb, ensureFuturePartitions } = require('./database');
const { API_TIMEZONE, formatDateInTimeZone } = require('./utils');
const { addPositions } = require('./rioOnibusStore');

async function fetchGPSData(windowInMinutes = 3, options = {}) {
    const { updateInMemoryStore = true } = options;
    const now = new Date();

    // overlap window configurável em minutos; default 3
    const startWindow = new Date(now.getTime() - windowInMinutes * 60 * 1000);

    const dataInicial = formatDateInTimeZone(startWindow, API_TIMEZONE);
    const dataFinal = formatDateInTimeZone(now, API_TIMEZONE);

    const startMsg = `Polling GPS data: ${dataInicial} to ${dataFinal}`;
    console.log(startMsg);

    // Log full URL used for fetchGPSData
    const urlBase = 'https://dados.mobilidade.rio/gps/sppo';
    const queryString = `?dataInicial=${encodeURIComponent(dataInicial)}&dataFinal=${encodeURIComponent(dataFinal)}`;
    const fullRequestUrl = `${urlBase}${queryString}`;
    console.log(`fetchGPSData URL: ${fullRequestUrl}`);

    try {
        const response = await axios.get(urlBase, {
            params: {
                dataInicial,
                dataFinal
            }
        });

        const records = response.data;
        if (updateInMemoryStore) addPositions(records);
        await saveRecordsToDb(records);

        const successMsg = `Success! Fetched ${records.length} records.`;
        console.log(successMsg);
    } catch (error) {
        const errorMsg = `Error fetching data: ${error.message}`;
        console.error(errorMsg);
    }
}

module.exports = {
    fetchGPSData,
};
