const axios = require('axios');
const { enrichRecordsWithSentido, saveRioToGpsSentido, saveRioToGpsProximidadeTerminalEvento, processarViagensRio } = require('../database/index');
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

async function fetchRioGPSData(options = {}) {
    const windowInMinutes = Number(options.windowInMinutes) || Number(process.env.RIO_POLLING_WINDOW_MINUTES) || 3;
 
    const now = new Date();

    // overlap window configurável em minutos; default 3
    const startWindow = new Date(now.getTime() - windowInMinutes * 60 * 1000);

    const dataInicial = formatDateInTimeZone(startWindow, API_TIMEZONE);
    const dataFinal = formatDateInTimeZone(now, API_TIMEZONE);

    // Log full URL used for fetchGPSData
    const urlBase = 'https://dados.mobilidade.rio/gps/sppo';

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
        
  

        // gps_proximidade_terminal_evento: recebe apenas registros únicos para análise
        if (options.saveToGpsProximidadeTerminalEvento) {
            await saveRioToGpsProximidadeTerminalEvento(latestRecords)
                .then(() => console.log(`[Rio][gps_proximidade_terminal_evento] Sucesso: ${latestRecords.length} registros`))
                .catch(err => console.error('[Rio][gps_proximidade_terminal_evento] Falha:', err.message))
        }
        

        const enrichedRecords = await enrichRecordsWithSentido(latestRecords);
        
        // processar viagens: recebe registros enriquecidos para detectar mudanças de sentido (ANTES do upsert)
        if (options.processarViagens) {
            try {
                await processarViagensRio(enrichedRecords)
                    .then(() => console.log(`[Rio][viagens] Sucesso: ${enrichedRecords.length} registros processados`))
                    .catch(err => console.error('[Rio][viagens] Falha:', err.message))
            } catch (err) {
                console.error('[Rio][viagens] processarViagensRio failed', err);
            }
        }

        // gps_sentido: recebe apenas os mais recentes (com sentido) (DEPOIS do processamento de viagens)
        if (options.saveToGpsSentido) {
            // Enrich apenas os registros únicos e usar retorno direto
            try {
                await saveRioToGpsSentido(enrichedRecords)
                    .then(() => console.log(`[Rio][gps_sentido] Sucesso: ${enrichedRecords.length} registros`))
                    .catch(err => console.error('[Rio][gps_sentido] Falha:', err.message))
            } catch (err) {
                console.error('[Rio][sentido] enrichRecordsWithSentido failed; continuing without sentido', err);
            }
        }

 

        if (options.updateInMemoryStore) await addPositions(latestRecords);

    } catch (error) {
        const errorMsg = `[Rio] Error fetching data: ${error.message}`;
        console.error(errorMsg);
    }
}

module.exports = {
    fetchRioGPSData,
};
