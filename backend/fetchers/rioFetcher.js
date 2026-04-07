const axios = require('axios');
const { 
    saveRioGpsApiHistory, 
    saveRioToGpsUltimaPassagem,
    atualizarUltimasPosicoes,
    processarSentidoNovaLogica,
    upsertGpsSentidoBatch,
    processarViagensRio,
} = require('../database/index');
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
        
        // Salvar histórico bruto ANTES de qualquer transformação (incluindo deduplicação)
        if (options.saveRawHistory) {
            await saveRioGpsApiHistory(records)
                .catch(err => console.error('[Rio][gps_api_history] Falha:', err.message));
        }
        
        // Deduplicar: manter apenas o registro mais recente de cada ordem
        // 22k registros → ~3k registros únicos
        const latestRecords = deduplicateByOrdem(records);

        // atualizarUltimasPosicoes: atualiza tabela auxiliar de últimas 5 posições
        if (options.atualizarUltimasPosicoes) {
            await atualizarUltimasPosicoes(latestRecords)
                .catch(err => console.error('[Rio][ultimas_posicoes] Falha:', err.message));
        }

        // gps_ultima_passagem: atualiza tabela de última passagem usando regra B (150m, 8min)
        // Também atualiza coluna em_terminal na tabela
        if (options.saveToGpsUltimaPassagem) {
            try {
                const totalProcessed = await saveRioToGpsUltimaPassagem(latestRecords);
            } catch (err) {
                console.error('[Rio][gps_ultima_passagem] Falha:', err.message);
            }
        }

        // Fluxo: enriquecer → processar viagens → upsert gps_sentido
        let enrichedRecords = [];
        if (options.saveToGpsSentido || options.processarViagens) {
            // 1. Detectar sentido e enriquecer (em_terminal é consultado da tabela gps_ultima_passagem)
            const result = await processarSentidoNovaLogica(latestRecords, 'PMRJ');
            enrichedRecords = result.registros || [];
        }
        
        // 2. Processar viagens ANTES do upsert (usa dados enriquecidos)
        if (options.processarViagens && enrichedRecords.length > 0) {
            await processarViagensRio(enrichedRecords)
                .catch(err => console.error('[Rio][viagens] Falha:', err.message));
        }

        // 3. Fazer upsert em gps_sentido por último
        if (options.saveToGpsSentido && enrichedRecords.length > 0) {
            const upserted = await upsertGpsSentidoBatch(enrichedRecords);
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
