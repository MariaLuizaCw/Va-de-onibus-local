const axios = require('axios');
const { enrichAngraRecordsWithSentido, saveAngraToGpsSentido } = require('../database/index');
const { addPositions } = require('../stores/angraOnibusStore');
const { logJobExecution } = require('../database/jobLogs');

const SSX_BASE_URL = 'https://integration.systemsatx.com.br';

// Deduplicar registros por VehicleId, mantendo apenas o mais recente de cada
function deduplicateByVehicleId(records) {
    const byVehicle = new Map();
    for (const record of records) {
        const key = String(record.VehicleIntegrationCode);
        const existing = byVehicle.get(key);
        // Comparar por PositionDateTime (ISO string ou timestamp)
        const currentTime = new Date(record.PositionDateTime).getTime();
        const existingTime = existing ? new Date(existing.PositionDateTime).getTime() : 0;
        if (!existing || currentTime > existingTime) {
            byVehicle.set(key, record);
        }
    }
    return Array.from(byVehicle.values());
}

let cachedToken = null;
let tokenExpiresAt = null;
let circularLinesCache = new Set();

async function login() {
    const username = process.env.ANGRA_SSX_USERNAME;
    const password = process.env.ANGRA_SSX_PASSWORD;
    const clientIntegrationCodeBus = process.env.ANGRA_SSX_CLIENT_CODE;

    if (!username || !password) {
        throw new Error('[Angra] Missing ANGRA_SSX_USERNAME or ANGRA_SSX_PASSWORD environment variables');
    }


    const loginUrl = `${SSX_BASE_URL}/Login`;
    const params = new URLSearchParams({
        Username: username,
        Password: password,
        ClientIntegrationCodeBus: clientIntegrationCodeBus,
    });

    try {
        const response = await axios.post(`${loginUrl}?${params.toString()}`, '', {
            headers: {
                'accept': 'application/json',
            }
        });

        const { AccessToken, ExpiresIn } = response.data;

        if (!AccessToken) {
            throw new Error('[Angra] Login response missing AccessToken');
        }

        // Decode URL-encoded token
        cachedToken = decodeURIComponent(AccessToken);
        
        // Token refresh interval from env (default 5 hours)
        const tokenRefreshMs = Number(process.env.ANGRA_SSX_TOKEN_REFRESH_MS) || (5 * 60 * 60 * 1000);
        tokenExpiresAt = Date.now() + tokenRefreshMs;

        return cachedToken;
    } catch (error) {
        console.error('[Angra] Login failed:', error.message);
        throw error;
    }
}

async function getTokenLogged() {
    const tokenStartedAt = new Date();
    const token = await getToken();
    const tokenFinishedAt = new Date();

    // Log da subtask de obtenção de token
    await logJobExecution({
        jobName: 'angra-get-token',
        parentJob: 'angra-gps-fetch',
        subtask: true,
        startedAt: tokenStartedAt,
        finishedAt: tokenFinishedAt,
        durationMs: tokenFinishedAt - tokenStartedAt,
        status: 'success',
        infoMessage: cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt ? 'Token cacheado' : 'Novo token'
    });

    return token;
}

async function apiRequestLogged(token) {
    const apiStartedAt = new Date();
    
    const response = await axios.post(
        `${SSX_BASE_URL}/GlobalBus/LastPosition/List`,
        [],  // Empty array as body
        {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json',
            }
        }
    );
    
    const apiFinishedAt = new Date();
    const records = response.data;

    // Log da subtask de requisição API
    await logJobExecution({
        jobName: 'angra-api-request',
        parentJob: 'angra-gps-fetch',
        subtask: true,
        startedAt: apiStartedAt,
        finishedAt: apiFinishedAt,
        durationMs: apiFinishedAt - apiStartedAt,
        status: 'success',
        infoMessage: `${records.length} registros recebidos`
    });

    return response;
}

async function getToken() {
    // Return cached token if still valid
    if (cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt) {
        return cachedToken;
    }

    // Otherwise, login again
    return await login();
}

async function fetchCircularLines() {
    try {
        const token = await getToken();
        const payload = {
            ClientIntegrationCode: process.env.ANGRA_SSX_CLIENT_CODE,
            QueryConditions: [],
        };
        const response = await axios.post(`${SSX_BASE_URL}/GlobalBus/Line/ListLine`, payload, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json',
            }
        });

        const lines = response.data;
        if (!Array.isArray(lines)) {
            console.warn('[Angra] Circular lines response malformed');
            return;
        }

        const circularNumbers = lines
            .filter(line => line.IsCircularLine === true)
            .map(line => String(line.Number || '').trim())
            .filter(Boolean);
        circularLinesCache = new Set(circularNumbers);
    } catch (error) {
        console.error('[Angra] Failed to fetch circular lines:', error.message);
    }
}

async function fetchAngraGPSData(options = {}) {
    
    try {
        const token = await getTokenLogged();
        const response = await apiRequestLogged(token);
        const records = response.data;

        if (!Array.isArray(records)) {
            return;
        }

        // Deduplicar: manter apenas o registro mais recente de cada VehicleId
        const latestRecords = deduplicateByVehicleId(records);

        // Enriquecer com dados da tabela itinerario
        try {
            await enrichAngraRecordsWithSentido(latestRecords);
        } catch (err) {
            console.error('[Angra][sentido] enrichAngraRecordsWithSentido failed; continuing without sentido', err.message);
        }

        if (options.updateInMemoryStore) {
            addPositions(latestRecords);
        }

        if (options.saveToGpsSentido) {
            saveAngraToGpsSentido(latestRecords)
                .then(() => console.log(`[Angra][gps_sentido] Sucesso: ${latestRecords.length} registros`))
                .catch(err => console.error('[Angra][gps_sentido] Falha:', err.message));
        }

    } catch (error) {
        console.error(`[Angra] Error fetching data: ${error.message}`);
        
        // If unauthorized, clear cached token to force re-login
        if (error.response?.status === 401) {
            cachedToken = null;
            tokenExpiresAt = null;
        }
    }
}

module.exports = {
    fetchAngraGPSData,
    login,
    getToken,
    fetchCircularLines,
};
