const axios = require('axios');
const { saveAngraRecordsToDb, enrichAngraRecordsWithSentido, saveAngraToGpsSentido } = require('../database/index');
const { addPositions } = require('../stores/angraOnibusStore');

const SSX_BASE_URL = 'https://integration.systemsatx.com.br';

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

async function getToken() {
    // Return cached token if still valid
    if (cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt) {
        return cachedToken;
    }

    // Otherwise, login again
    return await login();
}

function getRouteType(record) {
    if (!record) return 'indefinido';

    if (record.IsGarage === true || String(record.IsGarage).toLowerCase() === 'true') {
        return 'garagem';
    }

    if (circularLinesCache.has(String(record.LineNumber))) {
        return 'circular';
    }


    if (record.RouteDirection === 1) {
        return 'ida';
    }

    if (record.RouteDirection === 2) {
        return 'volta';
    }

    return 'indefinido';
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
    const { 
        updateInMemoryStore = true,
        saveToDb = true,
        saveToGpsSentido = true,
        saveToGpsOnibusEstado = true
    } = options;

    try {
        const token = await getToken();

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

        const records = response.data;

        if (!Array.isArray(records)) {
            return;
        }

        const enhancedRecords = records.map(record => ({
            ...record,
            RouteType: getRouteType(record),
        }));

        // Enriquecer com dados da tabela itinerario
        try {
            await enrichAngraRecordsWithSentido(enhancedRecords);
        } catch (err) {
            console.error('[Angra][sentido] enrichAngraRecordsWithSentido failed; continuing without sentido', err.message);
        }

        if (updateInMemoryStore) {
            addPositions(enhancedRecords);
        }

        if (saveToDb) {
            saveAngraRecordsToDb(enhancedRecords)
                .then(() => console.log(`[Angra][gps_posicoes] Sucesso: ${enhancedRecords.length} registros`))
                .catch(err => console.error('[Angra][gps_posicoes] Falha:', err.message));
        }

        if (saveToGpsSentido) {
            saveAngraToGpsSentido(enhancedRecords)
                .then(() => console.log(`[Angra][gps_sentido] Sucesso: ${enhancedRecords.length} registros`))
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
