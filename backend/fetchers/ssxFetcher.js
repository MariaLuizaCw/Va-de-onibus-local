/**
 * Fetcher unificado para integrações SSX (SystemsATX)
 * Suporta múltiplas viações: Angra (Bonfim), Barra do Piraí, Pedro Antônio, Resendense
 */

const axios = require('axios');
const { enrichSsxRecordsWithSentido, saveSsxToGpsSentido } = require('../database/index');
const { logJobExecution } = require('../database/jobLogs');
const { addPositions, getCompanyVehicles, getLineLastPositions, getLoadedCompanies, getStoreStatus } = require('../stores/ssxVehiclesStore');

const SSX_BASE_URL = 'https://integration.systemsatx.com.br';

// Configuração das viações SSX
// Cada viação tem suas próprias credenciais e token
const SSX_INTEGRATIONS = {
    angra: {
        name: 'Angra',
        token: 'Bonfim',  // Token usado na tabela gps_sentido
        envPrefix: 'ANGRA_SSX',
        jobPrefix: 'angra'
    },
    barradopirai: {
        name: 'Barra do Piraí',
        token: 'BarraPirai',
        envPrefix: 'BARRAPIRAI_SSX',
        jobPrefix: 'barrapirai'
    },
    pedroantonio: {
        name: 'Pedro Antônio',
        token: 'PedroAntonio',
        envPrefix: 'PEDROANTONIO_SSX',
        jobPrefix: 'pedroantonio'
    },
    resendense: {
        name: 'Resendense',
        token: 'Resendense',
        envPrefix: 'RESENDENSE_SSX',
        jobPrefix: 'resendense'
    }
};

// Cache de tokens por integração
const tokenCache = new Map();

// Deduplicar registros por VehicleId, mantendo apenas o mais recente de cada
function deduplicateByVehicleId(records) {
    const byVehicle = new Map();
    for (const record of records) {
        const key = String(record.VehicleIntegrationCode);
        const existing = byVehicle.get(key);
        const currentTime = new Date(record.PositionDateTime).getTime();
        const existingTime = existing ? new Date(existing.PositionDateTime).getTime() : 0;
        if (!existing || currentTime > existingTime) {
            byVehicle.set(key, record);
        }
    }
    return Array.from(byVehicle.values());
}

// Obtém configuração de ambiente para uma integração
function getEnvConfig(integrationKey) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    if (!integration) {
        throw new Error(`[SSX] Integração desconhecida: ${integrationKey}`);
    }

    const prefix = integration.envPrefix;
    return {
        username: process.env[`${prefix}_USERNAME`],
        password: process.env[`${prefix}_PASSWORD`],
        clientCode: process.env[`${prefix}_CLIENT_CODE`],
        tokenRefreshMs: Number(process.env[`${prefix}_TOKEN_REFRESH_MS`]) || 18000000 // 5 horas default
    };
}

// Login para obter token
async function login(integrationKey) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    const config = getEnvConfig(integrationKey);

    if (!config.username || !config.password) {
        throw new Error(`[${integration.name}] Credenciais não configuradas (${integration.envPrefix}_USERNAME, ${integration.envPrefix}_PASSWORD)`);
    }

    const loginUrl = `${SSX_BASE_URL}/Login`;
    const params = new URLSearchParams({
        Username: config.username,
        Password: config.password,
        ClientIntegrationCodeBus: config.clientCode,
    });

    try {
        const response = await axios.post(`${loginUrl}?${params.toString()}`, '', {
            headers: { 'accept': 'application/json' }
        });

        const { AccessToken } = response.data;

        if (!AccessToken) {
            throw new Error(`[${integration.name}] Login response missing AccessToken`);
        }

        const token = decodeURIComponent(AccessToken);
        
        tokenCache.set(integrationKey, {
            token,
            expiresAt: Date.now() + config.tokenRefreshMs
        });

        return token;
    } catch (error) {
        console.error(`[${integration.name}] Login failed:`, error.message);
        throw error;
    }
}

// Obtém token (do cache ou faz login)
async function getToken(integrationKey) {
    const cached = tokenCache.get(integrationKey);
    if (cached && Date.now() < cached.expiresAt) {
        return cached.token;
    }
    return await login(integrationKey);
}

// Obtém token com logging
async function getTokenLogged(integrationKey) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    const tokenStartedAt = new Date();
    const cached = tokenCache.get(integrationKey);
    const wasFromCache = cached && Date.now() < cached.expiresAt;
    
    const token = await getToken(integrationKey);
    const tokenFinishedAt = new Date();

    await logJobExecution({
        jobName: `${integration.jobPrefix}-get-token`,
        parentJob: `${integration.jobPrefix}-gps-fetch`,
        subtask: true,
        startedAt: tokenStartedAt,
        finishedAt: tokenFinishedAt,
        durationMs: tokenFinishedAt - tokenStartedAt,
        status: 'success',
        infoMessage: wasFromCache ? 'Token cacheado' : 'Novo token'
    });

    return token;
}

// Requisição à API com logging
async function apiRequestLogged(integrationKey, token) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    const apiStartedAt = new Date();
    
    const response = await axios.post(
        `${SSX_BASE_URL}/GlobalBus/LastPosition/List`,
        [],
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

    await logJobExecution({
        jobName: `${integration.jobPrefix}-api-request`,
        parentJob: `${integration.jobPrefix}-gps-fetch`,
        subtask: true,
        startedAt: apiStartedAt,
        finishedAt: apiFinishedAt,
        durationMs: apiFinishedAt - apiStartedAt,
        status: 'success',
        infoMessage: `${records.length} registros recebidos`
    });

    return response;
}

// Busca linhas circulares (opcional, usado por algumas integrações)
async function fetchCircularLines(integrationKey) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    const config = getEnvConfig(integrationKey);
    
    try {
        const token = await getToken(integrationKey);
        const payload = {
            ClientIntegrationCode: config.clientCode,
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
            console.warn(`[${integration.name}] Circular lines response malformed`);
            return new Set();
        }

        const circularNumbers = lines
            .filter(line => line.IsCircularLine === true)
            .map(line => String(line.Number || '').trim())
            .filter(Boolean);
        return new Set(circularNumbers);
    } catch (error) {
        console.error(`[${integration.name}] Failed to fetch circular lines:`, error.message);
        return new Set();
    }
}

// Função principal de fetch para uma integração específica
async function fetchSsxGPSData(integrationKey, options = {}) {
    const integration = SSX_INTEGRATIONS[integrationKey];
    
    if (!integration) {
        console.error(`[SSX] Integração desconhecida: ${integrationKey}`);
        return;
    }

    try {
        const token = await getTokenLogged(integrationKey);
        const response = await apiRequestLogged(integrationKey, token);
        const records = response.data;

        if (!Array.isArray(records)) {
            return;
        }

        // Deduplicar: manter apenas o registro mais recente de cada VehicleId
        const latestRecords = deduplicateByVehicleId(records);

        // Enriquecer com dados da tabela itinerario
        try {
            await enrichSsxRecordsWithSentido(latestRecords, integration.jobPrefix);
        } catch (err) {
            console.error(`[${integration.name}][sentido] enrichSsxRecordsWithSentido failed:`, err.message);
        }

        // Atualizar store em memória
        if (options.updateInMemoryStore) {
            addPositions(integrationKey, latestRecords);
        }

        // Salvar em gps_sentido
        if (options.saveToGpsSentido) {
            saveSsxToGpsSentido(latestRecords, integration.token, integration.jobPrefix)
                .catch(err => console.error(`[${integration.name}][gps_sentido] Falha:`, err.message));
        }

    } catch (error) {
        console.error(`[${integration.name}] Error fetching data:`, error.message);
        
        // Se não autorizado, limpa token do cache
        if (error.response?.status === 401) {
            tokenCache.delete(integrationKey);
        }
    }
}

// Handlers específicos para cada viação (usados pelo scheduler)
async function fetchAngraGPSData(options = {}) {
    return fetchSsxGPSData('angra', options);
}

async function fetchBarraPiraiGPSData(options = {}) {
    return fetchSsxGPSData('barradopirai', options);
}

async function fetchPedroAntonioGPSData(options = {}) {
    return fetchSsxGPSData('pedroantonio', options);
}

async function fetchResendenseGPSData(options = {}) {
    return fetchSsxGPSData('resendense', options);
}

module.exports = {
    // Handlers específicos
    fetchAngraGPSData,
    fetchBarraPiraiGPSData,
    fetchPedroAntonioGPSData,
    fetchResendenseGPSData,
    // Funções auxiliares
    fetchSsxGPSData,
    fetchCircularLines,
    getToken,
    login,
    SSX_INTEGRATIONS,
    // Acesso ao store em memória
    getCompanyVehicles,
    getLineLastPositions,
    getLoadedCompanies,
    getStoreStatus,
};
