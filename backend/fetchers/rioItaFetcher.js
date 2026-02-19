const axios = require('axios');
const { addPosition, getRioItaOnibus } = require('../stores/rioItaStore');
const { logJobExecution } = require('../database/jobLogs');

const CITTATI_BASE_URL = 'http://servicos.cittati.com.br/WSIntegracaoCittati';

let cachedToken = null;
let tokenExpiresAt = null;
let cachedEmpresas = [];

async function login() {
    const username = process.env.RIOITA_CITTATI_USERNAME;
    const password = process.env.RIOITA_CITTATI_PASSWORD;

    if (!username || !password) {
        throw new Error('[RioIta] Missing RIOITA_CITTATI_USERNAME or RIOITA_CITTATI_PASSWORD environment variables');
    }

    const loginUrl = `${CITTATI_BASE_URL}/Autenticacao/AutenticarUsuario`;
    const params = new URLSearchParams({
        usuario: username,
        senha: password
    });

    try {
        const response = await axios.get(`${loginUrl}?${params.toString()}`, {
            headers: {
                'accept': 'application/json'
            }
        });

        const { identificacaoLogin, empresas, retornoOK } = response.data;

        if (!retornoOK || !identificacaoLogin) {
            throw new Error('[RioIta] Login response invalid or retornoOK is false');
        }

        cachedToken = String(identificacaoLogin);
        cachedEmpresas = empresas || [];
        
        // Token refresh interval from env (default 4 hours)
        const tokenRefreshMs = Number(process.env.RIOITA_TOKEN_REFRESH_MS) || (4 * 60 * 60 * 1000);
        tokenExpiresAt = Date.now() + tokenRefreshMs;

        console.log(`[RioIta] Login successful. ${cachedEmpresas.length} empresas disponíveis`);

        return { token: cachedToken, empresas: cachedEmpresas };
    } catch (error) {
        console.error('[RioIta] Login failed:', error.message);
        throw error;
    }
}

async function getToken() {
    if (cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt) {
        return { token: cachedToken, empresas: cachedEmpresas };
    }
    return await login();
}

async function getTokenLogged() {
    const tokenStartedAt = new Date();
    const result = await getToken();
    const tokenFinishedAt = new Date();

    await logJobExecution({
        jobName: 'rioita-get-token',
        parentJob: 'rioita-gps-fetch',
        subtask: true,
        startedAt: tokenStartedAt,
        finishedAt: tokenFinishedAt,
        durationMs: tokenFinishedAt - tokenStartedAt,
        status: 'success',
        infoMessage: cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt ? 'Token cacheado' : 'Novo token'
    });

    return result;
}

async function fetchVeiculosForEmpresa(token, empresa) {
    const url = `${CITTATI_BASE_URL}/Operacional/veiculos`;
    const params = new URLSearchParams({ empresa });

    try {
        const response = await axios.get(`${url}?${params.toString()}`, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        });

        const { campos, dados } = response.data;

        if (!Array.isArray(campos) || !Array.isArray(dados)) {
            return [];
        }

        // Mapear dados para objetos usando os campos como chaves
        const records = dados.map(row => {
            const record = {};
            campos.forEach((campo, index) => {
                record[campo] = row[index];
            });
            record._empresa = empresa;
            return record;
        });

        return records;
    } catch (error) {
        console.error(`[RioIta] Error fetching veiculos for empresa ${empresa}:`, error.message);
        return [];
    }
}

async function fetchRioItaGPSData(options = {}) {
    try {
        const { token, empresas } = await getTokenLogged();

        const apiStartedAt = new Date();
        let allRecords = [];

        // Buscar veículos de todas as empresas em paralelo
        const promises = empresas.map(empresa => fetchVeiculosForEmpresa(token, empresa));
        const results = await Promise.all(promises);

        for (const records of results) {
            allRecords = allRecords.concat(records);
        }

        const apiFinishedAt = new Date();

        await logJobExecution({
            jobName: 'rioita-api-request',
            parentJob: 'rioita-gps-fetch',
            subtask: true,
            startedAt: apiStartedAt,
            finishedAt: apiFinishedAt,
            durationMs: apiFinishedAt - apiStartedAt,
            status: 'success',
            infoMessage: `${allRecords.length} registros de ${empresas.length} empresas`
        });

        if (options.updateInMemoryStore) {
            for (const record of allRecords) {
                addPosition(record);
            }
            console.log(`[RioIta] Store atualizado com ${allRecords.length} registros`);
        }

    } catch (error) {
        console.error(`[RioIta] Error fetching data: ${error.message}`);

        // If unauthorized, clear cached token to force re-login
        if (error.response?.status === 401) {
            cachedToken = null;
            tokenExpiresAt = null;
            cachedEmpresas = [];
        }
    }
}

module.exports = {
    fetchRioItaGPSData,
    login,
    getToken
};
