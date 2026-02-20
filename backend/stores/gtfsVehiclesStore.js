/**
 * Store em memória para veículos GTFS-RT (por empresa)
 * Guarda últimas posições por linha, separado por empresa
 * NÃO salva sentido - apenas dados originais da API
 */

// Estrutura: { EMPRESA: { LINHA: { VEICULO_ID: [posições] } } }
const gtfsVehicles = Object.create(null);

/**
 * Garante que o bucket existe para empresa/linha/veículo
 */
function ensureBucket(company, linha, vehicleId) {
    const companyKey = String(company).toUpperCase();
    const linhaKey = String(linha);
    const vehicleKey = String(vehicleId);

    if (!gtfsVehicles[companyKey]) gtfsVehicles[companyKey] = Object.create(null);
    if (!gtfsVehicles[companyKey][linhaKey]) gtfsVehicles[companyKey][linhaKey] = Object.create(null);
    if (!gtfsVehicles[companyKey][linhaKey][vehicleKey]) gtfsVehicles[companyKey][linhaKey][vehicleKey] = [];

    return gtfsVehicles[companyKey][linhaKey][vehicleKey];
}

/**
 * Adiciona uma posição de veículo
 * @param {string} company - Nome da empresa
 * @param {Object} record - Registro do veículo (dados originais da API)
 * @param {string} linha - Número da linha (route_short_name)
 */
function addPosition(company, record, linha) {
    if (!record || !linha) return;
    
    const vehicleId = record.vehicle?.vehicle?.id;
    if (!vehicleId) return;

    const bucket = ensureBucket(company, linha, vehicleId);

    // Armazenar registro original da API
    bucket.push(record);

    // Ordenar por timestamp (mais recente primeiro)
    bucket.sort((a, b) => {
        const timeA = parseInt(a.vehicle?.timestamp || '0', 10);
        const timeB = parseInt(b.vehicle?.timestamp || '0', 10);
        return timeB - timeA;
    });

    // Manter apenas as 3 últimas posições
    if (bucket.length > 3) bucket.length = 3;
}

/**
 * Adiciona múltiplas posições para uma empresa
 * @param {string} company - Nome da empresa
 * @param {Array} records - Array de registros de veículos
 * @param {Function} getLinhaFn - Função para obter linha do registro (routeId -> route_short_name)
 */
function addPositions(company, records, getLinhaFn) {
    if (!Array.isArray(records) || records.length === 0) return;
    
    for (const record of records) {
        const routeId = record.vehicle?.trip?.routeId;
        if (!routeId) continue; // Veículo sem trip não tem linha
        
        const linha = getLinhaFn(routeId);
        if (!linha) continue;
        
        addPosition(company, record, linha);
    }
}

/**
 * Obtém todos os veículos de uma empresa
 * @param {string} company - Nome da empresa
 * @returns {Object} Estrutura { linha: [posições] }
 */
function getCompanyVehicles(company) {
    const companyKey = String(company).toUpperCase();
    const companyData = gtfsVehicles[companyKey];
    
    if (!companyData) return {};

    const result = Object.create(null);
    for (const linhaKey of Object.keys(companyData)) {
        result[linhaKey] = getLineLastPositions(company, linhaKey);
    }
    return result;
}

/**
 * Obtém última posição de cada veículo de uma linha
 * @param {string} company - Nome da empresa
 * @param {string} linha - Número da linha
 * @returns {Array} Array com última posição de cada veículo
 */
function getLineLastPositions(company, linha) {
    const companyKey = String(company).toUpperCase();
    const linhaKey = String(linha);
    
    const linhaData = gtfsVehicles[companyKey]?.[linhaKey];
    if (!linhaData) return [];

    const result = [];
    for (const vehicleKey of Object.keys(linhaData)) {
        const bucket = linhaData[vehicleKey];
        if (Array.isArray(bucket) && bucket.length > 0) {
            result.push(bucket[0]);
        }
    }
    return result;
}

/**
 * Obtém todas as empresas com veículos carregados
 * @returns {Array<string>} Lista de nomes das empresas
 */
function getLoadedCompanies() {
    return Object.keys(gtfsVehicles);
}

/**
 * Obtém informações de status do store
 * @returns {Object} Status de cada empresa
 */
function getStoreStatus() {
    const status = {};
    for (const company of Object.keys(gtfsVehicles)) {
        const linhas = Object.keys(gtfsVehicles[company]);
        let totalVehicles = 0;
        for (const linha of linhas) {
            totalVehicles += Object.keys(gtfsVehicles[company][linha]).length;
        }
        status[company] = {
            lineCount: linhas.length,
            vehicleCount: totalVehicles
        };
    }
    return status;
}

/**
 * Limpa todos os veículos de uma empresa
 * @param {string} company - Nome da empresa
 */
function clearCompanyVehicles(company) {
    const companyKey = String(company).toUpperCase();
    delete gtfsVehicles[companyKey];
}

/**
 * Limpa todos os veículos de todas as empresas
 */
function clearAllVehicles() {
    for (const key of Object.keys(gtfsVehicles)) {
        delete gtfsVehicles[key];
    }
}

/**
 * Substitui snapshot de veículos de uma empresa
 * @param {string} company - Nome da empresa
 * @param {Object} snapshot - Snapshot no formato { linha: [posições] }
 */
function replaceCompanySnapshot(company, snapshot) {
    const companyKey = String(company).toUpperCase();
    
    if (!snapshot || typeof snapshot !== 'object') return;

    // Limpar dados existentes
    gtfsVehicles[companyKey] = Object.create(null);

    // Reconstruir a partir do snapshot
    for (const linhaKey of Object.keys(snapshot)) {
        const positions = snapshot[linhaKey];
        if (!Array.isArray(positions)) continue;

        for (const pos of positions) {
            addPosition(company, pos, linhaKey);
        }
    }
}

module.exports = {
    addPosition,
    addPositions,
    getCompanyVehicles,
    getLineLastPositions,
    getLoadedCompanies,
    getStoreStatus,
    clearCompanyVehicles,
    clearAllVehicles,
    replaceCompanySnapshot
};
