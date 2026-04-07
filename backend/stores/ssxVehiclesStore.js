/**
 * Store em memória para veículos SSX (por empresa)
 * Guarda últimas posições por linha, separado por empresa (Angra, BarraPirai, PedroAntonio, Resendense)
 * Estrutura similar ao gtfsVehiclesStore
 */

// Estrutura: { EMPRESA: { LINHA: { VEICULO_ID: [posições] } } }
const ssxVehicles = Object.create(null);

/**
 * Garante que o bucket existe para empresa/linha/veículo
 */
function ensureBucket(company, linha, vehicleId) {
    const companyKey = String(company).toLowerCase();
    const linhaKey = String(linha);
    const vehicleKey = String(vehicleId);

    if (!ssxVehicles[companyKey]) ssxVehicles[companyKey] = Object.create(null);
    if (!ssxVehicles[companyKey][linhaKey]) ssxVehicles[companyKey][linhaKey] = Object.create(null);
    if (!ssxVehicles[companyKey][linhaKey][vehicleKey]) ssxVehicles[companyKey][linhaKey][vehicleKey] = [];

    return ssxVehicles[companyKey][linhaKey][vehicleKey];
}

/**
 * Adiciona uma posição de veículo
 * @param {string} company - Nome da empresa (angra, barrapirai, pedroantonio, resendense)
 * @param {Object} record - Registro do veículo (dados originais da API SSX)
 */
function addPosition(company, record) {
    if (!record) return;
    
    const linha = record.LineNumber;
    const vehicleId = record.VehicleIntegrationCode;
    
    if (linha == null || vehicleId == null) return;

    const bucket = ensureBucket(company, linha, vehicleId);

    // Armazenar registro original da API
    bucket.push(record);

    // Ordenar por PositionDateTime (mais recente primeiro)
    bucket.sort((a, b) => {
        const timeA = new Date(a.PositionDateTime).getTime();
        const timeB = new Date(b.PositionDateTime).getTime();
        return timeB - timeA;
    });

    // Manter apenas as 3 últimas posições
    if (bucket.length > 3) bucket.length = 3;
}

/**
 * Adiciona múltiplas posições para uma empresa
 * @param {string} company - Nome da empresa
 * @param {Array} records - Array de registros de veículos
 */
function addPositions(company, records) {
    if (!Array.isArray(records) || records.length === 0) return;
    
    for (const record of records) {
        addPosition(company, record);
    }
}

/**
 * Obtém todos os veículos de uma empresa
 * @param {string} company - Nome da empresa
 * @returns {Object} Estrutura { linha: [posições] }
 */
function getCompanyVehicles(company) {
    const companyKey = String(company).toLowerCase();
    const companyData = ssxVehicles[companyKey];
    
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
    const companyKey = String(company).toLowerCase();
    const linhaKey = String(linha);
    
    const linhaData = ssxVehicles[companyKey]?.[linhaKey];
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
 * Obtém histórico de posições de um veículo específico
 * @param {string} company - Nome da empresa
 * @param {string} linha - Número da linha
 * @param {string} vehicleId - ID do veículo
 * @returns {Array} Array com posições do veículo
 */
function getVehiclePositions(company, linha, vehicleId) {
    const companyKey = String(company).toLowerCase();
    const linhaKey = String(linha);
    const vehicleKey = String(vehicleId);
    
    return ssxVehicles[companyKey]?.[linhaKey]?.[vehicleKey] || [];
}

/**
 * Obtém todas as empresas com veículos carregados
 * @returns {Array<string>} Lista de nomes das empresas
 */
function getLoadedCompanies() {
    return Object.keys(ssxVehicles);
}

/**
 * Obtém informações de status do store
 * @returns {Object} Status de cada empresa
 */
function getStoreStatus() {
    const status = {};
    for (const company of Object.keys(ssxVehicles)) {
        const linhas = Object.keys(ssxVehicles[company]);
        let totalVehicles = 0;
        for (const linha of linhas) {
            totalVehicles += Object.keys(ssxVehicles[company][linha]).length;
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
    const companyKey = String(company).toLowerCase();
    delete ssxVehicles[companyKey];
}

/**
 * Limpa todos os veículos de todas as empresas
 */
function clearAllVehicles() {
    for (const key of Object.keys(ssxVehicles)) {
        delete ssxVehicles[key];
    }
}

/**
 * Substitui snapshot de veículos de uma empresa
 * @param {string} company - Nome da empresa
 * @param {Object} snapshot - Snapshot no formato { linha: [posições] }
 */
function replaceCompanySnapshot(company, snapshot) {
    const companyKey = String(company).toLowerCase();
    
    if (!snapshot || typeof snapshot !== 'object') return;

    // Limpar dados existentes
    ssxVehicles[companyKey] = Object.create(null);

    // Reconstruir a partir do snapshot
    for (const linhaKey of Object.keys(snapshot)) {
        const positions = snapshot[linhaKey];
        if (!Array.isArray(positions)) continue;

        for (const pos of positions) {
            addPosition(company, pos);
        }
    }
}

module.exports = {
    addPosition,
    addPositions,
    getCompanyVehicles,
    getLineLastPositions,
    getVehiclePositions,
    getLoadedCompanies,
    getStoreStatus,
    clearCompanyVehicles,
    clearAllVehicles,
    replaceCompanySnapshot
};
