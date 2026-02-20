/**
 * Store em memória para rotas GTFS-RT (por empresa)
 * Usado para correlação routeId -> route_short_name
 * Atualizado na inicialização e 1 vez por dia
 */

// Estrutura: { EMPRESA: { routes: [...], routeMap: Map(route_id -> route), lastUpdated: Date } }
const gtfsRoutes = Object.create(null);

/**
 * Substitui todas as rotas de uma empresa
 * @param {string} company - Nome da empresa (ex: TRANSOCEANICO)
 * @param {Array} routes - Array de rotas do endpoint /api/routes/:company
 */
function setCompanyRoutes(company, routes) {
    const companyKey = String(company).toUpperCase();
    
    if (!Array.isArray(routes)) {
        console.warn(`[gtfsRoutesStore] Rotas inválidas para ${companyKey}`);
        return;
    }

    // Criar Map para lookup rápido por route_id
    const routeMap = new Map();
    for (const route of routes) {
        if (route.route_id) {
            routeMap.set(String(route.route_id), route);
        }
    }

    gtfsRoutes[companyKey] = {
        routes,
        routeMap,
        lastUpdated: new Date()
    };

    console.log(`[gtfsRoutesStore] ${companyKey}: ${routes.length} rotas carregadas`);
}

/**
 * Obtém todas as rotas de uma empresa
 * @param {string} company - Nome da empresa
 * @returns {Array} Array de rotas ou []
 */
function getCompanyRoutes(company) {
    const companyKey = String(company).toUpperCase();
    return gtfsRoutes[companyKey]?.routes || [];
}

/**
 * Obtém uma rota específica por route_id
 * @param {string} company - Nome da empresa
 * @param {string} routeId - ID da rota
 * @returns {Object|null} Objeto da rota ou null
 */
function getRouteById(company, routeId) {
    const companyKey = String(company).toUpperCase();
    const routeIdKey = String(routeId);
    return gtfsRoutes[companyKey]?.routeMap?.get(routeIdKey) || null;
}

/**
 * Obtém o route_short_name (número da linha) a partir do route_id
 * @param {string} company - Nome da empresa
 * @param {string} routeId - ID da rota
 * @returns {string|null} Número da linha ou null
 */
function getRouteShortName(company, routeId) {
    const route = getRouteById(company, routeId);
    return route?.route_short_name || null;
}

/**
 * Obtém o shape correspondente a um direction_id
 * @param {string} company - Nome da empresa
 * @param {string} routeId - ID da rota
 * @param {number} directionId - ID da direção (0 ou 1)
 * @returns {Object|null} Shape com start_coord e end_coord ou null
 */
function getShapeByDirection(company, routeId, directionId) {
    const route = getRouteById(company, routeId);
    if (!route || !Array.isArray(route.shapes)) return null;

    // Encontrar o primeiro shape com direction_id correspondente
    // Priorizar shapes sem parênteses no trip_headsign (principal)
    const shapes = route.shapes.filter(s => s.direction_id === directionId);
    
    if (shapes.length === 0) return null;
    
    // Prioridade: sem parênteses (variações)
    const principal = shapes.find(s => !s.trip_headsign?.includes('('));
    return principal || shapes[0];
}

/**
 * Obtém todas as empresas carregadas
 * @returns {Array<string>} Lista de nomes das empresas
 */
function getLoadedCompanies() {
    return Object.keys(gtfsRoutes);
}

/**
 * Verifica se uma empresa tem rotas carregadas
 * @param {string} company - Nome da empresa
 * @returns {boolean}
 */
function hasCompanyRoutes(company) {
    const companyKey = String(company).toUpperCase();
    return !!gtfsRoutes[companyKey]?.routes?.length;
}

/**
 * Obtém informações de status do store
 * @returns {Object} Status de cada empresa
 */
function getStoreStatus() {
    const status = {};
    for (const company of Object.keys(gtfsRoutes)) {
        status[company] = {
            routeCount: gtfsRoutes[company].routes.length,
            lastUpdated: gtfsRoutes[company].lastUpdated
        };
    }
    return status;
}

/**
 * Limpa todas as rotas de uma empresa
 * @param {string} company - Nome da empresa
 */
function clearCompanyRoutes(company) {
    const companyKey = String(company).toUpperCase();
    delete gtfsRoutes[companyKey];
}

/**
 * Limpa todas as rotas de todas as empresas
 */
function clearAllRoutes() {
    for (const key of Object.keys(gtfsRoutes)) {
        delete gtfsRoutes[key];
    }
}

module.exports = {
    setCompanyRoutes,
    getCompanyRoutes,
    getRouteById,
    getRouteShortName,
    getShapeByDirection,
    getLoadedCompanies,
    hasCompanyRoutes,
    getStoreStatus,
    clearCompanyRoutes,
    clearAllRoutes
};
