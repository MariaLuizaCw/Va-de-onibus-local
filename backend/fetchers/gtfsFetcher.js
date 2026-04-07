/**
 * Fetcher para empresas GTFS-RT
 * Busca dados de veículos e rotas da API GTFS-RT
 */

const axios = require('axios');
const { logJobExecution } = require('../database/jobLogs');
const { setCompanyRoutes, getRouteShortName, getShapeByDirection, hasCompanyRoutes } = require('../stores/gtfsRoutesStore');
const { addPositions, getCompanyVehicles } = require('../stores/gtfsVehiclesStore');
const { enrichGtfsRecordsWithSentido, saveGtfsToGpsSentido, enrichVehicles } = require('../database/index');

// URL base da API GTFS-RT (variável de ambiente)
const GTFS_RT_URL = process.env.GTFSRTURL;

/**
 * Deduplicar veículos por ID, mantendo o mais recente
 */
function deduplicateByVehicleId(entities) {
    const byVehicle = new Map();
    for (const entity of entities) {
        const vehicleId = entity.vehicle?.vehicle?.id;
        if (!vehicleId) continue;
        
        const existing = byVehicle.get(vehicleId);
        const currentTime = parseInt(entity.vehicle?.timestamp || '0', 10);
        const existingTime = existing ? parseInt(existing.vehicle?.timestamp || '0', 10) : 0;
        
        if (!existing || currentTime > existingTime) {
            byVehicle.set(vehicleId, entity);
        }
    }
    return Array.from(byVehicle.values());
}

/**
 * Busca lista de empresas disponíveis
 */
async function fetchAvailableCompanies() {
    if (!GTFS_RT_URL) {
        console.error('[GTFS-RT] GTFSRTURL não configurada');
        return [];
    }

    try {
        const response = await axios.get(`${GTFS_RT_URL}/companies`, {
            timeout: 10000
        });
        
        const companies = response.data?.availableCompanies || [];
        return companies;
    } catch (error) {
        console.error('[GTFS-RT] Erro ao buscar empresas:', error.message);
        return [];
    }
}

/**
 * Busca rotas de uma empresa
 */
async function fetchCompanyRoutes(company) {
    if (!GTFS_RT_URL) return null;

    const startedAt = new Date();
    
    try {
        const response = await axios.get(`${GTFS_RT_URL}/api/routes/${company}`, {
            timeout: 30000
        });
        
        const routes = response.data;
        
        if (Array.isArray(routes)) {
            setCompanyRoutes(company, routes);
        }
        
        const finishedAt = new Date();
        await logJobExecution({
            jobName: `gtfs-routes-${company.toLowerCase()}`,
            parentJob: 'gtfs-routes-refresh',
            subtask: true,
            startedAt,
            finishedAt,
            durationMs: finishedAt - startedAt,
            status: 'success',
            infoMessage: `${routes?.length || 0} rotas`
        });
        
        return routes;
    } catch (error) {
        console.error(`[GTFS-RT][${company}] Erro ao buscar rotas:`, error.message);
        
        const finishedAt = new Date();
        await logJobExecution({
            jobName: `gtfs-routes-${company.toLowerCase()}`,
            parentJob: 'gtfs-routes-refresh',
            subtask: true,
            startedAt,
            finishedAt,
            durationMs: finishedAt - startedAt,
            status: 'error',
            errorMessage: error.message
        });
        
        return null;
    }
}

/**
 * Busca veículos de uma empresa
 */
async function fetchCompanyVehicles(company) {
    if (!GTFS_RT_URL) return null;

    const startedAt = new Date();
    
    try {
        const response = await axios.get(`${GTFS_RT_URL}/gtfs/${company}`, {
            timeout: 15000
        });
        
        const data = response.data;
        const entities = data?.data?.entity || [];
        
        
        const finishedAt = new Date();
        await logJobExecution({
            jobName: `gtfs-vehicles-${company.toLowerCase()}`,
            parentJob: 'gtfs-gps-fetch',
            subtask: true,
            startedAt,
            finishedAt,
            durationMs: finishedAt - startedAt,
            status: 'success',
            infoMessage: `${entities.length} veículos`
        });
        
        return {
            company: data.company,
            timestamp: data.timestamp,
            entities
        };
    } catch (error) {
        console.error(`[GTFS-RT][${company}] Erro ao buscar veículos:`, error.message);
        
        const finishedAt = new Date();
        await logJobExecution({
            jobName: `gtfs-vehicles-${company.toLowerCase()}`,
            parentJob: 'gtfs-gps-fetch',
            subtask: true,
            startedAt,
            finishedAt,
            durationMs: finishedAt - startedAt,
            status: 'error',
            errorMessage: error.message
        });
        
        return null;
    }
}





/**
 * Job principal: Atualiza rotas de todas as empresas
 * Executado na inicialização e 1 vez por dia
 */
async function fetchGtfsRoutesData(options = {}) {
    if (!GTFS_RT_URL) {
        console.warn('[GTFS-RT] GTFSRTURL não configurada, pulando fetch de rotas');
        return;
    }

    
    const companies = await fetchAvailableCompanies();
    
    if (companies.length === 0) {
        console.warn('[GTFS-RT] Nenhuma empresa disponível');
        return;
    }

    for (const company of companies) {
        await fetchCompanyRoutes(company);
    }

}

/**
 * Job principal: Busca veículos de todas as empresas
 * Executado periodicamente (ex: a cada 1 minuto)
 */
async function fetchGtfsGPSData(options = {}) {
    if (!GTFS_RT_URL) {
        console.warn('[GTFS-RT] GTFSRTURL não configurada, pulando fetch de GPS');
        return;
    }

    
    const companies = await fetchAvailableCompanies();
    
    if (companies.length === 0) {
        console.warn('[GTFS-RT] Nenhuma empresa disponível');
        return;
    }

    let totalVehicles = 0;

    for (const company of companies) {
        // Verificar se temos rotas carregadas para esta empresa
        if (!hasCompanyRoutes(company)) {
            await fetchCompanyRoutes(company);
        }

        const result = await fetchCompanyVehicles(company);
        
        if (!result?.entities) continue;

        // Deduplicar veículos
        const deduplicated = deduplicateByVehicleId(result.entities);

        // Enriquecer com número da linha
        let enriched = await enrichVehicles(company, deduplicated);
        
        // Enriquecer com sentido em batch
        if (options.identificarSentido) {
            enriched = await enrichGtfsRecordsWithSentido(enriched, {
                maxDistance: Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300
            });
        }
        
        // Salvar no banco se solicitado
        if (options.saveToGpsSentido) {
            saveGtfsToGpsSentido(enriched)
                .catch(err => console.error(`[GTFS-RT][${company}] Erro ao salvar GPS:`, err.message));
        }
        
        // Atualizar store em memória
        if (options.updateInMemoryStore) {
            addPositions(company, enriched, (routeId) => getRouteShortName(company, routeId));
        }

        totalVehicles += enriched.length;
    }

}

module.exports = {
    fetchAvailableCompanies,
    fetchCompanyRoutes,
    fetchCompanyVehicles,
    fetchGtfsRoutesData,
    fetchGtfsGPSData,
    deduplicateByVehicleId
};
