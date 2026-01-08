const { boss } = require('./boss');
const { fetchRioGPSData } = require('../fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines } = require('../fetchers/angraFetcher');
const { ensureFuturePartitions, saveRioOnibusSnapshot, saveAngraOnibusSnapshot, generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('../database/index');
const { getRioOnibus } = require('../stores/rioOnibusStore');
const { getAngraOnibus } = require('../stores/angraOnibusStore');
const { loadItinerarioIntoMemory } = require('../itinerarioStore');
const jobsConfig = require('../config/jobs.json');

const handlers = {
    loadItinerarioIntoMemory,
    ensureFuturePartitions,
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
    fetchRioGPSData: (data = {}) => fetchRioGPSData(null, data),
    fetchAngraGPSData: (data = {}) => fetchAngraGPSData(null, data),
    fetchCircularLines,
    saveRioOnibusSnapshot: () => saveRioOnibusSnapshot(getRioOnibus()),
    saveAngraOnibusSnapshot: () => saveAngraOnibusSnapshot(getAngraOnibus())
};

function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
    return `${(ms / 60000).toFixed(2)}min`;
}

function wrapHandler(jobName, handlerFn) {
    return async (job) => {
        const startTime = Date.now();
        console.log(`[job:${jobName}] Iniciando execução`);
        
        try {
            await handlerFn(job?.data || {});
            const duration = Date.now() - startTime;
            console.log(`[job:${jobName}] Sucesso | duração=${formatDuration(duration)}`);
        } catch (error) {
            const duration = Date.now() - startTime;
            console.error(`[job:${jobName}] Erro | duração=${formatDuration(duration)} | erro=${error.message}`);
            throw error;
        }
    };
}

async function registerWorkers() {
    for (const jobConfig of jobsConfig.jobs) {
        const { name, handler } = jobConfig;
        
        if (!handlers[handler]) {
            console.error(`[scheduler] Handler "${handler}" não encontrado para job "${name}"`);
            continue;
        }

        await boss.work(name, wrapHandler(name, (jobData) => handlers[handler]({ ...jobData, ...jobConfig.options })));
        console.log(`[scheduler] Worker registrado: ${name}`);
    }
}

async function scheduleJobs() {
    for (const jobConfig of jobsConfig.jobs) {
        const { name, cron, description } = jobConfig;

        // 🔑 ISSO É O QUE FALTAVA
        await boss.createQueue(name);

        await boss.schedule(
            name,
            cron,
            {},
            { tz: 'America/Sao_Paulo' }
        );

        console.log(
            `[scheduler] Job agendado: ${name} | cron="${cron}" | ${description}`
        );
    }
}


async function startScheduler() {
    console.log('[scheduler] Iniciando pg-boss...');
    await boss.start();
    console.log('[scheduler] pg-boss iniciado');

    // Importante: criar/agendar as filas ANTES de registrar workers.
    // Caso contrário, o worker pode começar a pollar e o pg-boss reclamar que a Queue não existe.
    await scheduleJobs();
    await registerWorkers();

    
    console.log(`[scheduler] ${jobsConfig.jobs.length} jobs configurados e prontos`);
}

async function stopScheduler() {
    console.log('[scheduler] Parando pg-boss...');
    await boss.stop();
    console.log('[scheduler] pg-boss parado');
}

module.exports = { startScheduler, stopScheduler, boss };
