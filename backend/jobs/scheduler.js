const cron = require('node-cron');
const { fetchRioGPSData } = require('../fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines } = require('../fetchers/angraFetcher');
const { saveRioOnibusSnapshot, saveAngraOnibusSnapshot, generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('../database/index');
const { getRioOnibus } = require('../stores/rioOnibusStore');
const { getAngraOnibus } = require('../stores/angraOnibusStore');
const { loadItinerarioIntoMemory } = require('../stores/itinerarioStore');
const { logJobExecution, deleteOldJobExecutions } = require('../database/jobLogs');
const jobsConfig = require('../config/jobs.json');

const scheduledTasks = new Map();

const handlers = {
    loadItinerarioIntoMemory,
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
    fetchRioGPSData: (data = {}) => fetchRioGPSData(null, data),
    fetchAngraGPSData: (data = {}) => fetchAngraGPSData(data),
    fetchCircularLines,
    saveRioOnibusSnapshot: () => saveRioOnibusSnapshot(getRioOnibus()),
    saveAngraOnibusSnapshot: () => saveAngraOnibusSnapshot(getAngraOnibus()),
    deleteOldJobExecutions
};

function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
    return `${(ms / 60000).toFixed(2)}min`;
}

async function executeJob(jobName, cronExpression, handlerFn, options = {}, subtask = false) {
    const startedAt = new Date();
    const startTime = Date.now();
    
    console.log(`[job:${jobName}] Iniciando execução`);
    
    let status = 'success';
    let errorMessage = null;
    
    try {
        await handlerFn(options);
        const duration = Date.now() - startTime;
        console.log(`[job:${jobName}] Sucesso | duração=${formatDuration(duration)}`);
    } catch (error) {
        status = 'error';
        errorMessage = error.message;
        const duration = Date.now() - startTime;
        console.error(`[job:${jobName}] Erro | duração=${formatDuration(duration)} | erro=${error.message}`);
    }
    
    const finishedAt = new Date();
    const durationMs = Date.now() - startTime;
    
    await logJobExecution({
        jobName,
        subtask,
        cronExpression,
        startedAt,
        finishedAt,
        durationMs,
        status,
        errorMessage
    });
}

function scheduleJobs() {
    for (const jobConfig of jobsConfig.jobs) {
        const { name, handler, cron: cronExpression, description, options = {} } = jobConfig;

        if (!handlers[handler]) {
            console.error(`[scheduler] Handler "${handler}" não encontrado para job "${name}"`);
            continue;
        }

        if (!cron.validate(cronExpression)) {
            console.error(`[scheduler] Cron inválido para job "${name}": ${cronExpression}`);
            continue;
        }

        const task = cron.schedule(cronExpression, () => {
            executeJob(name, cronExpression, handlers[handler], options, false);
        }, {
            scheduled: true,
            timezone: 'America/Sao_Paulo'
        });

        scheduledTasks.set(name, task);
        console.log(`[scheduler] Job agendado: ${name} | cron="${cronExpression}" | ${description}`);
    }
}

async function startScheduler() {
    console.log('[scheduler] Iniciando node-cron scheduler...');
    
    // Executar jobs com runOnStartup: true primeiro
    await runStartupJobs();
    
    // Depois agendar os jobs recorrentes
    scheduleJobs();
    
    console.log(`[scheduler] ${scheduledTasks.size} jobs configurados e prontos`);
}

async function runStartupJobs() {
    console.log('[scheduler] Verificando jobs para execução no startup...');
    
    for (const jobConfig of jobsConfig.jobs) {
        const { name, handler, runOnStartup = false, options = {} } = jobConfig;
        
        if (runOnStartup && handlers[handler]) {
            console.log(`[scheduler] Executando job no startup: ${name}`);
            try {
                await executeJob(name, 'startup', handlers[handler], options, false);
            } catch (error) {
                console.error(`[scheduler] Erro ao executar job ${name} no startup:`, error.message);
            }
        }
    }
}

async function stopScheduler() {
    console.log('[scheduler] Parando scheduler...');
    
    for (const [name, task] of scheduledTasks) {
        task.stop();
        console.log(`[scheduler] Job parado: ${name}`);
    }
    scheduledTasks.clear();
    
    console.log('[scheduler] Scheduler parado');
}

module.exports = { startScheduler, stopScheduler, scheduledTasks, executeJob };
