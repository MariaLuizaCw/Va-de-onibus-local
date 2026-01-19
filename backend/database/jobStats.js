const { dbPool } = require('./pool');

/**
 * Busca estatísticas agregadas dos jobs para uma data específica
 * @param {string} date - Data no formato YYYY-MM-DD
 * @returns {Promise<Object>} Estatísticas dos jobs
 */
async function getJobStats(date) {
    const query = `SELECT * FROM fn_get_job_stats($1)`;
    const result = await dbPool.query(query, [date]);
    
    // Organizar dados em estrutura hierárquica
    const parentJobs = {};
    const childJobs = [];

    for (const row of result.rows) {
        const jobData = {
            jobName: row.job_name,
            executionCount: parseInt(row.execution_count),
            avgDurationMs: parseFloat(row.avg_duration_ms) || 0,
            stddevDurationMs: parseFloat(row.stddev_duration_ms) || 0,
            minDurationMs: parseInt(row.min_duration_ms) || 0,
            maxDurationMs: parseInt(row.max_duration_ms) || 0,
            successCount: parseInt(row.success_count),
            errorCount: parseInt(row.error_count),
            status: getStatusLabel(parseInt(row.success_count), parseInt(row.error_count))
        };

        if (row.type === 'parent') {
            parentJobs[row.job_name] = {
                ...jobData,
                children: []
            };
        } else {
            childJobs.push({
                ...jobData,
                parentJob: row.parent_job
            });
        }
    }

    // Associar filhos aos pais
    for (const child of childJobs) {
        if (parentJobs[child.parentJob]) {
            parentJobs[child.parentJob].children.push(child);
        }
    }

    return {
        date,
        jobs: Object.values(parentJobs)
    };
}

/**
 * Busca execuções de um job específico ao longo do dia (para gráfico de linha)
 * @param {string} jobName - Nome do job
 * @param {string} date - Data no formato YYYY-MM-DD
 * @param {boolean} includeChildren - Incluir subtasks
 * @returns {Promise<Array>} Lista de execuções com timestamp e duração
 */
async function getJobTimeline(jobName, date, includeChildren = false) {
    const query = `SELECT * FROM fn_get_job_timeline($1, $2, $3)`;
    const result = await dbPool.query(query, [jobName, date, includeChildren]);
    
    return result.rows.map(row => ({
        jobName: row.job_name,
        parentJob: row.parent_job,
        subtask: row.subtask,
        startedAt: row.started_at,
        finishedAt: row.finished_at,
        durationMs: row.duration_ms,
        status: row.status,
        infoMessage: row.info_message,
        errorMessage: row.error_message
    }));
}


/**
 * Busca distribuição de status por hora para um job
 * @param {string} jobName - Nome do job
 * @param {string} date - Data no formato YYYY-MM-DD
 * @returns {Promise<Array>} Distribuição por hora
 */
async function getJobHourlyDistribution(jobName, date) {
    const query = `SELECT * FROM fn_get_job_hourly_distribution($1, $2)`;
    const result = await dbPool.query(query, [jobName, date]);
    
    return result.rows.map(row => ({
        hour: parseInt(row.hour),
        total: parseInt(row.total),
        successCount: parseInt(row.success_count),
        errorCount: parseInt(row.error_count),
        avgDurationMs: parseFloat(row.avg_duration_ms) || 0
    }));
}

function getStatusLabel(successCount, errorCount) {
    if (errorCount === 0) return 'success';
    if (successCount === 0) return 'error';
    return 'mixed';
}

module.exports = {
    getJobStats,
    getJobTimeline,
    getJobHourlyDistribution
};
