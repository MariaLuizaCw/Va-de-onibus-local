const { dbPool } = require('./pool');

const RETENTION_DAYS = Number(process.env.JOB_LOG_RETENTION_DAYS) || 90;

async function logJobExecution({ jobName, subtask = false, cronExpression = null, startedAt, finishedAt, durationMs, status, errorMessage = null }) {
    const query = `SELECT fn_insert_job_execution($1, $2, $3, $4, $5, $6, $7, $8)`;
    
    try {
        await dbPool.query(query, [
            jobName,
            subtask,
            cronExpression,
            startedAt,
            finishedAt,
            durationMs,
            status,
            errorMessage
        ]);
    } catch (error) {
        console.error('[jobLogs] Erro ao inserir log:', error.message);
    }
}

async function deleteOldJobExecutions() {
    const query = `SELECT fn_delete_old_job_executions($1) AS deleted_count`;
    
    try {
        const result = await dbPool.query(query, [RETENTION_DAYS]);
        const deletedCount = result.rows[0]?.deleted_count || 0;
        if (deletedCount > 0) {
            console.log(`[jobLogs] ${deletedCount} registros removidos (retenção: ${RETENTION_DAYS} dias)`);
        }
        return deletedCount;
    } catch (error) {
        console.error('[jobLogs] Erro ao deletar logs antigos:', error.message);
        return 0;
    }
}

module.exports = {
    logJobExecution,
    deleteOldJobExecutions
};
