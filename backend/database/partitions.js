const { dbPool } = require('./pool');
const { API_TIMEZONE, formatDateYYYYMMDDInTimeZone } = require('../utils');

async function cleanupOldPartitionsForTable(tablePrefix, retentionDays = 7) {
    const todaySpStr = formatDateYYYYMMDDInTimeZone(new Date(), API_TIMEZONE);

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
    const cutoffSpStr = formatDateYYYYMMDDInTimeZone(cutoffDate, API_TIMEZONE);

    console.log(`[partitions][${tablePrefix}] cleanup start tz=${API_TIMEZONE} today=${todaySpStr} retentionDays=${retentionDays} cutoff=${cutoffSpStr}`);

    let result;
    try {
        result = await dbPool.query(
            `
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE '${tablePrefix}_%'
            `
        );
    } catch (err) {
        console.error(`[partitions][${tablePrefix}] Error listing partition tables`, err);
        return;
    }

    console.log(`[partitions][${tablePrefix}] found ${result.rows.length} tables matching ${tablePrefix}_%`);

    const prefixLen = tablePrefix.length + 1; // +1 for underscore
    const regex = new RegExp(`^${tablePrefix}_\\d{8}$`);

    const tablesToDrop = result.rows
        .map((r) => r.tablename)
        .filter((name) => regex.test(name))
        .map((name) => {
            const y = name.slice(prefixLen, prefixLen + 4);
            const m = name.slice(prefixLen + 4, prefixLen + 6);
            const d = name.slice(prefixLen + 6, prefixLen + 8);
            return { name, dateStr: `${y}-${m}-${d}` };
        })
        .filter(({ dateStr }) => dateStr < cutoffSpStr)
        .sort((a, b) => a.dateStr.localeCompare(b.dateStr));

    if (tablesToDrop.length === 0) {
        console.log(`[partitions][${tablePrefix}] nothing to drop (cutoff=${cutoffSpStr})`);
        return;
    }

    console.log(
        `[partitions][${tablePrefix}] will drop ${tablesToDrop.length} partitions older than ${cutoffSpStr}: ${tablesToDrop
            .map((t) => `${t.name}(${t.dateStr})`)
            .join(', ')}`
    );

    for (const { name, dateStr } of tablesToDrop) {
        try {
            console.log(`[partitions][${tablePrefix}] dropping ${name} (date=${dateStr})`);
            await dbPool.query(`DROP TABLE IF EXISTS public.${name};`);
        } catch (err) {
            console.error(`[partitions][${tablePrefix}] Error dropping partition`, name, dateStr, { cutoffSpStr, todaySpStr }, err);
        }
    }

    console.log(`[partitions][${tablePrefix}] cleanup done dropped=${tablesToDrop.length}`);
}

async function cleanupOldPartitions(retentionDays = 7) {
    await cleanupOldPartitionsForTable('gps_posicoes_rio', retentionDays);
    await cleanupOldPartitionsForTable('gps_posicoes_angra', retentionDays);
    await cleanupOldPartitionsForTable('gps_sentido', retentionDays);
}

// partitionType: 'bigint_ms' (Rio - milissegundos) ou 'timestamp' (Angra)
async function createPartitionForDate(tablePrefix, dateStr, partitionType = 'bigint_ms') {
    const tableSuffix = dateStr.replace(/-/g, '');
    const tableName = `${tablePrefix}_${tableSuffix}`;

    let rangeClause;
    if (partitionType === 'bigint_ms') {
        rangeClause = `
            FOR VALUES FROM (
                (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE '${API_TIMEZONE}') * 1000)::bigint
            ) TO (
                (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE '${API_TIMEZONE}' + interval '1 day') * 1000)::bigint
            )`;
    } else {
        rangeClause = `
            FOR VALUES FROM ('${dateStr} 00:00:00'::timestamp AT TIME ZONE '${API_TIMEZONE}') TO ('${dateStr} 00:00:00'::timestamp AT TIME ZONE '${API_TIMEZONE}' + interval '1 day')`;
    }

    const text = `
        CREATE TABLE IF NOT EXISTS public.${tableName}
        PARTITION OF public.${tablePrefix}
        ${rangeClause};
    `;

    try {
        await dbPool.query(text);
        console.log(`[partitions][${tablePrefix}] created partition ${tableName}`);
    } catch (err) {
        if (!err.message.includes('already exists')) {
            console.error(`[partitions][${tablePrefix}] Error creating partition for date`, dateStr, err);
        }
    }
}

async function ensureFuturePartitions() {
    const now = new Date();
    const offsets = [0, 1, 2]; // today, tomorrow, day after

    for (const offset of offsets) {
        const targetDate = new Date(now.getTime() + offset * 24 * 60 * 60 * 1000);
        const dateStr = formatDateYYYYMMDDInTimeZone(targetDate, API_TIMEZONE);
        await createPartitionForDate('gps_posicoes_rio', dateStr, 'bigint_ms');
        await createPartitionForDate('gps_posicoes_angra', dateStr, 'timestamp');
        await createPartitionForDate('gps_sentido', dateStr, 'timestamp');
    }

    const retentionDays = Number(process.env.PARTITION_RETENTION_DAYS) || 7;
    await cleanupOldPartitions(retentionDays);
}

module.exports = {
    cleanupOldPartitionsForTable,
    cleanupOldPartitions,
    createPartitionForDate,
    ensureFuturePartitions,
};
