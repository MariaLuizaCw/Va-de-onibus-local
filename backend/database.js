const { Pool } = require('pg');
const { API_TIMEZONE, formatDateYYYYMMDDInTimeZone } = require('./utils');

const dbPool = new Pool({
    host: process.env.DATABASE_HOST,
    port: Number(process.env.DATABASE_PORT) || 5432,
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME
});

async function saveRecordsToDb(records) {
    if (!records || records.length === 0) return;
    const BATCH_SIZE = Number(process.env.DB_BATCH_SIZE) || 500;
    const PARAMS_PER_ROW = 8;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        const values = [];
        const placeholders = [];

        batch.forEach((record, index) => {
            const lat = typeof record.latitude === 'string'
                ? Number(record.latitude.replace(',', '.'))
                : Number(record.latitude);
            const lon = typeof record.longitude === 'string'
                ? Number(record.longitude.replace(',', '.'))
                : Number(record.longitude);

            values.push(
                record.ordem,
                lat,
                lon,
                Number(record.datahora),
                Number(record.velocidade),
                record.linha,
                Number(record.datahoraenvio),
                Number(record.datahoraservidor)
            );

            const baseIndex = index * PARAMS_PER_ROW;
            placeholders.push(
                `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8})`
            );
        });

        const text = `
            INSERT INTO gps_posicoes (
                ordem,
                latitude,
                longitude,
                datahora,
                velocidade,
                linha,
                datahoraenvio,
                datahoraservidor
            ) VALUES
                ${placeholders.join(',\n')}
            ON CONFLICT ON CONSTRAINT gps_posicoes_unique_ponto DO NOTHING;
        `;

        try {
            await dbPool.query(text, values);
        } catch (err) {
            console.error('Error inserting GPS records into database:', err);
        }
    }
}

function formatDateYYYYMMDD(date) {
    const pad = (n) => n.toString().padStart(2, '0');
    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    return `${year}-${month}-${day}`;
}

async function cleanupOldPartitions(retentionDays = 7) {
    const todaySpStr = formatDateYYYYMMDDInTimeZone(new Date(), API_TIMEZONE);

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
    const cutoffSpStr = formatDateYYYYMMDDInTimeZone(cutoffDate, API_TIMEZONE);

    console.log(`[partitions] cleanup start tz=${API_TIMEZONE} today=${todaySpStr} retentionDays=${retentionDays} cutoff=${cutoffSpStr}`);

    let result;
    try {
        result = await dbPool.query(
            `
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE 'gps_posicoes_%'
            `
        );
    } catch (err) {
        console.error('Error listing GPS partition tables', err);
        return;
    }

    console.log(`[partitions] found ${result.rows.length} tables matching gps_posicoes_%`);

    const tablesToDrop = result.rows
        .map((r) => r.tablename)
        .filter((name) => /^gps_posicoes_\d{8}$/.test(name))
        .map((name) => {
            const y = name.slice('gps_posicoes_'.length, 'gps_posicoes_'.length + 4);
            const m = name.slice('gps_posicoes_'.length + 4, 'gps_posicoes_'.length + 6);
            const d = name.slice('gps_posicoes_'.length + 6, 'gps_posicoes_'.length + 8);
            return { name, dateStr: `${y}-${m}-${d}` };
        })
        .filter(({ dateStr }) => dateStr < cutoffSpStr)
        .sort((a, b) => a.dateStr.localeCompare(b.dateStr));

    if (tablesToDrop.length === 0) {
        console.log(`[partitions] nothing to drop (cutoff=${cutoffSpStr})`);
        return;
    }

    console.log(
        `[partitions] will drop ${tablesToDrop.length} partitions older than ${cutoffSpStr}: ${tablesToDrop
            .map((t) => `${t.name}(${t.dateStr})`)
            .join(', ')}`
    );

    for (const { name, dateStr } of tablesToDrop) {
        try {
            console.log(`[partitions] dropping ${name} (date=${dateStr})`);
            await dbPool.query(`DROP TABLE IF EXISTS public.${name};`);
        } catch (err) {
            console.error('Error dropping old GPS partition', name, dateStr, { cutoffSpStr, todaySpStr }, err);
        }
    }

    console.log(`[partitions] cleanup done dropped=${tablesToDrop.length}`);
}

async function createPartitionForDate(dateStr) {
    const tableSuffix = dateStr.replace(/-/g, '');
    const tableName = `gps_posicoes_${tableSuffix}`;

    const text = `
        CREATE TABLE IF NOT EXISTS public.${tableName}
        PARTITION OF public.gps_posicoes
        FOR VALUES FROM (
            (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE 'America/Sao_Paulo') * 1000)::bigint
        ) TO (
            (EXTRACT(EPOCH FROM timestamp '${dateStr} 00:00:00' AT TIME ZONE 'America/Sao_Paulo' + interval '1 day') * 1000)::bigint
        );
    `;

    try {
        await dbPool.query(text);
    } catch (err) {
        console.error('Error creating partition for date', dateStr, err);
    }
}

async function ensureFuturePartitions() {
    const now = new Date();
    const offsets = [2];

    for (const offset of offsets) {
        const targetDate = new Date(now.getTime() + offset * 24 * 60 * 60 * 1000);
        const dateStr = formatDateYYYYMMDD(targetDate);
        await createPartitionForDate(dateStr);
    }

    await cleanupOldPartitions(7);
}

module.exports = {
    dbPool,
    saveRecordsToDb,
    ensureFuturePartitions,
};
