const { Pool } = require('pg');

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
}

module.exports = {
    dbPool,
    saveRecordsToDb,
    ensureFuturePartitions,
};
