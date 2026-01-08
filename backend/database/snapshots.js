const { dbPool } = require('./pool');

async function loadOnibusSnapshot(city = 'rio') {
    try {
        const result = await dbPool.query('SELECT * FROM fn_load_onibus_snapshot($1)', [city]);
        if (!result.rows || result.rows.length === 0 || !result.rows[0].fn_load_onibus_snapshot) {
            return null;
        }
        return result.rows[0].fn_load_onibus_snapshot || null;
    } catch (err) {
        console.error(`[snapshot][${city}] Error loading snapshot from database:`, err);
        return null;
    }
}

async function saveOnibusSnapshot(snapshot, city = 'rio') {
    if (!snapshot) return;

    try {
        await dbPool.query('SELECT fn_save_onibus_snapshot($1, $2::jsonb)', [city, snapshot]);
    } catch (err) {
        console.error(`[snapshot][${city}] Error inserting snapshot into database:`, err);
    }
}

// Aliases para compatibilidade
async function loadLatestRioOnibusSnapshot() {
    return loadOnibusSnapshot('rio');
}

async function saveRioOnibusSnapshot(snapshot) {
    return saveOnibusSnapshot(snapshot, 'rio');
}

async function loadLatestAngraOnibusSnapshot() {
    return loadOnibusSnapshot('angra');
}

async function saveAngraOnibusSnapshot(snapshot) {
    return saveOnibusSnapshot(snapshot, 'angra');
}

module.exports = {
    loadOnibusSnapshot,
    saveOnibusSnapshot,
    loadLatestRioOnibusSnapshot,
    saveRioOnibusSnapshot,
    loadLatestAngraOnibusSnapshot,
    saveAngraOnibusSnapshot,
};
