const express = require('express');
const cors = require('cors');
const { fetchGPSData } = require('./fetcher');
const { ensureFuturePartitions, saveRioOnibusSnapshot, loadLatestRioOnibusSnapshot } = require('./database');
const { getRioOnibus, getLineLastPositions, replaceRioOnibusSnapshot } = require('./rioOnibusStore');

const app = express();

app.use(cors());
app.use(express.json());

app.get('/rio_onibus', (req, res) => {
    const { linha } = req.query;

    if (linha != null) {
        const ordens = getLineLastPositions(linha);

        return res.json({ linha: String(linha), ordens });
    }

    return res.json(getRioOnibus());
});

// Ensure future partitions are created periodically
ensureFuturePartitions();
// Run once per day
setInterval(ensureFuturePartitions, 24 * 60 * 60 * 1000);

// Load snapshot on startup
loadLatestRioOnibusSnapshot()
    .then(snapshot => { if (snapshot) replaceRioOnibusSnapshot(snapshot); })
    .catch(err => console.error('[snapshot] failed to load', err));

// Catchup fetch (skips enrich and memory update, just persists to DB)
const CATCHUP_HOURS = Number(process.env.CATCHUP_HOURS) || 1;
console.log(`Running catchup with window of ${CATCHUP_HOURS} hours...`);
fetchGPSData(CATCHUP_HOURS * 60, { skipEnrich: true, updateInMemoryStore: false });

// Start polling
const POLLING_INTERVAL_MS = Number(process.env.POLLING_INTERVAL_MS) || 60000;
const POLLING_WINDOW_MINUTES = Number(process.env.POLLING_WINDOW_MINUTES) || 3;
setInterval(() => fetchGPSData(POLLING_WINDOW_MINUTES), POLLING_INTERVAL_MS);


// Save a periodic snapshot of the in-memory cache (for crash recovery/debugging)
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS) || 900000;
setInterval(() => saveRioOnibusSnapshot(getRioOnibus()), SNAPSHOT_INTERVAL_MS);



app.listen(process.env.BACKEND_PORT || 3001, () => {
    console.log('GPS Backend server running');
});
