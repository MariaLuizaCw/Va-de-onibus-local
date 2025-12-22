const express = require('express');
const cors = require('cors');
const { fetchGPSData } = require('./fetcher');
const { ensureFuturePartitions } = require('./database');
const { getRioOnibus, getLineLastPositions } = require('./rioOnibusStore');

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

// Initial catch-up fetch: larger window to cover downtime gaps (e.g. 5 hours)
const CATCHUP_HOURS = Number(process.env.CATCHUP_HOURS) || 1;
console.log(`Running catchup with window of ${CATCHUP_HOURS} hours...`);
fetchGPSData(CATCHUP_HOURS * 60);

// Start polling (every 60s) with default small window (3 minutes)
setInterval(() => fetchGPSData(), 60000);

app.listen(process.env.BACKEND_PORT || 3001, () => {
    console.log('GPS Backend server running');
});
