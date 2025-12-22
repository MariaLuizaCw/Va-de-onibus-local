const express = require('express');
const cors = require('cors');
const { fetchGPSData } = require('./fetcher');
const { ensureFuturePartitions } = require('./database');
const { getRioOnibus, getLineLastPositions, getLastPosition } = require('./rioOnibusStore');
const { loadItinerarioIntoMemory } = require('./itinerarioStore');
const { computeSentidoMetricsForPoint } = require('./itinerarioStore');

const app = express();

app.use(cors());
app.use(express.json());

(async () => {
    try {
        await loadItinerarioIntoMemory();
    } catch (err) {
        console.error('[itinerario] failed to load into memory', err);
    }
})();

app.get('/rio_onibus', (req, res) => {
    const { linha, ordem } = req.query;

    if (linha != null && ordem != null) {
        return res.json({ linha: String(linha), ordem: String(ordem), ultima: getLastPosition(linha, ordem) });
    }

    if (linha != null) {
        const ordens = getLineLastPositions(linha);

        const entries = Object.entries(ordens);

        (async () => {
            const result = Object.create(null);

            for (let i = 0; i < entries.length; i++) {
                const [ordemKey, pos] = entries[i];

                if (!pos) {
                    result[ordemKey] = null;
                } else {
                    const cacheKey = `${String(linha)}|${String(ordemKey)}|${Number(pos.datahora)}`;
                    const metrics = computeSentidoMetricsForPoint(linha, pos.longitude, pos.latitude);
                    const best = metrics && metrics.best ? metrics.best : null;

                    result[ordemKey] = {
                        ...pos,
                        sentido: best ? best.sentido : null
                    };
                }

                if (i % 25 === 24) {
                    await new Promise((resolve) => setImmediate(resolve));
                }
            }

            return res.json({ linha: String(linha), ordens: result });
        })().catch((err) => {
            console.error('[rio_onibus] failed to compute sentido', err);
            return res.status(500).json({ error: 'failed to compute sentido' });
        });

        return;
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
