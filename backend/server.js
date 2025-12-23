const express = require('express');
const cors = require('cors');
const { fetchRioGPSData } = require('./fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines } = require('./fetchers/angraFetcher');
const { ensureFuturePartitions, saveRioOnibusSnapshot, loadLatestRioOnibusSnapshot, saveAngraOnibusSnapshot, loadLatestAngraOnibusSnapshot, generateSentidoCoverageReport } = require('./database/index');
const { getRioOnibus, getLineLastPositions: getRioLineLastPositions, replaceRioOnibusSnapshot } = require('./stores/rioOnibusStore');
const { getAngraOnibus, getLineLastPositions: getAngraLineLastPositions, replaceAngraOnibusSnapshot } = require('./stores/angraOnibusStore');

const app = express();

app.use(cors());
app.use(express.json());

// Rio endpoints
app.get('/rio_onibus', (req, res) => {
    const { linha } = req.query;

    if (linha != null) {
        const ordens = getRioLineLastPositions(linha);
        return res.json({ linha: String(linha), ordens });
    }

    return res.json(getRioOnibus());
});

// Angra endpoints
app.get('/angra_onibus', (req, res) => {
    const { linha } = req.query;

    if (linha != null) {
        const ordens = getAngraLineLastPositions(linha);
        return res.json({ linha: String(linha), ordens });
    }

    return res.json(getAngraOnibus());
});

// Lifecycle helpers --------------------------------------------------------
const PARTITION_CHECK_INTERVAL_MS = Number(process.env.PARTITION_CHECK_INTERVAL_MS) || 86400000;
const COVERAGE_REPORT_INTERVAL_MS = Number(process.env.COVERAGE_REPORT_INTERVAL_MS) || 86400000;
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS) || 900000;

function setupPartitionChecks() {
    console.log(`[server] Running partition check (interval ${PARTITION_CHECK_INTERVAL_MS} ms)`);
    ensureFuturePartitions();
    setInterval(() => {
        console.log('[server] Running scheduled partition check');
        ensureFuturePartitions();
    }, PARTITION_CHECK_INTERVAL_MS);
}

function setupCoverageReporting() {
    console.log('[server] Generating initial sentido coverage report');
    generateSentidoCoverageReport();
    setInterval(() => {
        console.log('[server] Generating scheduled sentido coverage report');
        generateSentidoCoverageReport();
    }, COVERAGE_REPORT_INTERVAL_MS);
}

function setupSnapshots() {
    console.log('[server] Loading Rio snapshot');
    loadLatestRioOnibusSnapshot()
        .then(snapshot => { if (snapshot) replaceRioOnibusSnapshot(snapshot); })
        .catch(err => console.error('[snapshot][rio] failed to load', err));

    loadLatestAngraOnibusSnapshot()
        .then(snapshot => { if (snapshot) replaceAngraOnibusSnapshot(snapshot); })
        .catch(err => console.error('[snapshot][angra] failed to load', err));

    setInterval(() => {
        console.log('[server] Saving Rio snapshot');
        saveRioOnibusSnapshot(getRioOnibus());
    }, SNAPSHOT_INTERVAL_MS);
    setInterval(() => {
        console.log('[server] Saving Angra snapshot');
        saveAngraOnibusSnapshot(getAngraOnibus());
    }, SNAPSHOT_INTERVAL_MS);
}

// Rio ----------------------------------------------------------------------
const CATCHUP_HOURS = Number(process.env.CATCHUP_HOURS) || 1;
const RIO_POLLING_INTERVAL_MS = Number(process.env.RIO_POLLING_INTERVAL_MS) || Number(process.env.POLLING_INTERVAL_MS) || 60000;
const RIO_POLLING_WINDOW_MINUTES = Number(process.env.RIO_POLLING_WINDOW_MINUTES) || Number(process.env.POLLING_WINDOW_MINUTES) || 3;

function setupRioPolling() {
    console.log(`[Rio] Running catchup with window of ${CATCHUP_HOURS} hours...`);
    fetchRioGPSData(CATCHUP_HOURS * 60, { skipEnrich: true, updateInMemoryStore: false });
    console.log('[Rio] Starting polling...');
    setInterval(() => {
        console.log('[Rio] Fetching Rio GPS data (scheduled)');
        fetchRioGPSData(RIO_POLLING_WINDOW_MINUTES);
    }, RIO_POLLING_INTERVAL_MS);
}

// Angra --------------------------------------------------------------------
const ANGRA_POLLING_INTERVAL_MS = Number(process.env.ANGRA_POLLING_INTERVAL_MS) || 60000;
const ANGRA_CIRCULAR_LINES_POLL_MS = Number(process.env.ANGRA_CIRCULAR_LINES_POLL_MS) || 24 * 60 * 60 * 1000;

async function setupAngraPolling() {
    console.log('[Angra] Loading circular line definitions');
    await fetchCircularLines().catch(err => console.error('[Angra] Initial circular lines fetch failed', err));

    console.log('[Angra] Starting polling...');
    fetchAngraGPSData();
    setInterval(() => {
        console.log('[Angra] Fetching Angra GPS data (scheduled)');
        fetchAngraGPSData();
    }, ANGRA_POLLING_INTERVAL_MS);

    setInterval(() => {
        console.log('[Angra] Fetching circular line definitions (scheduled)');
        fetchCircularLines();
    }, ANGRA_CIRCULAR_LINES_POLL_MS);
}

setupPartitionChecks();
setupCoverageReporting();
setupSnapshots();
setupRioPolling();
setupAngraPolling().catch(err => console.error('[Angra] Failed to bootstrap polling', err));

app.listen(process.env.BACKEND_PORT || 3001, () => {
    console.log('GPS Backend server running');
});
