const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { fetchRioGPSData } = require('./fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines } = require('./fetchers/angraFetcher');
const { ensureFuturePartitions, saveRioOnibusSnapshot, loadLatestRioOnibusSnapshot, saveAngraOnibusSnapshot, loadLatestAngraOnibusSnapshot, generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('./database/index');
const { getRioOnibus, getLineLastPositions: getRioLineLastPositions, replaceRioOnibusSnapshot } = require('./stores/rioOnibusStore');
const { getAngraOnibus, getLineLastPositions: getAngraLineLastPositions, replaceAngraOnibusSnapshot } = require('./stores/angraOnibusStore');
const { resolveRioTimestamp, resolveAngraTimestamp, summarizeLines } = require('./utils/stats');
const { loadItinerarioIntoMemory } = require('./itinerarioStore');

const app = express();

const AUTH_USERNAME = process.env.AUTH_USERNAME;
const AUTH_PASSWORD = process.env.AUTH_PASSWORD;
const JWT_SECRET = process.env.JWT_SECRET;
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN;

const DAY_MS = 24 * 60 * 60 * 1000;
const MULTI_ORDER_LIMIT = Number(process.env.MULTI_ORDER_LIMIT) || 5;

if (!AUTH_USERNAME || !AUTH_PASSWORD) {
    console.error('[auth] AUTH_USERNAME and AUTH_PASSWORD must be set.');
    process.exit(1);
}

if (!JWT_SECRET) {
    console.error('[auth] JWT_SECRET must be set.');
    process.exit(1);
}

if (!JWT_EXPIRES_IN) {
    console.error('[auth] JWT_EXPIRES_IN must be set.');
    process.exit(1);
}

app.use(cors());
app.use(express.json());

const API_BASE_PATH = process.env.API_BASE_PATH || '';

// Auth helpers -------------------------------------------------------------
function createToken(username) {
    return jwt.sign({ sub: username }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
}

function respondUnauthorized(res, message = 'Token inválido ou ausente.') {
    return res.status(401).json({ error: message });
}

function authenticateToken(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader) {
        return respondUnauthorized(res, 'Cabeçalho Authorization ausente.');
    }

    const [scheme, token] = authHeader.split(' ');
    if (scheme !== 'Bearer' || !token) {
        return respondUnauthorized(res, 'Formato do token inválido.');
    }

    try {
        req.user = jwt.verify(token, JWT_SECRET);
        next();
    } catch (err) {
        return respondUnauthorized(res);
    }
}

const router = express.Router();

// Login route -------------------------------------------------------------
router.post('/auth/login', (req, res) => {
    const { username, password } = req.body || {};
    if (!username || !password) {
        return res.status(400).json({ error: 'Usuário e senha são obrigatórios.' });
    }

    if (username !== AUTH_USERNAME || password !== AUTH_PASSWORD) {
        return res.status(401).json({ error: 'Credenciais inválidas.' });
    }

    const token = createToken(username);
    return res.json({ token });
});

router.use(authenticateToken);

router.get('/stats/lines', (req, res) => {
    const rioStats = summarizeLines(getRioOnibus(), resolveRioTimestamp);
    const angraStats = summarizeLines(getAngraOnibus(), resolveAngraTimestamp);
    return res.json({
        rio: rioStats,
        angra: angraStats
    });
});

// Rio endpoints
router.post('/rio_onibus', (req, res) => {
    const { linha } = req.body || {};

    if (linha != null && String(linha).trim() !== '') {
        const ordens = getRioLineLastPositions(linha);
        return res.json({ linha: String(linha), ordens });
    }

    return res.json(getRioOnibus());
});

// Angra endpoints
router.post('/angra_onibus', (req, res) => {
    const { linha } = req.body || {};

    if (linha != null && String(linha).trim() !== '') {
        const ordens = getAngraLineLastPositions(linha);
        return res.json({ linha: String(linha), ordens });
    }

    return res.json(getAngraOnibus());
});

// Mount the router with prefix (or at root if empty)
if (API_BASE_PATH) {
    app.use(API_BASE_PATH, router);
    console.log(`API mounted at: ${API_BASE_PATH}`);
} else {
    app.use(router);
    console.log('API mounted at root');
}


// Lifecycle helpers --------------------------------------------------------
const PARTITION_CHECK_INTERVAL_MS = Number(process.env.PARTITION_CHECK_INTERVAL_MS) || 86400000;
const COVERAGE_REPORT_INTERVAL_MS = Number(process.env.COVERAGE_REPORT_INTERVAL_MS) || 86400000;
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS) || 900000;
const ITINERARIO_REFRESH_INTERVAL_MS = Number(process.env.ITINERARIO_REFRESH_INTERVAL_MS) || 6 * 60 * 60 * 1000;

async function setupItinerarioCache() {
    console.log('[itinerario] Loading itinerario cache');
    try {
        await loadItinerarioIntoMemory();
    } catch (err) {
        console.error('[itinerario] failed to load', err);
        throw err;
    }
    setInterval(() => {
        console.log('[itinerario] Refreshing itinerario cache');
        loadItinerarioIntoMemory().catch(err => console.error('[itinerario] failed to refresh', err));
    }, ITINERARIO_REFRESH_INTERVAL_MS);
}

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
    generateAngraRouteTypeReport();
    setInterval(() => {
        console.log('[server] Generating scheduled sentido coverage report');
        generateSentidoCoverageReport();
    }, COVERAGE_REPORT_INTERVAL_MS);
    setInterval(() => {
        console.log('[server] Generating scheduled angra route_type report');
        generateAngraRouteTypeReport();
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

async function bootstrap() {
    setupPartitionChecks();
    setupCoverageReporting();
    setupSnapshots();
    await setupItinerarioCache();
    setupRioPolling();
    await setupAngraPolling();
}

bootstrap().catch(err => console.error('[server] bootstrap failed', err));

app.listen(process.env.BACKEND_PORT || 3001, () => {
    console.log('GPS Backend server running');
});
