const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { fetchRioGPSData } = require('./fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines } = require('./fetchers/angraFetcher');
const { loadLatestRioOnibusSnapshot, loadLatestAngraOnibusSnapshot, generateSentidoCoverageReport, generateAngraRouteTypeReport } = require('./database/index');
const { getRioOnibus, getLineLastPositions: getRioLineLastPositions, replaceRioOnibusSnapshot } = require('./stores/rioOnibusStore');
const { getAngraOnibus, getLineLastPositions: getAngraLineLastPositions, replaceAngraOnibusSnapshot } = require('./stores/angraOnibusStore');
const { resolveRioTimestamp, resolveAngraTimestamp, summarizeLines } = require('./utils/stats');
const { loadItinerarioIntoMemory } = require('./stores/itinerarioStore');
const { startScheduler, stopScheduler } = require('./jobs');

const app = express();

const AUTH_USERNAME = process.env.AUTH_USERNAME;
const AUTH_PASSWORD = process.env.AUTH_PASSWORD;
const JWT_SECRET = process.env.JWT_SECRET;
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN;


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
const CATCHUP_HOURS = Number(process.env.CATCHUP_HOURS) || 1;

async function bootstrap() {
    await startScheduler();
}

bootstrap().catch(err => console.error('[server] bootstrap failed', err));

const server = app.listen(process.env.BACKEND_PORT || 3001, () => {
    console.log('GPS Backend server running');
});

process.on('SIGTERM', async () => {
    console.log('[server] SIGTERM received, shutting down...');
    await stopScheduler();
    server.close(() => {
        console.log('[server] HTTP server closed');
        process.exit(0);
    });
});

process.on('SIGINT', async () => {
    console.log('[server] SIGINT received, shutting down...');
    await stopScheduler();
    server.close(() => {
        console.log('[server] HTTP server closed');
        process.exit(0);
    });
});
