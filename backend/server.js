const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { fetchRioGPSData } = require('./fetchers/rioFetcher');
const { fetchAngraGPSData, fetchCircularLines, getCompanyVehicles: getSsxVehicles, getLineLastPositions: getSsxLineLastPositions, getLoadedCompanies: getSsxLoadedCompanies, getStoreStatus: getSsxStoreStatus, SSX_INTEGRATIONS } = require('./fetchers/ssxFetcher');
const { loadLatestRioOnibusSnapshot } = require('./database/index');
const { getRioOnibus, getLineLastPositions: getRioLineLastPositions, replaceRioOnibusSnapshot } = require('./stores/rioOnibusStore');
const { getCompanyVehicles: getGtfsVehicles, getLineLastPositions: getGtfsLineLastPositions, getStoreStatus: getGtfsVehiclesStatus } = require('./stores/gtfsVehiclesStore');
const { getCompanyRoutes: getGtfsRoutes, getStoreStatus: getGtfsRoutesStatus, getLoadedCompanies: getGtfsLoadedCompanies } = require('./stores/gtfsRoutesStore');
const { resolveRioTimestamp, summarizeLines } = require('./utils/stats');
const { loadItinerarioIntoMemory } = require('./stores/itinerarioStore');
const { startScheduler, stopScheduler } = require('./jobs');
const { getJobStats, getJobTimeline, getJobHourlyDistribution } = require('./database/jobStats');


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

// Servir arquivos estáticos do common_settings
app.use('/common_settings', express.static('../common_settings'));

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

// GTFS-RT endpoints (autenticados)
router.get('/gtfs/companies', (req, res) => {
    const companies = getGtfsLoadedCompanies();
    return res.json({ companies });
});

router.get('/gtfs/status', (req, res) => {
    return res.json({
        routes: getGtfsRoutesStatus(),
        vehicles: getGtfsVehiclesStatus()
    });
});

router.get('/stats/lines', (req, res) => {
    const rioStats = summarizeLines(getRioOnibus(), resolveRioTimestamp);
    return res.json({
        rio: rioStats
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

// GTFS-RT endpoint (autenticado)
router.post('/gtfs_onibus', (req, res) => {
    const { empresa, linha } = req.body || {};

    if (!empresa || String(empresa).trim() === '') {
        return res.status(400).json({ error: 'Parâmetro empresa é obrigatório' });
    }

    const companyKey = String(empresa).toUpperCase();

    if (linha != null && String(linha).trim() !== '') {
        const positions = getGtfsLineLastPositions(companyKey, linha);
        return res.json({ empresa: companyKey, linha: String(linha), positions });
    }

    return res.json({ empresa: companyKey, data: getGtfsVehicles(companyKey) });
});

// SSX endpoints (Angra, BarraPirai, PedroAntonio, Resendense)
router.get('/ssx/companies', (req, res) => {
    const companies = getSsxLoadedCompanies();
    const available = Object.keys(SSX_INTEGRATIONS).map(key => ({
        key,
        name: SSX_INTEGRATIONS[key].name,
        token: SSX_INTEGRATIONS[key].token
    }));
    return res.json({ loaded: companies, available });
});

router.get('/ssx/status', (req, res) => {
    return res.json(getSsxStoreStatus());
});

router.post('/ssx_onibus', (req, res) => {
    const { empresa, linha } = req.body || {};

    if (!empresa || String(empresa).trim() === '') {
        return res.status(400).json({ error: 'Parâmetro empresa é obrigatório (angra, barrapirai, pedroantonio, resendense)' });
    }

    const companyKey = String(empresa).toLowerCase();

    // Validar se é uma empresa SSX válida
    if (!SSX_INTEGRATIONS[companyKey]) {
        return res.status(400).json({ 
            error: `Empresa inválida: ${empresa}`,
            validCompanies: Object.keys(SSX_INTEGRATIONS)
        });
    }

    if (linha != null && String(linha).trim() !== '') {
        const positions = getSsxLineLastPositions(companyKey, linha);
        return res.json({ empresa: companyKey, linha: String(linha), positions });
    }

    return res.json({ empresa: companyKey, data: getSsxVehicles(companyKey) });
});

router.get('/gtfs/routes/:empresa', (req, res) => {
    const empresa = req.params.empresa.toUpperCase();
    const routes = getGtfsRoutes(empresa);
    return res.json({ empresa, routes });
});

// Job Stats endpoints
router.get('/jobs/stats', async (req, res) => {
    try {
        const date = req.query.date || new Date().toISOString().split('T')[0];
        const stats = await getJobStats(date);
        res.json(stats);
    } catch (error) {
        console.error('[jobs/stats] Error:', error.message);
        res.status(500).json({ error: 'Erro ao buscar estatísticas de jobs' });
    }
});

router.get('/jobs/timeline/:jobName', async (req, res) => {
    try {
        const { jobName } = req.params;
        const date = req.query.date || new Date().toISOString().split('T')[0];
        const includeChildren = req.query.includeChildren === 'true';
        const timeline = await getJobTimeline(jobName, date, includeChildren);
        res.json(timeline);
    } catch (error) {
        console.error('[jobs/timeline] Error:', error.message);
        res.status(500).json({ error: 'Erro ao buscar timeline do job' });
    }
});


router.get('/jobs/hourly/:jobName', async (req, res) => {
    try {
        const { jobName } = req.params;
        const date = req.query.date || new Date().toISOString().split('T')[0];
        const distribution = await getJobHourlyDistribution(jobName, date);
        res.json(distribution);
    } catch (error) {
        console.error('[jobs/hourly] Error:', error.message);
        res.status(500).json({ error: 'Erro ao buscar distribuição horária' });
    }
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
