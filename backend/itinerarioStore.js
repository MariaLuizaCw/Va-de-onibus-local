const turf = require('@turf/turf');
let dbPool = null;

function getDbPool() {
    if (!dbPool) {
        dbPool = require('./database').dbPool;
    }
    return dbPool;
}

const itinerarioByLinha = Object.create(null);
let loaded = false;

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function loadItinerarioIntoMemory() {
    for (const key of Object.keys(itinerarioByLinha)) {
        delete itinerarioByLinha[key];
    }

    let result;
    try {
        result = await getDbPool().query('SELECT * FROM fn_get_itinerarios_habilitados()');
    } catch (err) {
        console.error('[itinerario] Query error:', err);
        throw err;
    }

    for (const row of result.rows) {
        const linha = String(row.numero_linha);
        const sentido = String(row.sentido);
        const itinerario_id = row.itinerario_id;
        const route_name = row.route_name;
        const geom = row.geom;

        let line = null;
        if (geom) {
            try {
                if (geom.type === 'LineString') {
                    line = turf.lineString(geom.coordinates);
                } else if (geom.type === 'MultiLineString') {
                    line = turf.multiLineString(geom.coordinates);
                }
            } catch (err) {
                line = null;
            }
        }

        if (!itinerarioByLinha[linha]) itinerarioByLinha[linha] = [];
        itinerarioByLinha[linha].push({ sentido, itinerario_id, route_name, line });
    }

    loaded = true;
}

function isLoaded() {
    return loaded;
}

function getItinerariosByLinha(linha) {
    return itinerarioByLinha[String(linha)] || [];
}

function computeSentidoMetricsForPoint(linha, lon, lat) {
    const candidates = getItinerariosByLinha(linha);
    if (!candidates || candidates.length === 0) return null;

    const lonN = Number(lon);
    const latN = Number(lat);
    if (!Number.isFinite(lonN) || !Number.isFinite(latN)) return null;

    const pt = turf.point([lonN, latN]);

    const distancias = [];
    let best = null;

    for (const cand of candidates) {
        try {
            if (!cand.line) continue;
            const distMeters = turf.pointToLineDistance(pt, cand.line, { units: 'meters' });
            distancias.push({ sentido: cand.sentido, distancia_metros: distMeters });
            if (best == null || distMeters < best.distancia_metros) {
                best = { sentido: cand.sentido, distancia_metros: distMeters };
            }
        } catch (err) {
            continue;
        }
    }

    distancias.sort((a, b) => (a.distancia_metros || 0) - (b.distancia_metros || 0));
    if (!best) return null;

    return { best, distancias };
}

function chooseSentidoForPoint(linha, lon, lat) {
    const metrics = computeSentidoMetricsForPoint(linha, lon, lat);
    if (!metrics) return null;
    if (!metrics.best) return null;
    if (metrics.best.distancia_metros > MAX_SNAP_DISTANCE_METERS) return null;
    return metrics.best;
}

module.exports = {
    loadItinerarioIntoMemory,
    isLoaded,
    getItinerariosByLinha,
    computeSentidoMetricsForPoint,
    chooseSentidoForPoint,
    MAX_SNAP_DISTANCE_METERS,
};
