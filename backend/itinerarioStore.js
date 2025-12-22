const turf = require('@turf/turf');
const { dbPool } = require('./database');

const itinerarioByLinha = Object.create(null);
let loaded = false;

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function loadItinerarioIntoMemory() {
    const text = `
        SELECT
            numero_linha,
            sentido,
            ST_AsGeoJSON(the_geom)::json AS geom
        FROM public.itinerario
        WHERE habilitado = true
    `;

    const result = await dbPool.query(text);

    for (const row of result.rows) {
        const linha = String(row.numero_linha);
        const sentido = String(row.sentido);
        const geom = row.geom;

        if (!geom) continue;

        let line;
        try {
            if (geom.type === 'LineString') {
                line = turf.lineString(geom.coordinates);
            } else if (geom.type === 'MultiLineString') {
                line = turf.multiLineString(geom.coordinates);
            } else {
                continue;
            }
        } catch (err) {
            continue;
        }

        if (!itinerarioByLinha[linha]) itinerarioByLinha[linha] = [];
        itinerarioByLinha[linha].push({ sentido, line });
    }

    loaded = true;
    console.log(`[itinerario] loaded linhas=${Object.keys(itinerarioByLinha).length} rows=${result.rows.length}`);
}

function isLoaded() {
    return loaded;
}

function getItinerariosByLinha(linha) {
    return itinerarioByLinha[String(linha)] || [];
}

function chooseSentidoForPoint(linha, lon, lat) {
    const candidates = getItinerariosByLinha(linha);
    if (!candidates || candidates.length === 0) return null;

    const lonN = Number(lon);
    const latN = Number(lat);
    if (!Number.isFinite(lonN) || !Number.isFinite(latN)) return null;

    const pt = turf.point([lonN, latN]);

    let best = null;

    for (const cand of candidates) {
        try {
            if (!cand.line) continue;
            const distMeters = turf.pointToLineDistance(pt, cand.line, { units: 'meters' });
            if (best == null || distMeters < best.distancia_metros) {
                best = { sentido: cand.sentido, distancia_metros: distMeters };
            }
        } catch (err) {
            continue;
        }
    }

    if (!best) return null;
    if (best.distancia_metros > MAX_SNAP_DISTANCE_METERS) return null;

    return best;
}

module.exports = {
    loadItinerarioIntoMemory,
    isLoaded,
    getItinerariosByLinha,
    chooseSentidoForPoint,
    MAX_SNAP_DISTANCE_METERS,
};
