const rio_onibus = Object.create(null);

function replaceRioOnibusSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return;

    for (const key of Object.keys(rio_onibus)) {
        delete rio_onibus[key];
    }

    for (const linhaKey of Object.keys(snapshot)) {
        const positions = snapshot[linhaKey];
        if (!Array.isArray(positions)) continue;

        for (const pos of positions) {
            addPosition(pos);
        }
    }
}

function ensureBucket(linha, ordem) {
    const linhaKey = String(linha);
    const ordemKey = String(ordem);

    if (!rio_onibus[linhaKey]) rio_onibus[linhaKey] = Object.create(null);
    if (!rio_onibus[linhaKey][ordemKey]) rio_onibus[linhaKey][ordemKey] = [];

    return rio_onibus[linhaKey][ordemKey];
}

function addPosition(record) {
    if (!record) return;
    if (record.linha == null || record.ordem == null) return;

    const bucket = ensureBucket(record.linha, record.ordem);

    const latitude = typeof record.latitude === 'string'
        ? Number(record.latitude.replace(',', '.'))
        : Number(record.latitude);
    const longitude = typeof record.longitude === 'string'
        ? Number(record.longitude.replace(',', '.'))
        : Number(record.longitude);

    const pos = {
        ordem: record.ordem,
        linha: record.linha,
        latitude,
        longitude,
        datahora: Number(record.datahora),
        velocidade: Number(record.velocidade),
        datahoraenvio: Number(record.datahoraenvio),
        datahoraservidor: Number(record.datahoraservidor),
        sentido: record.sentido != null ? String(record.sentido) : null,
        distancia_metros: record.distancia_metros != null ? Number(record.distancia_metros) : null
    };

    bucket.push(pos);

    bucket.sort((a, b) => (Number(b.datahora) || 0) - (Number(a.datahora) || 0));
    if (bucket.length > 3) bucket.length = 3;
}

function addPositions(records) {
    if (!Array.isArray(records) || records.length === 0) return;
    for (const record of records) addPosition(record);
}

function getRioOnibus() {
    const result = Object.create(null);
    for (const linhaKey of Object.keys(rio_onibus)) {
        result[linhaKey] = getLineLastPositions(linhaKey);
    }
    return result;
}

function getLine(linha) {
    const linhaKey = String(linha);
    return rio_onibus[linhaKey] || Object.create(null);
}

function getLineLastPositions(linha) {
    const ordens = getLine(linha);
    const result = [];

    for (const ordemKey of Object.keys(ordens)) {
        const bucket = ordens[ordemKey];
        if (Array.isArray(bucket) && bucket.length > 0) result.push(bucket[0]);
    }

    return result;
}

function getLastPositions(linha, ordem) {
    const linhaKey = String(linha);
    const ordemKey = String(ordem);
    const byLinha = rio_onibus[linhaKey];
    if (!byLinha) return [];
    const bucket = byLinha[ordemKey];
    if (!bucket) return [];
    return bucket;
}

function getLastPosition(linha, ordem) {
    const bucket = getLastPositions(linha, ordem);
    return bucket.length > 0 ? bucket[0] : null;
}

module.exports = {
    addPosition,
    addPositions,
    replaceRioOnibusSnapshot,
    getRioOnibus,
    getLine,
    getLineLastPositions,
    getLastPositions,
    getLastPosition,
};
