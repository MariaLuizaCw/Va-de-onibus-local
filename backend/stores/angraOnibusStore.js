const angra_onibus = Object.create(null);

function replaceAngraOnibusSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return;

    for (const key of Object.keys(angra_onibus)) {
        delete angra_onibus[key];
    }

    for (const linhaKey of Object.keys(snapshot)) {
        const positions = snapshot[linhaKey];
        if (!Array.isArray(positions)) continue;

        for (const pos of positions) {
            addPosition(pos);
        }
    }
}

function ensureBucket(linha, veiculo) {
    const linhaKey = String(linha);
    const veiculoKey = String(veiculo);

    if (!angra_onibus[linhaKey]) angra_onibus[linhaKey] = Object.create(null);
    if (!angra_onibus[linhaKey][veiculoKey]) angra_onibus[linhaKey][veiculoKey] = [];

    return angra_onibus[linhaKey][veiculoKey];
}

function addPosition(record) {
    if (!record) return;
    // Use original API field names: LineNumber and VehicleIntegrationCode
    if (record.LineNumber == null || record.VehicleIntegrationCode == null) return;

    const bucket = ensureBucket(record.LineNumber, record.VehicleIntegrationCode);

    // Store record as-is from API
    bucket.push(record);

    // Sort by EventDate (most recent first)
    bucket.sort((a, b) => {
        const dateA = new Date(a.EventDate).getTime();
        const dateB = new Date(b.EventDate).getTime();
        return dateB - dateA;
    });
    if (bucket.length > 3) bucket.length = 3;
}

function addPositions(records) {
    if (!Array.isArray(records) || records.length === 0) return;
    for (const record of records) addPosition(record);
}

function getAngraOnibus() {
    const result = Object.create(null);
    for (const linhaKey of Object.keys(angra_onibus)) {
        result[linhaKey] = getLineLastPositions(linhaKey);
    }
    return result;
}

function getLine(linha) {
    const linhaKey = String(linha);
    return angra_onibus[linhaKey] || Object.create(null);
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
    const byLinha = angra_onibus[linhaKey];
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
    replaceAngraOnibusSnapshot,
    getAngraOnibus,
    getLine,
    getLineLastPositions,
    getLastPositions,
    getLastPosition,
};
