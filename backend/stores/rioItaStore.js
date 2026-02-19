const rioita_onibus = Object.create(null);

function replaceRioItaOnibusSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return;

    for (const key of Object.keys(rioita_onibus)) {
        delete rioita_onibus[key];
    }

    for (const ordemKey of Object.keys(snapshot)) {
        const positions = snapshot[ordemKey];
        if (!Array.isArray(positions)) continue;

        for (const pos of positions) {
            addPosition(pos);
        }
    }
}

function ensureBucket(ordem) {
    const ordemKey = String(ordem);

    if (!rioita_onibus[ordemKey]) rioita_onibus[ordemKey] = [];

    return rioita_onibus[ordemKey];
}

function parseDataHora(datahoraStr) {
    if (!datahoraStr) return 0;
    // Formato: "DD/MM/YYYY HH:mm:ss"
    const parts = datahoraStr.match(/(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2}):(\d{2})/);
    if (!parts) return 0;
    const [, day, month, year, hour, min, sec] = parts;
    return new Date(year, month - 1, day, hour, min, sec).getTime();
}

function addPosition(record) {
    if (!record) return;
    // Prefixo é equivalente a ordem
    if (record.Prefixo == null) return;

    const bucket = ensureBucket(record.Prefixo);

    // Verificar se já existe registro com mesmo Prefixo e DataHora
    const existingIndex = bucket.findIndex(r => 
        r.Prefixo === record.Prefixo && r.DataHora === record.DataHora
    );

    if (existingIndex >= 0) {
        // Já existe, não adicionar duplicado
        return;
    }

    bucket.push(record);

    // Ordenar por DataHora (mais recente primeiro)
    bucket.sort((a, b) => {
        const dateA = parseDataHora(a.DataHora);
        const dateB = parseDataHora(b.DataHora);
        return dateB - dateA;
    });

    // Manter apenas as 3 últimas posições
    if (bucket.length > 3) bucket.length = 3;
}

function getRioItaOnibus() {
    return rioita_onibus;
}

function getLastPositions(ordem) {
    const ordemKey = String(ordem);
    const bucket = rioita_onibus[ordemKey];
    if (!bucket) return [];
    return bucket;
}

function getLastPosition(ordem) {
    const bucket = getLastPositions(ordem);
    return bucket.length > 0 ? bucket[0] : null;
}

function getAllLastPositions() {
    const result = [];
    for (const ordemKey of Object.keys(rioita_onibus)) {
        const bucket = rioita_onibus[ordemKey];
        if (Array.isArray(bucket) && bucket.length > 0) {
            result.push(bucket[0]);
        }
    }
    return result;
}

module.exports = {
    addPosition,
    replaceRioItaOnibusSnapshot,
    getRioItaOnibus,
    getLastPositions,
    getLastPosition,
    getAllLastPositions
};
