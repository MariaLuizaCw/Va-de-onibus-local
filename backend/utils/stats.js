const HOURS_48_MS = 48 * 60 * 60 * 1000;

function resolveRioTimestamp(record) {
    if (!record) return null;
    const ts = record.datahora ?? record.datahoraservidor ?? record.datahoraenvio;
    const value = typeof ts === 'string' ? Number(ts) : ts;
    return Number.isFinite(value) ? value : null;
}

function resolveAngraTimestamp(record) {
    if (!record || !record.EventDate) return null;
    const value = new Date(record.EventDate).getTime();
    return Number.isFinite(value) ? value : null;
}

function summarizeLines(snapshot, resolveTimestamp) {
    const now = Date.now();
    const threshold = now - HOURS_48_MS;

    let totalLines = 0;
    let activeLines = 0;
    let totalOrders = 0;
    let activeOrders = 0;

    const lines = [];

    for (const linhaKey of Object.keys(snapshot)) {
        const orders = snapshot[linhaKey];
        if (!Array.isArray(orders) || orders.length === 0) continue;

        totalLines += 1;
        const lineOrderCount = orders.length;
        totalOrders += lineOrderCount;

        let latestTimestamp = 0;
        let lineActiveOrders = 0;

        for (const orderBucket of orders) {
            const record = Array.isArray(orderBucket) && orderBucket.length > 0 ? orderBucket[0] : orderBucket;
            if (!record) continue;
            const ts = Number(resolveTimestamp(record));
            if (!Number.isFinite(ts)) continue;

            if (ts > latestTimestamp) {
                latestTimestamp = ts;
            }

            if (ts >= threshold) {
                lineActiveOrders += 1;
                activeOrders += 1;
            }
        }

        const isLineActive = latestTimestamp >= threshold;
        if (isLineActive) {
            activeLines += 1;
        }

        lines.push({
            linha: linhaKey,
            lastUpdate: latestTimestamp > 0 ? new Date(latestTimestamp).toISOString() : null,
            isActive: isLineActive,
            totalOrders: lineOrderCount,
            activeOrders: lineActiveOrders
        });
    }

    lines.sort((a, b) => {
        if (!a.lastUpdate && !b.lastUpdate) return a.linha.localeCompare(b.linha);
        if (!a.lastUpdate) return 1;
        if (!b.lastUpdate) return -1;
        return new Date(b.lastUpdate).getTime() - new Date(a.lastUpdate).getTime();
    });

    return {
        totalLines,
        activeLines,
        totalOrders,
        activeOrders,
        lines
    };
}

module.exports = {
    resolveRioTimestamp,
    resolveAngraTimestamp,
    summarizeLines
};
