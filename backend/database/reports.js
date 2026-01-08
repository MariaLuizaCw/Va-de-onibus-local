const { dbPool } = require('./pool');
const { API_TIMEZONE, formatDateYYYYMMDDInTimeZone } = require('../utils');

const MAX_SNAP_DISTANCE_METERS = Number(process.env.MAX_SNAP_DISTANCE_METERS) || 300;

async function generateSentidoCoverageReport() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const reportDate = formatDateYYYYMMDDInTimeZone(yesterday, API_TIMEZONE);

    try {
        const result = await dbPool.query(
            'SELECT * FROM fn_generate_sentido_coverage_report($1::date, $2, $3)',
            [reportDate, API_TIMEZONE, MAX_SNAP_DISTANCE_METERS]
        );
        const rowCount = result.rows[0]?.fn_generate_sentido_coverage_report || 0;
    } catch (err) {
        console.error('[coverage][rio]  Error generating sentido coverage report:', err);
    }
}

async function generateAngraRouteTypeReport() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const reportDate = formatDateYYYYMMDDInTimeZone(yesterday, API_TIMEZONE);

    try {
        const result = await dbPool.query(
            'SELECT * FROM fn_generate_angra_route_type_report($1::date, $2)',
            [reportDate, API_TIMEZONE]
        );
        const rowCount = result.rows[0]?.fn_generate_angra_route_type_report || 0;
    } catch (err) {
        console.error('[coverage][angra] Error generating Angra route_type report:', err);
    }
}

module.exports = {
    generateSentidoCoverageReport,
    generateAngraRouteTypeReport,
};
