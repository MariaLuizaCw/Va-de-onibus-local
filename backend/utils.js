const API_TIMEZONE = process.env.API_TIMEZONE || 'America/Sao_Paulo';


// Helper: format date for a specific timezone (e.g. America/Sao_Paulo)
function formatDateInTimeZone(date, timeZone = API_TIMEZONE) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23'
    });

    const parts = formatter.formatToParts(date).reduce((acc, part) => {
        if (part.type !== 'literal') acc[part.type] = part.value;
        return acc;
    }, {});

    const YYYY = parts.year;
    const MM = parts.month;
    const DD = parts.day;
    const HH = parts.hour;
    const MIN = parts.minute;
    const SS = parts.second;

    return `${YYYY}-${MM}-${DD} ${HH}:${MIN}:${SS}`;
}

function formatDateYYYYMMDDInTimeZone(date, timeZone = API_TIMEZONE) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });

    return formatter.format(date);
}

module.exports = {
    API_TIMEZONE,
    formatDateInTimeZone,
    formatDateYYYYMMDDInTimeZone,
};
