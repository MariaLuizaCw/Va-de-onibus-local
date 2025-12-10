const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = 3001;
const LOG_FILE = path.join(__dirname, 'polling_logs.txt');

app.use(cors());
app.use(express.json());

// Helper: Append log to file
function logToFile(message) {
    const timestamp = new Date().toISOString();
    const logLine = `[${timestamp}] ${message}\n`;

    fs.appendFile(LOG_FILE, logLine, (err) => {
        if (err) console.error('Failed to write to log file:', err);
    });
}

// In-memory storage
const MAX_HISTORY = 5;
let lastPositions = [];
let latestStats = {
    uptime: 0, // seconds
    lastFetch: null,
    totalPolls: 0,
    successPolls: 0,
    failedPolls: 0,
    lastBatchStats: {
        totalRecords: 0,
        uniqueLines: 0,
        avgTotalTimeMs: 0
    }
};

const startTime = Date.now();
const seenRecords = new Set(); // For deduplication

// Helper: Format Date for API (YYYY-MM-DD HH:MM:SS)
function formatDate(date) {
    const pad = (n) => n.toString().padStart(2, '0');
    const YYYY = date.getFullYear();
    const MM = pad(date.getMonth() + 1);
    const DD = pad(date.getDate());
    const HH = pad(date.getHours());
    const MIN = pad(date.getMinutes());
    const SS = pad(date.getSeconds());
    return `${YYYY}-${MM}-${DD} ${HH}:${MIN}:${SS}`;
}

async function fetchGPSData() {
    latestStats.totalPolls++;
    const now = new Date();
    // 3 minute overlap window
    const startWindow = new Date(now.getTime() - 3 * 60 * 1000);

    const dataInicial = formatDate(startWindow);
    const dataFinal = formatDate(now);

    const startMsg = `Polling GPS data: ${dataInicial} to ${dataFinal}`;
    console.log(startMsg);
    logToFile(`START REQUEST: ${dataInicial} -> ${dataFinal}`);

    try {
        const response = await axios.get('https://dados.mobilidade.rio/gps/sppo', {
            params: {
                dataInicial,
                dataFinal
            }
        });

        const records = response.data;
        latestStats.lastFetch = now;
        latestStats.successPolls++;

        // Process batch stats
        let totalLatency = 0;
        const uniqueLinesSet = new Set();
        let newRecordsCount = 0;

        // Sort by timestamp (datahora) to keep latest at the end/top logic later
        // API returns array of objects

        // Deduplication and updating lastPositions
        records.forEach(record => {
            // Composite ID: ordem + datahora
            const recordId = `${record.ordem}_${record.datahora}`;

            // Stats calculation (doing it for all received to reflect current batch quality)
            uniqueLinesSet.add(record.linha);
            if (record.datahora && record.datahoraservidor) {
                totalLatency += (reqDataHora(record.datahoraservidor) - reqDataHora(record.datahora));
            }

            if (!seenRecords.has(recordId)) {
                seenRecords.add(recordId);
                newRecordsCount++;

                // Add to lastPositions
                lastPositions.push(record);
            }
        });

        // Limit seenRecords to prevent memory leak (optional, simplified for now)
        // In a real app we'd clear old keys. 
        if (seenRecords.size > 10000) seenRecords.clear(); // Simple flush

        // Keep only last 5 positions globally (or specifically? User said "last 5 positions")
        // Assuming global last 5 received.
        if (lastPositions.length > MAX_HISTORY) {
            lastPositions = lastPositions.slice(-MAX_HISTORY);
        }

        // Update Stats
        latestStats.lastBatchStats = {
            totalRecords: records.length,
            uniqueLines: uniqueLinesSet.size,
            avgTotalTimeMs: records.length > 0 ? (totalLatency / records.length).toFixed(2) : 0
        };

        const successMsg = `Success! Fetched ${records.length} records. New: ${newRecordsCount}. Stats: ${JSON.stringify(latestStats.lastBatchStats)}`;
        console.log(successMsg);
        logToFile(successMsg);

    } catch (error) {
        const errorMsg = `Error fetching data: ${error.message}`;
        console.error(errorMsg);
        logToFile(errorMsg);
        latestStats.failedPolls++;
    }
}

// Helper to parse timestamp safely (handles string vs number if API varies)
function reqDataHora(val) {
    // If it's a string timestamp or number
    return new Date(Number(val)).getTime();
}

// Start Polling (every 60s)
setInterval(fetchGPSData, 60000);
// Initial fetch
fetchGPSData();

// Routes
app.get('/api/status', (req, res) => {
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    res.json({
        ...latestStats,
        uptime
    });
});

app.get('/api/data', (req, res) => {
    res.json({
        data: lastPositions,
        logs: [`Last fetch: ${latestStats.lastFetch ? latestStats.lastFetch.toISOString() : 'None'}`]
    });
});

app.listen(PORT, () => {
    console.log(`GPS Backend server running on port ${PORT}`);
});
