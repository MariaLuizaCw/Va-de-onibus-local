<script lang="ts">
    import { onMount } from 'svelte';

    // Types
    interface Record {
        ordem: string;
        latitude: string;
        longitude: string;
        datahora: number;
        velocidade: string;
        linha: string;
        datahoraservidor: number;
    }

    interface Stats {
        uptime: number;
        lastFetch: string | null;
        totalPolls: number;
        successPolls: number;
        failedPolls: number;
        lastBatchStats: {
            totalRecords: number;
            uniqueLines: number;
            avgTotalTimeMs: number;
        };
    }

    let stats: Stats | null = null;
    let records: Record[] = [];
    let logs: string[] = [];
    let error: string | null = null;
    let loading = true;

    async function fetchData() {
        try {
            // Fetch Status
            const statusRes = await fetch('http://localhost:3001/api/status');
            stats = await statusRes.json();

            // Fetch Data
            const dataRes = await fetch('http://localhost:3001/api/data');
            const dataJson = await dataRes.json();
            records = dataJson.data;
            logs = dataJson.logs;
            
            error = null;
        } catch (e) {
            console.error(e);
            error = "Failed to connect to backend.";
        } finally {
            loading = false;
        }
    }

    onMount(() => {
        fetchData();
        const interval = setInterval(fetchData, 5000); // Polling frontend every 5s to check for updates
        return () => clearInterval(interval);
    });

    // Format uptime
    $: uptimeFormatted = stats ? new Date(stats.uptime * 1000).toISOString().substr(11, 8) : '00:00:00';
    
    // Format timestamp
    function formatTime(ts: number) {
        return new Date(ts).toLocaleTimeString();
    }
</script>

<svelte:head>
    <title>GPS Dashboard</title>
</svelte:head>

<main class="dashboard">
    <header>
        <h1>Rio GPS Monitor</h1>
        <div class="status-badge" class:error={!!error}>
            {error ? 'Offline' : 'Live System'}
        </div>
    </header>

    {#if loading && !stats}
        <div class="loading">Initializing Dashboard...</div>
    {:else}
        <!-- Stats Grid -->
        <div class="stats-grid">
            <div class="card">
                <h3>Uptime</h3>
                <div class="value">{uptimeFormatted}</div>
            </div>
            <div class="card">
                <h3>Total Polls</h3>
                <div class="value">{stats?.totalPolls || 0}</div>
                <div class="sub">Success: {stats?.successPolls} | Failed: {stats?.failedPolls}</div>
            </div>
            <div class="card highlight">
                <h3>Avg Latency</h3>
                <div class="value">{stats?.lastBatchStats?.avgTotalTimeMs || 0} <span class="unit">ms</span></div>
            </div>
            <div class="card">
                <h3>Active Lines</h3>
                <div class="value">{stats?.lastBatchStats?.uniqueLines || 0}</div>
            </div>
        </div>

        <!-- Main Content -->
        <div class="content-grid">
            <!-- Latest Positions -->
            <section class="card table-card">
                <h2>Latest GPS Records (Mem 5)</h2>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Ordem</th>
                                <th>Linha</th>
                                <th>Lat / Lon</th>
                                <th>Time (GPS)</th>
                                <th>Speed</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each records.reverse() as record}
                                <tr>
                                    <td><span class="ordem">{record.ordem}</span></td>
                                    <td>{record.linha}</td>
                                    <td>{record.latitude}, {record.longitude}</td>
                                    <td>{formatTime(record.datahora)}</td>
                                    <td>{record.velocidade} km/h</td>
                                </tr>
                            {:else}
                                <tr><td colspan="5" style="text-align:center; opacity: 0.5;">No data available yet</td></tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            </section>

            <!-- Logs -->
            <section class="card logs-card">
                <h2>System Logs</h2>
                <div class="logs">
                    {#each logs as log}
                        <div class="log-entry">
                            <span class="dot"></span> {log}
                        </div>
                    {/each}
                    {#if error}
                         <div class="log-entry error">
                            <span class="dot red"></span> {error}
                        </div>
                    {/if}
                </div>
            </section>
        </div>
    {/if}
</main>

<style>
    /* Premium Dashboard Styles */
    .dashboard {
        max-width: 1200px;
        margin: 0 auto;
        padding: 2rem;
    }

    header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 2rem;
    }

    h1 {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(to right, #60a5fa, #c084fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }

    .status-badge {
        padding: 0.5rem 1rem;
        background: rgba(16, 185, 129, 0.2);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.4);
        border-radius: 9999px;
        font-size: 0.875rem;
        font-weight: 600;
        backdrop-filter: blur(4px);
    }
    .status-badge.error {
        background: rgba(239, 68, 68, 0.2);
        color: #f87171;
        border-color: rgba(239, 68, 68, 0.4);
    }

    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1.5rem;
        margin-bottom: 2rem;
    }

    .card {
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 1rem;
        padding: 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        transition: transform 0.2s;
    }
    .card:hover {
        transform: translateY(-2px);
        border-color: rgba(255, 255, 255, 0.2);
    }

    .card h3 {
        margin: 0 0 0.5rem 0;
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94a3b8;
    }

    .card .value {
        font-size: 2rem;
        font-weight: 700;
        color: #f8fafc;
    }
    .card .unit {
        font-size: 1rem;
        color: #cbd5e1;
    }
    .card .sub {
        font-size: 0.75rem;
        margin-top: 0.5rem;
        color: #64748b;
    }

    .highlight .value {
        color: #c084fc;
    }

    .content-grid {
        display: grid;
        grid-template-columns: 2fr 1fr;
        gap: 1.5rem;
    }
    @media (max-width: 768px) {
        .content-grid {
            grid-template-columns: 1fr;
        }
    }

    h2 {
        font-size: 1.25rem;
        margin-bottom: 1rem;
        color: #e2e8f0;
    }

    /* Table Styles */
    .table-container {
        overflow-x: auto;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.875rem;
    }
    th {
        text-align: left;
        padding: 1rem;
        color: #94a3b8;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    td {
        padding: 1rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        color: #cbd5e1;
    }
    tr:last-child td {
        border-bottom: none;
    }
    .ordem {
        background: rgba(56, 189, 248, 0.15);
        color: #38bdf8;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-family: monospace;
    }

    /* Logs Styles */
    .logs {
        font-family: monospace;
        font-size: 0.8rem;
        color: #94a3b8;
    }
    .log-entry {
        padding: 0.5rem 0;
        border-bottom: 1px dashed rgba(255, 255, 255, 0.1);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .dot {
        width: 6px;
        height: 6px;
        background: #34d399;
        border-radius: 50%;
        display: inline-block;
    }
    .dot.red {
        background: #f87171;
    }
</style>
