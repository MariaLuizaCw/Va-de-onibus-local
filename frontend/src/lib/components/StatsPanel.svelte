<!-- Purpose: exibe estatísticas gerais (linhas/ordens ativas e totais) no topo do dashboard. -->
<script lang="ts">
    import type { CityStats } from '$lib/types/api';

    export let stats: CityStats | null = null;
    export let loading = false;
    export let cityLabel = '';
</script>

<div class="stats-panel">
    <h3 class="stats-title">{cityLabel}</h3>
    {#if loading}
        <p class="stats-loading">Carregando estatísticas...</p>
    {:else if stats}
        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-value">{stats.activeLines}</span>
                <span class="stat-label">Linhas Ativas</span>
            </div>
            <div class="stat-card">
                <span class="stat-value">{stats.totalLines}</span>
                <span class="stat-label">Linhas Totais</span>
            </div>
            <div class="stat-card">
                <span class="stat-value">{stats.activeOrders}</span>
                <span class="stat-label">Ordens Ativas</span>
            </div>
            <div class="stat-card">
                <span class="stat-value">{stats.totalOrders}</span>
                <span class="stat-label">Ordens Totais</span>
            </div>
        </div>
    {:else}
        <p class="stats-empty">Sem estatísticas disponíveis.</p>
    {/if}
</div>

<style>
    .stats-panel {
        background: rgba(15, 23, 42, 0.9);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 1rem;
        padding: 1.25rem;
        margin-bottom: 1.5rem;
    }

    .stats-title {
        margin: 0 0 1rem;
        font-size: 1rem;
        color: #a5b4fc;
        text-transform: uppercase;
        letter-spacing: 0.15em;
    }

    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 1rem;
    }

    .stat-card {
        background: rgba(30, 41, 59, 0.8);
        border-radius: 0.75rem;
        padding: 1rem;
        text-align: center;
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }

    .stat-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: #38bdf8;
    }

    .stat-label {
        font-size: 0.75rem;
        color: rgba(226, 232, 240, 0.7);
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }

    .stats-loading,
    .stats-empty {
        color: rgba(226, 232, 240, 0.6);
        font-size: 0.9rem;
        margin: 0;
    }
</style>
