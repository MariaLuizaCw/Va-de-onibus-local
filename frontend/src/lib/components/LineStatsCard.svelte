<!-- Purpose: exibe estatísticas da linha selecionada ao lado da tabela de resultados. -->
<script lang="ts">
    import type { LineStats } from '$lib/types/api';

    export let lineStats: LineStats | null = null;
</script>

{#if lineStats}
    <div class="line-stats-card">
        <h4 class="line-title">Linha {lineStats.linha}</h4>
        <div class="line-info">
            <div class="info-row">
                <span class="info-label">Última atualização:</span>
                <span class="info-value">
                    {#if lineStats.lastUpdate}
                        {new Date(lineStats.lastUpdate).toLocaleString('pt-BR')}
                    {:else}
                        —
                    {/if}
                </span>
            </div>
            <div class="info-row">
                <span class="info-label">Status:</span>
                <span class="info-value status" class:active={lineStats.isActive} class:inactive={!lineStats.isActive}>
                    {lineStats.isActive ? 'Ativa' : 'Inativa'}
                </span>
            </div>
            <div class="info-row">
                <span class="info-label">Ordens ativas:</span>
                <span class="info-value">{lineStats.activeOrders} / {lineStats.totalOrders}</span>
            </div>
        </div>
    </div>
{/if}

<style>
    .line-stats-card {
        background: rgba(15, 23, 42, 0.9);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 1rem;
        padding: 1.25rem;
        min-width: 220px;
    }

    .line-title {
        margin: 0 0 1rem;
        font-size: 1.1rem;
        color: #f8fafc;
    }

    .line-info {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
    }

    .info-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.5rem;
    }

    .info-label {
        font-size: 0.8rem;
        color: rgba(226, 232, 240, 0.7);
    }

    .info-value {
        font-size: 0.9rem;
        color: #f8fafc;
        font-weight: 500;
    }

    .status.active {
        color: #34d399;
    }

    .status.inactive {
        color: #f87171;
    }
</style>
