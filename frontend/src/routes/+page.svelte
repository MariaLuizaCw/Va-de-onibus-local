<script lang="ts">
    import RouteFilter from '$lib/components/RouteFilter.svelte';
    import ApiTable from '$lib/components/ApiTable.svelte';
    import type { ApiRecord } from '$lib/types/api';

    const BACKEND_BASE_URL = import.meta.env.VITE_BACKEND_URL ?? 'http://localhost:3001';

    type RawResponse = ApiRecord[] | { linha: string; ordens: ApiRecord[] } | Record<string, ApiRecord[]>;

    const cities = [
        { id: 'rio', label: 'Rio' },
        { id: 'angra', label: 'Angra dos Reis' }
    ];

    let city = 'rio';
    let linha = '';
    let tableData: ApiRecord[] = [];
    let errorMessage: string | null = null;
    let loading = false;
    let statusMessage = 'Selecione a cidade e informe a linha para ver os registros.';
    let lastFetched: string | null = null;
    $: preferredSortFields = city === 'rio' ? ['datahora'] : ['event_date', 'EventDate'];

    function normalizeRecords(payload: RawResponse, targetLine: string) {
        if (Array.isArray(payload)) {
            return payload;
        }

        if ('ordens' in payload && Array.isArray(payload.ordens)) {
            return payload.ordens;
        }

        const recordsMap = payload as Record<string, ApiRecord[]>;
        const lineRecords = recordsMap[targetLine];
        if (Array.isArray(lineRecords)) {
            return lineRecords;
        }

        return [];
    }

    async function fetchRouteData() {
        if (!linha.trim()) {
            errorMessage = 'Informe uma linha para continuar.';
            tableData = [];
            statusMessage = 'Linha obrigatória.';
            return;
        }

        loading = true;
        errorMessage = null;
        statusMessage = 'Carregando registros...';

        try {
            const endpoint = new URL(`${BACKEND_BASE_URL}/${city}_onibus`);
            endpoint.searchParams.set('linha', linha.trim());
            console.log('[api explorer] requesting', endpoint.toString());

            const response = await fetch(endpoint.toString());
            if (!response.ok) throw new Error(`Falha ao buscar: ${response.status}`);

            const payload: RawResponse = await response.json();
            tableData = normalizeRecords(payload, linha.trim());
            lastFetched = new Date().toLocaleTimeString();

            if (tableData.length === 0) {
                statusMessage = 'Nenhum registro encontrado para essa linha.';
            } else {
                statusMessage = `Foram retornados ${tableData.length} registros às ${lastFetched}.`;
            }
        } catch (err) {
            console.error(err);
            errorMessage = 'Não foi possível conectar ao backend.';
            statusMessage = 'Tente novamente mais tarde.';
            tableData = [];
        } finally {
            loading = false;
        }
    }

    function handleCityChange(event: CustomEvent<string>) {
        city = event.detail;
    }

    function handleLineChange(event: CustomEvent<string>) {
        linha = event.detail;
    }
</script>

<svelte:head>
    <title>API Explorer | Va-de-Onibus</title>
</svelte:head>

<main class="page">
        <section class="hero">
            <div>
                <p class="eyebrow">Dashboard API</p>
                <h1>Explore rotas do backend em tempo real</h1>
                <p class="lead">
                    Escolha o município, informe a linha pesquisada e visualize instantaneamente os dados retornados pela API.
                </p>
            </div>

            <RouteFilter
                {cities}
                selectedCity={city}
                {linha}
                {loading}
                {statusMessage}
                {errorMessage}
                on:submit={fetchRouteData}
                on:citychange={handleCityChange}
                on:linechange={handleLineChange}
            />
        </section>

        <section class="table-section">
            <header>
                <div>
                    <span class="subtitle">Resposta JSON</span>
                    <h2>{city === 'rio' ? 'Rio de Janeiro' : 'Angra dos Reis'} · Linha {linha || 'n/a'}</h2>
                </div>
                {#if lastFetched}
                    <span class="timestamp">Última atualização: {lastFetched}</span>
                {/if}
            </header>

            <ApiTable records={tableData} {preferredSortFields} />
        </section>
</main>

<style>
    :global(body) {
        margin: 0;
        font-family: 'Space Grotesk', 'Inter', sans-serif;
        background: radial-gradient(circle at top right, rgba(59, 130, 246, 0.25), transparent 45%),
            radial-gradient(circle at 20% 20%, rgba(236, 72, 153, 0.25), transparent 35%),
            #030712;
        color: #e2e8f0;
        min-height: 100vh;
    }

    .page {
        padding: 3rem clamp(1.5rem, 3vw, 3.5rem) 4rem;
        max-width: 1100px;
        margin: 0 auto;
        display: flex;
        flex-direction: column;
        gap: 2rem;
    }

    .hero {
        background: rgba(15, 23, 42, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 1.5rem;
        padding: clamp(1.5rem, 2vw, 2.5rem);
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 1.25rem;
        box-shadow: 0 30px 45px -25px rgba(2, 6, 23, 0.8);
        position: relative;
        overflow: hidden;
    }

    .hero::after {
        content: '';
        position: absolute;
        inset: 0;
        background: radial-gradient(circle, rgba(99, 102, 241, 0.25), transparent 60%);
        pointer-events: none;
    }

    .hero > div:first-child {
        position: relative;
        z-index: 1;
    }

    .filter-panel {
        position: relative;
        z-index: 1;
        border-radius: 1.25rem;
        background: rgba(15, 23, 42, 0.9);
        border: 1px solid rgba(148, 163, 184, 0.2);
        padding: 1.5rem;
        display: flex;
        flex-direction: column;
        gap: 1rem;
    }

    .filter-panel label {
        font-size: 0.9rem;
        color: #cbd5f5;
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
    }

    .filter-panel select,
    .filter-panel input {
        border-radius: 0.65rem;
        border: 1px solid rgba(148, 163, 184, 0.3);
        padding: 0.85rem 1rem;
        background: rgba(15, 23, 42, 0.6);
        color: #f8fafc;
        font-size: 1rem;
    }

    .filter-panel input::placeholder {
        color: rgba(148, 163, 184, 0.7);
    }

    .filter-panel button.primary {
        border: none;
        border-radius: 999px;
        padding: 0.9rem 1.25rem;
        font-size: 1rem;
        font-weight: 600;
        color: #fff;
        background: linear-gradient(135deg, #38bdf8, #818cf8);
        box-shadow: 0 10px 25px rgba(59, 130, 246, 0.35);
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .filter-panel button.primary:disabled {
        opacity: 0.6;
        cursor: not-allowed;
    }

    .filter-panel button.primary:not(:disabled):hover {
        transform: translateY(-1px);
        box-shadow: 0 12px 28px rgba(59, 130, 246, 0.45);
    }

    .filter-panel .status {
        font-size: 0.85rem;
        color: rgba(226, 232, 240, 0.65);
        margin: 0;
        min-height: 1.25rem;
    }

    .filter-panel .status::before {
        content: '';
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 0.5rem;
        background: currentColor;
        opacity: 0.8;
        vertical-align: middle;
    }

    .eyebrow {
        letter-spacing: 0.3em;
        text-transform: uppercase;
        font-size: 0.75rem;
        color: #a5b4fc;
        margin: 0 0 0.5rem;
    }

    h1 {
        margin: 0;
        font-size: clamp(2rem, 4vw, 2.8rem);
    }

    .lead {
        color: rgba(226, 232, 240, 0.84);
        line-height: 1.6;
        margin-top: 0.5rem;
    }

    .table-section {
        background: rgba(15, 23, 42, 0.8);
        border-radius: 1.5rem;
        border: 1px solid rgba(148, 163, 184, 0.15);
        padding: 2rem;
        box-shadow: 0 25px 40px rgba(2, 6, 23, 0.7);
    }

    .table-section header {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        gap: 1rem;
        flex-wrap: wrap;
        margin-bottom: 1.5rem;
    }

    h2 {
        margin: 0;
        font-size: 1.6rem;
    }

    .subtitle {
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.3em;
        color: #94a3b8;
        display: block;
    }

    .timestamp {
        font-size: 0.9rem;
        color: #cbd5f5;
    }
    @media (max-width: 640px) {
        .page {
            padding-top: 2rem;
        }

        .table-section {
            padding: 1.5rem;
        }
    }
</style>
