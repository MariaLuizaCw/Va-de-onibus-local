<!-- Purpose: apresenta a tela principal consumindo o store unificado do app. -->
<script lang="ts">
    import { onMount } from 'svelte';
    import RouteFilter from '$lib/components/RouteFilter.svelte';
    import ApiTable from '$lib/components/ApiTable.svelte';
    import LoginScreen from '$lib/components/LoginScreen.svelte';
    import LogoutButton from '$lib/components/LogoutButton.svelte';
    import StatsPanel from '$lib/components/StatsPanel.svelte';
    import { appStore, cities } from '$lib/stores/app.store';
    import { TOKEN_STORAGE_KEY, fetchJobStats, fetchJobTimeline, fetchJobsConfig } from '$lib/services/api';
    import type { JobStatsResponse, JobParentStats, JobTimelineEntry, JobChildStats, JobsConfig, JobConfig } from '$lib/types/api';
    import JobCard from '$lib/components/JobCard.svelte';
    import JobTimelineChart from '$lib/components/JobTimelineChart.svelte';
    import DatePicker from '$lib/components/DatePicker.svelte';

    const store = appStore;
    const { setCity, setLine, login, logout, fetchRoute, loadStats } = store;

    // Configuração de retenção de logs (do .env ou 90 dias por padrão)
    const JOB_LOG_RETENTION_DAYS = Number(import.meta.env.PUBLIC_JOB_LOG_RETENTION_DAYS) || 90;

    let currentView = 'jobs'; // 'jobs' ou 'routes'
    let token: string | null = null;
    let jobStats: JobStatsResponse | null = null;
    let availableDates: string[] = [];
    let selectedDate: string = new Date().toISOString().split('T')[0];
    let loading = false;
    let error: string | null = null;
    
    let selectedJob: JobParentStats | null = null;
    let selectedSubtask: JobChildStats | null = null; // Para filtrar timeline por subtask
    let timeline: JobTimelineEntry[] = [];
    let filteredTimeline: JobTimelineEntry[] = [];
    let timelineLoading = false;
    let jobsConfig: JobsConfig | null = null;

    // Filtrar timeline quando subtask é selecionada
    $: filteredTimeline = selectedSubtask 
        ? timeline.filter(t => t.jobName === selectedSubtask.jobName)
        : timeline;

    $: currentCityStats = $store.stats ? ($store.city === 'rio' ? $store.stats.rio : $store.stats.angra) : null;
    $: cityLabel = $store.city === 'rio' ? 'Rio de Janeiro' : 'Angra dos Reis';

    $: if ($store.authToken && !$store.stats && !$store.statsLoading) {
        loadStats();
    }

    // Gerar datas disponíveis baseado em JOB_LOG_RETENTION_DAYS
    function generateAvailableDates(): string[] {
        const dates: string[] = [];
        const today = new Date();
        for (let i = 0; i < JOB_LOG_RETENTION_DAYS; i++) {
            const date = new Date(today);
            date.setDate(today.getDate() - i);
            dates.push(date.toISOString().split('T')[0]);
        }
        return dates;
    }

    onMount(() => {
        token = localStorage.getItem(TOKEN_STORAGE_KEY);
        if (token) {
            availableDates = generateAvailableDates();
            loadJobStats();
            loadJobsConfig();
        }
    });

    async function loadJobStats() {
        if (!token) return;
        loading = true;
        error = null;
        try {
            // Garantir que selectedDate seja válido
            const dateToUse = selectedDate || new Date().toISOString().split('T')[0];
            jobStats = await fetchJobStats(token, dateToUse);
        } catch (err: any) {
            error = err.message || 'Erro ao carregar estatísticas';
        } finally {
            loading = false;
        }
    }

    async function loadJobsConfig() {
        try {
            jobsConfig = await fetchJobsConfig();
            console.log('Jobs config carregado:', jobsConfig);
        } catch (err) {
            console.error('Error loading jobs config:', err);
            jobsConfig = null;
        }
    }

    async function loadTimeline(job: JobParentStats) {
        if (!token) return;
        selectedJob = job;
        timelineLoading = true;
        try {
            // Garantir que selectedDate seja válido
            const dateToUse = selectedDate || new Date().toISOString().split('T')[0];
            timeline = await fetchJobTimeline(token, job.jobName, dateToUse, true);
        } catch (err) {
            console.error('Error loading timeline:', err);
            timeline = [];
        } finally {
            timelineLoading = false;
        }
    }

    function getJobConfig(jobName: string): JobConfig | null {
        if (!jobsConfig) {
            console.log('jobsConfig é null para job:', jobName);
            return null;
        }
        const config = jobsConfig.jobs.find(job => job.name === jobName) || null;
        console.log(`Config para ${jobName}:`, config);
        return config;
    }

    function handleDateChange(event: CustomEvent<string>) {
        const newDate = event.detail;
        // Validar formato da data antes de atribuir
        if (newDate && /^\d{4}-\d{2}-\d{2}$/.test(newDate)) {
            selectedDate = newDate;
        } else {
            console.warn('Data inválida recebida:', newDate);
            selectedDate = new Date().toISOString().split('T')[0]; // Fallback
        }
        selectedJob = null;
        selectedSubtask = null;
        timeline = [];
        loadJobStats();
    }

    function selectSubtask(child: JobChildStats | null) {
        selectedSubtask = child;
    }

    function formatDuration(ms: number): string {
        if (ms < 1000) return `${ms.toFixed(0)}ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
        return `${(ms / 60000).toFixed(2)}min`;
    }

    function toggleView() {
        currentView = currentView === 'jobs' ? 'routes' : 'jobs';
    }
</script>

<svelte:head>
    <title>Va-de-Onibus | Dashboard</title>
</svelte:head>

{#if !$store.authToken}
    <LoginScreen loginLoading={$store.loginLoading} loginError={$store.loginError} on:login={event => login(event.detail)} />
{:else}
    <main class="page">
        <header class="page-header">
            <div class="header-content">
                <p class="eyebrow">Va-de-Onibus</p>
                <h1>{currentView === 'jobs' ? 'Dashboard de Jobs' : 'Explorer de Rotas'}</h1>
            </div>
            <div class="header-actions">
                <button class="view-toggle" on:click={toggleView}>
                    {currentView === 'jobs' ? '🚌 Rotas' : '📊 Jobs'}
                </button>
                <LogoutButton on:logout={() => logout()} />
            </div>
        </header>

        {#if currentView === 'jobs'}
            <!-- Jobs Dashboard -->
            <div class="jobs-dashboard">
                {#if loading}
                    <div class="loading-state">
                        <div class="spinner"></div>
                        <p>Carregando estatísticas...</p>
                    </div>
                {:else if error}
                    <div class="error-state">
                        <p>{error}</p>
                        <button on:click={loadJobStats}>Tentar novamente</button>
                    </div>
                {:else if jobStats}
                    <section class="stats-overview">
                        <div class="overview-card">
                            <span class="label">Data</span>
                            <span class="value">{selectedDate ? new Date(selectedDate + 'T12:00:00').toLocaleDateString('pt-BR') : 'N/A'}</span>
                        </div>
                        <div class="overview-card">
                            <span class="label">Jobs Monitorados</span>
                            <span class="value">{jobStats.jobs.length}</span>
                        </div>
                        <div class="overview-card">
                            <span class="label">Total Execuções</span>
                            <span class="value">{jobStats.jobs.reduce((acc, j) => acc + j.executionCount, 0)}</span>
                        </div>
                    </section>

                    <div class="jobs-grid">
                        <div class="jobs-list">
                            <div class="date-filter">
                                <DatePicker 
                                    dates={availableDates} 
                                    selected={selectedDate} 
                                    on:change={handleDateChange} 
                                />
                            </div>
                            <div class="jobs-container">
                                {#each jobStats.jobs as job}
                                    <JobCard 
                                        {job} 
                                        {formatDuration}
                                        jobConfig={getJobConfig(job.jobName)}
                                        selected={selectedJob?.jobName === job.jobName}
                                        on:click={() => loadTimeline(job)}
                                    />
                                {/each}
                            </div>
                        </div>

                        <div class="job-detail">
                            {#if selectedJob}
                                <div class="detail-header">
                                    <h2>{selectedJob.jobName}</h2>
                                    <span class="status-badge {selectedJob.status}">{selectedJob.status}</span>
                                </div>

                                <div class="detail-stats">
                                    <div class="stat">
                                        <span class="label">Execuções</span>
                                        <span class="value">{selectedJob.executionCount}</span>
                                    </div>
                                    <div class="stat">
                                        <span class="label">Tempo Médio</span>
                                        <span class="value">{formatDuration(selectedJob.avgDurationMs)}</span>
                                    </div>
                                    <div class="stat">
                                        <span class="label">Desvio Padrão</span>
                                        <span class="value">{formatDuration(selectedJob.stddevDurationMs)}</span>
                                    </div>
                                    <div class="stat">
                                        <span class="label">Min / Max</span>
                                        <span class="value">{formatDuration(selectedJob.minDurationMs)} / {formatDuration(selectedJob.maxDurationMs)}</span>
                                    </div>
                                </div>

                                {#if selectedJob.children.length > 0}
                                    <div class="children-section">
                                        <h3>Subtasks <span class="subtask-hint">(clique para filtrar timeline)</span></h3>
                                        <div class="children-list">
                                            {#each selectedJob.children as child}
                                                <button 
                                                    class="child-card" 
                                                    class:selected={selectedSubtask?.jobName === child.jobName}
                                                    on:click={() => selectSubtask(selectedSubtask?.jobName === child.jobName ? null : child)}
                                                >
                                                    <div class="child-header">
                                                        <span class="child-name">{child.jobName}</span>
                                                        <span class="status-badge small {child.status}">{child.status}</span>
                                                    </div>
                                                    <div class="child-stats">
                                                        <span>{child.executionCount} exec</span>
                                                        <span>μ {formatDuration(child.avgDurationMs)}</span>
                                                        <span>σ {formatDuration(child.stddevDurationMs)}</span>
                                                    </div>
                                                </button>
                                            {/each}
                                        </div>
                                    </div>
                                {/if}

                                <div class="chart-section">
                                    <h3>
                                        Tempo de Execução ao Longo do Dia
                                        {#if selectedSubtask}
                                            <span class="filter-badge">Filtrado: {selectedSubtask.jobName}</span>
                                        {/if}
                                    </h3>
                                    {#if timelineLoading}
                                        <div class="chart-loading">
                                            <div class="spinner small"></div>
                                        </div>
                                    {:else}
                                        <JobTimelineChart timeline={filteredTimeline} {formatDuration} />
                                    {/if}
                                </div>
                            {:else}
                                <div class="no-selection">
                                    <p>Selecione um job para ver detalhes</p>
                                </div>
                            {/if}
                        </div>
                    </div>
                {/if}
            </div>
        {:else}
            <!-- Routes Explorer -->
            <div class="routes-dashboard">
                <StatsPanel stats={currentCityStats} loading={$store.statsLoading} cityLabel={cityLabel} />

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
                        selectedCity={$store.city}
                        linha={$store.linha}
                        loading={$store.loading}
                        statusMessage={$store.statusMessage}
                        errorMessage={$store.errorMessage}
                        on:submit={() => fetchRoute()}
                        on:citychange={event => setCity(event.detail)}
                        on:linechange={event => setLine(event.detail)}
                    />
                </section>

                <section class="table-section">
                    <header>
                        <div class="table-header">
                            <div>
                                <span class="subtitle">Resposta JSON</span>
                                <h2>{cityLabel} · Linha {$store.lastSearchedLine || 'n/a'}</h2>
                            </div>

                            {#if $store.lastFetched}
                                <span class="timestamp">Última atualização: {$store.lastFetched}</span>
                            {/if}
                        </div>

                        {#if $store.selectedLineStats}
                            <div class="line-stats-inline">
                                <div class="stat">
                                    <span class="label">Última atualização</span>
                                    <span class="value">
                                        {#if $store.selectedLineStats.lastUpdate}
                                            {new Date($store.selectedLineStats.lastUpdate).toLocaleString('pt-BR')}
                                        {:else}
                                            —
                                        {/if}
                                    </span>
                                </div>
                                <div class="stat">
                                    <span class="label">Status</span>
                                    <span class="value status" class:active={$store.selectedLineStats.isActive} class:inactive={!$store.selectedLineStats.isActive}>
                                        {$store.selectedLineStats.isActive ? 'Ativa' : 'Inativa'}
                                    </span>
                                </div>
                                <div class="stat">
                                    <span class="label">Ordens ativas</span>
                                    <span class="value">{$store.selectedLineStats.activeOrders} / {$store.selectedLineStats.totalOrders}</span>
                                </div>
                            </div>
                        {/if}
                    </header>

                    <ApiTable records={$store.tableData} preferredSortFields={$store.preferredSortFields} />
                </section>
            </div>
        {/if}
    </main>
{/if}

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
        padding: 2rem clamp(1rem, 3vw, 2.5rem);
        max-width: 1600px;
        margin: 0 auto;
        min-height: 100vh;
        display: flex;
        flex-direction: column;
        gap: 1.5rem;
    }

    .page-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1.5rem;
        margin-bottom: 1rem;
        flex-wrap: wrap;
    }

    .header-content {
        flex: 1;
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
        font-size: clamp(1.75rem, 3vw, 2.5rem);
        color: #f8fafc;
    }

    .header-actions {
        display: flex;
        gap: 0.75rem;
        align-items: center;
    }

    .view-toggle {
        background: rgba(99, 102, 241, 0.2);
        border: 1px solid rgba(99, 102, 241, 0.4);
        color: #a5b4fc;
        padding: 0.5rem 1rem;
        border-radius: 0.5rem;
        font-size: 0.875rem;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.2s;
    }

    .view-toggle:hover {
        background: rgba(99, 102, 241, 0.3);
        color: #c7d2fe;
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
        background: rgba(15, 23, 42, 0.85);
        border-radius: 1.25rem;
        border: 1px solid rgba(148, 163, 184, 0.2);
        padding: 1.5rem;
        box-shadow: 0 20px 35px rgba(2, 6, 23, 0.7);
    }

    .table-section header {
        display: flex;
        flex-direction: column;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }

    .table-header {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        gap: 1rem;
        flex-wrap: wrap;
    }

    .line-stats-inline {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 1rem;
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 0.9rem;
        padding: 1rem;
    }

    .line-stats-inline .stat {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        min-width: 0;
    }

    .line-stats-inline .label {
        font-size: 0.75rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: rgba(226, 232, 240, 0.65);
    }

    .line-stats-inline .value {
        font-size: 0.95rem;
        color: #f8fafc;
        font-weight: 600;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }

    .line-stats-inline .status.active {
        color: #34d399;
    }

    .line-stats-inline .status.inactive {
        color: #f87171;
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

    @media (max-width: 768px) {
        .hero {
            grid-template-columns: 1fr;
        }

        .line-stats-inline {
            grid-template-columns: 1fr;
        }
    }

    @media (max-width: 640px) {
        .page {
            padding-top: 2rem;
        }

        .table-section {
            padding: 1.5rem;
        }
    }

    /* Jobs Dashboard Styles */
    .jobs-dashboard {
        display: flex;
        flex-direction: column;
        gap: 1.5rem;
    }

    .loading-state, .error-state, .no-selection {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 4rem 2rem;
        background: rgba(15, 23, 42, 0.6);
        border-radius: 1rem;
        border: 1px solid rgba(148, 163, 184, 0.1);
    }

    .spinner {
        width: 40px;
        height: 40px;
        border: 3px solid rgba(99, 102, 241, 0.2);
        border-top-color: #6366f1;
        border-radius: 50%;
        animation: spin 1s linear infinite;
    }

    .spinner.small {
        width: 24px;
        height: 24px;
        border-width: 2px;
    }

    @keyframes spin {
        to { transform: rotate(360deg); }
    }

    .stats-overview {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 1rem;
    }

    .overview-card {
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 0.75rem;
        padding: 1.25rem;
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .overview-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #94a3b8;
    }

    .overview-card .value {
        font-size: 1.5rem;
        font-weight: 600;
        color: #f8fafc;
    }

    .jobs-grid {
        display: grid;
        grid-template-columns: 1fr 2fr;
        gap: 1.5rem;
    }

    @media (max-width: 1024px) {
        .jobs-grid {
            grid-template-columns: 1fr;
        }
    }

    .jobs-list {
        display: flex;
        flex-direction: column;
        gap: 1rem;
    }

    .date-filter {
        background: rgba(30, 41, 59, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 0.5rem;
        padding: 1rem;
    }

    .jobs-container {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
    }

    .job-detail {
        background: rgba(15, 23, 42, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 1rem;
        padding: 1.5rem;
        min-height: 500px;
    }

    .detail-header {
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }

    .detail-header h2 {
        margin: 0;
        font-size: 1.25rem;
        color: #f8fafc;
    }

    .status-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }

    .status-badge.success {
        background: rgba(34, 197, 94, 0.2);
        color: #4ade80;
    }

    .status-badge.error {
        background: rgba(239, 68, 68, 0.2);
        color: #f87171;
    }

    .status-badge.mixed {
        background: rgba(251, 191, 36, 0.2);
        color: #fbbf24;
    }

    .status-badge.small {
        padding: 0.125rem 0.5rem;
        font-size: 0.625rem;
    }

    .detail-stats {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }

    .stat {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }

    .stat .label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #64748b;
    }

    .stat .value {
        font-size: 1rem;
        font-weight: 600;
        color: #e2e8f0;
    }

    .children-section {
        margin-bottom: 1.5rem;
    }

    .children-section h3 {
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #94a3b8;
        margin: 0 0 0.75rem;
    }

    .children-list {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .child-card {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 0.5rem;
        padding: 0.75rem 1rem;
        cursor: pointer;
        transition: all 0.2s;
        text-align: left;
        width: 100%;
    }

    .child-card:hover {
        background: rgba(51, 65, 85, 0.5);
        border-color: rgba(148, 163, 184, 0.2);
    }

    .child-card.selected {
        background: rgba(99, 102, 241, 0.15);
        border-color: rgba(99, 102, 241, 0.5);
    }

    .subtask-hint {
        font-size: 0.65rem;
        font-weight: 400;
        color: #64748b;
        text-transform: none;
        letter-spacing: normal;
    }

    .filter-badge {
        font-size: 0.65rem;
        font-weight: 500;
        background: rgba(99, 102, 241, 0.2);
        color: #a5b4fc;
        padding: 0.125rem 0.5rem;
        border-radius: 9999px;
        margin-left: 0.5rem;
        text-transform: none;
        letter-spacing: normal;
    }

    .child-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 0.5rem;
    }

    .child-name {
        font-weight: 500;
        color: #e2e8f0;
        font-size: 0.875rem;
    }

    .child-stats {
        display: flex;
        gap: 1rem;
        font-size: 0.75rem;
        color: #94a3b8;
    }

    .chart-section {
        margin-top: 1.5rem;
    }

    .chart-section h3 {
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #94a3b8;
        margin: 0 0 1rem;
    }

    .chart-loading {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 200px;
    }

    .error-state button {
        margin-top: 1rem;
        background: #6366f1;
        color: white;
        border: none;
        padding: 0.5rem 1.5rem;
        border-radius: 0.5rem;
        cursor: pointer;
    }

    .no-selection p {
        color: #64748b;
    }

    /* Routes Dashboard Styles */
    .routes-dashboard {
        display: flex;
        flex-direction: column;
        gap: 1.5rem;
    }
</style>
