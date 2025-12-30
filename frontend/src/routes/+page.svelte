<!-- Purpose: apresenta a tela principal consumindo o store unificado do app. -->
<script lang="ts">
    import RouteFilter from '$lib/components/RouteFilter.svelte';
    import ApiTable from '$lib/components/ApiTable.svelte';
    import LoginScreen from '$lib/components/LoginScreen.svelte';
    import LogoutButton from '$lib/components/LogoutButton.svelte';
    import { appStore, cities } from '$lib/stores/app.store';

    const store = appStore;
    const { setCity, setLine, login, logout, fetchRoute } = store;
</script>

<svelte:head>
    <title>API Explorer | Va-de-Onibus</title>
</svelte:head>

{#if !$store.authToken}
    <LoginScreen loginLoading={$store.loginLoading} loginError={$store.loginError} on:login={event => login(event.detail)} />
{:else}
    <main class="page">
        <div class="hero-logout">
            <LogoutButton on:logout={() => logout()} />
        </div>
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
                <div>
                    <span class="subtitle">Resposta JSON</span>
                    <h2>{$store.city === 'rio' ? 'Rio de Janeiro' : 'Angra dos Reis'} · Linha {$store.linha || 'n/a'}</h2>
                </div>
                {#if $store.lastFetched}
                    <span class="timestamp">Última atualização: {$store.lastFetched}</span>
                {/if}
            </header>

            <ApiTable records={$store.tableData} preferredSortFields={$store.preferredSortFields} />
        </section>
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
        padding: 3rem clamp(1.5rem, 3vw, 3.5rem) 4rem;
        max-width: 1100px;
        margin: 0 auto;
        display: flex;
        flex-direction: column;
        gap: 2rem;
    }

    .hero-logout {
        position: absolute;
        top: 2rem;
        right: 2rem;
        z-index: 10;
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

    @media (max-width: 768px) {
        .hero {
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
</style>
