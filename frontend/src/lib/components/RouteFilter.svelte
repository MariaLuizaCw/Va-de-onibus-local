<script lang="ts">
    import { createEventDispatcher } from 'svelte';

    export type CityOption = { id: string; label: string };

    export let cities: CityOption[] = [];
    export let selectedCity = 'rio';
    export let linha = '';
    export let loading = false;
    export let statusMessage = '';
    export let errorMessage: string | null = null;

    const dispatch = createEventDispatcher<{
        submit: void;
        citychange: string;
        linechange: string;
    }>();

    function handleSubmit(event: Event) {
        event.preventDefault();
        dispatch('submit');
    }

    function handleCityChange(event: Event) {
        const target = event.currentTarget as HTMLSelectElement;
        dispatch('citychange', target.value);
    }

    function handleLineChange(event: Event) {
        const target = event.currentTarget as HTMLInputElement;
        dispatch('linechange', target.value);
    }
</script>

<form class="filter-panel" on:submit={handleSubmit}>
    <label>
        Cidade
        <select bind:value={selectedCity} on:change={handleCityChange}>
            {#each cities as option}
                <option value={option.id}>{option.label}</option>
            {/each}
        </select>
    </label>

    <label>
        Linha
        <input
            type="text"
            placeholder="Ex: 1500"
            bind:value={linha}
            autocomplete="off"
            on:input={handleLineChange}
        />
    </label>

    <button type="submit" class="primary" disabled={loading}>
        {#if loading}
            Consultando...
        {:else}
            Buscar rota
        {/if}
    </button>

    <p class="status">{errorMessage ?? statusMessage}</p>
</form>

<style>
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
</style>
