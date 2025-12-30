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
