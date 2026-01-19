<script lang="ts">
    import { createEventDispatcher } from 'svelte';

    export let dates: string[] = [];
    export let selected: string;

    const dispatch = createEventDispatcher();

    function handleChange(event: Event) {
        const target = event.target as HTMLSelectElement;
        dispatch('change', target.value);
    }

    function formatDate(dateStr: string): string {
        const date = new Date(dateStr + 'T12:00:00');
        return date.toLocaleDateString('pt-BR', { 
            weekday: 'short', 
            day: '2-digit', 
            month: 'short' 
        });
    }
</script>

<div class="date-picker">
    <label for="date-select">Data:</label>
    <select id="date-select" value={selected} on:change={handleChange}>
        {#each dates as date}
            <option value={date}>{formatDate(date)}</option>
        {/each}
        {#if dates.length === 0}
            <option value={selected}>{formatDate(selected)}</option>
        {/if}
    </select>
</div>

<style>
    .date-picker {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        background: rgba(30, 41, 59, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 0.5rem;
        padding: 0.5rem 1rem;
    }

    label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #94a3b8;
    }

    select {
        background: transparent;
        border: none;
        color: #f8fafc;
        font-size: 0.875rem;
        font-weight: 500;
        cursor: pointer;
        padding: 0.25rem;
    }

    select:focus {
        outline: none;
    }

    select option {
        background: #1e293b;
        color: #f8fafc;
    }
</style>
