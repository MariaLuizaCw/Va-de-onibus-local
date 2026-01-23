<script lang="ts">
    import { createEventDispatcher } from 'svelte';
    import type { JobParentStats, JobConfig } from '$lib/types/api';

    export let job: JobParentStats;
    export let formatDuration: (ms: number) => string;
    export let selected = false;
    export let jobConfig: JobConfig | null = null;

    const dispatch = createEventDispatcher();

    function handleClick() {
        dispatch('click');
    }
</script>

<button class="job-card" class:selected on:click={handleClick}>
    <div class="card-header">
        <div class="job-name-section">
            <span class="job-name">{job.jobName}</span>
            {#if jobConfig?.description}
                <span class="job-description">{jobConfig.description}</span>
            {/if}
        </div>
        <span class="status-badge {job.status}">{job.status}</span>
    </div>
    <div class="card-stats">
        <div class="stat">
            <span class="value">{job.executionCount}</span>
            <span class="label">exec</span>
        </div>
        <div class="stat">
            <span class="value">{formatDuration(job.avgDurationMs)}</span>
            <span class="label">média</span>
        </div>
        <div class="stat">
            <span class="value">{formatDuration(job.stddevDurationMs)}</span>
            <span class="label">σ</span>
        </div>
        {#if job.children.length > 0}
            <div class="stat children-count">
                <span class="value">{job.children.length}</span>
                <span class="label">subtasks</span>
            </div>
        {/if}
    </div>
</button>

<style>
    .job-card {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 0.75rem;
        padding: 1rem;
        cursor: pointer;
        transition: all 0.2s;
        text-align: left;
        width: 100%;
    }

    .job-card:hover {
        background: rgba(51, 65, 85, 0.6);
        border-color: rgba(148, 163, 184, 0.25);
    }

    .job-card.selected {
        background: rgba(99, 102, 241, 0.15);
        border-color: rgba(99, 102, 241, 0.5);
    }

    .card-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 0.5rem;
    }

    .job-name-section {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        flex: 1;
    }

    .job-name {
        font-weight: 600;
        color: #f8fafc;
        font-size: 0.9rem;
    }

    .job-description {
        font-size: 0.75rem;
        color: #94a3b8;
        line-height: 1.3;
        font-weight: 400;
    }

    .status-badge {
        padding: 0.125rem 0.5rem;
        border-radius: 9999px;
        font-size: 0.625rem;
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

    .card-stats {
        display: flex;
        gap: 1rem;
        flex-wrap: wrap;
    }

    .stat {
        display: flex;
        flex-direction: column;
        gap: 0.125rem;
    }

    .stat .value {
        font-size: 0.875rem;
        font-weight: 600;
        color: #e2e8f0;
    }

    .stat .label {
        font-size: 0.625rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748b;
    }

    .children-count {
        margin-left: auto;
    }

    .children-count .value {
        color: #a5b4fc;
    }
</style>
