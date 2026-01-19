<script lang="ts">
    import { onMount, afterUpdate } from 'svelte';
    import type { JobTimelineEntry } from '$lib/types/api';

    export let timeline: JobTimelineEntry[] = [];
    export let formatDuration: (ms: number) => string;

    let canvas: HTMLCanvasElement;
    let ctx: CanvasRenderingContext2D | null = null;
    let containerWidth = 800;

    const PADDING = { top: 40, right: 20, bottom: 60, left: 70 };
    const HEIGHT = 300;

    onMount(() => {
        ctx = canvas.getContext('2d');
        drawChart();
    });

    afterUpdate(() => {
        drawChart();
    });

    function drawChart() {
        if (!ctx || !canvas || timeline.length === 0) return;

        const width = containerWidth;
        canvas.width = width * window.devicePixelRatio;
        canvas.height = HEIGHT * window.devicePixelRatio;
        canvas.style.width = `${width}px`;
        canvas.style.height = `${HEIGHT}px`;
        ctx.scale(window.devicePixelRatio, window.devicePixelRatio);

        const chartWidth = width - PADDING.left - PADDING.right;
        const chartHeight = HEIGHT - PADDING.top - PADDING.bottom;

        // Clear canvas
        ctx.fillStyle = 'rgba(15, 23, 42, 0.5)';
        ctx.fillRect(0, 0, width, HEIGHT);

        // Get data bounds
        const times = timeline.map(t => new Date(t.startedAt).getTime());
        const durations = timeline.map(t => t.durationMs);
        const minTime = Math.min(...times);
        const maxTime = Math.max(...times);
        const maxDuration = Math.max(...durations) * 1.1;

        // Draw grid
        ctx.strokeStyle = 'rgba(148, 163, 184, 0.1)';
        ctx.lineWidth = 1;

        // Horizontal grid lines
        const ySteps = 5;
        for (let i = 0; i <= ySteps; i++) {
            const y = PADDING.top + (chartHeight / ySteps) * i;
            ctx.beginPath();
            ctx.moveTo(PADDING.left, y);
            ctx.lineTo(width - PADDING.right, y);
            ctx.stroke();

            // Y-axis labels
            const value = maxDuration - (maxDuration / ySteps) * i;
            ctx.fillStyle = '#64748b';
            ctx.font = '11px Inter, sans-serif';
            ctx.textAlign = 'right';
            ctx.fillText(formatDuration(value), PADDING.left - 10, y + 4);
        }

        // X-axis labels (time)
        const xSteps = Math.min(6, timeline.length);
        ctx.textAlign = 'center';
        for (let i = 0; i <= xSteps; i++) {
            const x = PADDING.left + (chartWidth / xSteps) * i;
            const time = minTime + ((maxTime - minTime) / xSteps) * i;
            const date = new Date(time);
            ctx.fillStyle = '#64748b';
            ctx.fillText(date.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }), x, HEIGHT - PADDING.bottom + 20);
        }

        // Group by job type for different colors
        const parentEntries = timeline.filter(t => !t.subtask);
        const childEntries = timeline.filter(t => t.subtask);

        // Draw child entries (subtasks) - smaller dots
        ctx.fillStyle = 'rgba(167, 139, 250, 0.6)';
        for (const entry of childEntries) {
            const x = PADDING.left + ((new Date(entry.startedAt).getTime() - minTime) / (maxTime - minTime || 1)) * chartWidth;
            const y = PADDING.top + chartHeight - (entry.durationMs / maxDuration) * chartHeight;
            
            ctx.beginPath();
            ctx.arc(x, y, 3, 0, Math.PI * 2);
            ctx.fill();
        }

        // Draw parent entries - larger dots with line
        if (parentEntries.length > 1) {
            ctx.strokeStyle = 'rgba(99, 102, 241, 0.8)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            
            for (let i = 0; i < parentEntries.length; i++) {
                const entry = parentEntries[i];
                const x = PADDING.left + ((new Date(entry.startedAt).getTime() - minTime) / (maxTime - minTime || 1)) * chartWidth;
                const y = PADDING.top + chartHeight - (entry.durationMs / maxDuration) * chartHeight;
                
                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            }
            ctx.stroke();
        }

        // Draw parent entry dots
        for (const entry of parentEntries) {
            const x = PADDING.left + ((new Date(entry.startedAt).getTime() - minTime) / (maxTime - minTime || 1)) * chartWidth;
            const y = PADDING.top + chartHeight - (entry.durationMs / maxDuration) * chartHeight;
            
            ctx.fillStyle = entry.status === 'success' ? '#4ade80' : '#f87171';
            ctx.beginPath();
            ctx.arc(x, y, 5, 0, Math.PI * 2);
            ctx.fill();
        }

        // Legend
        ctx.font = '11px Inter, sans-serif';
        ctx.textAlign = 'left';
        
        // Parent job legend
        ctx.fillStyle = '#4ade80';
        ctx.beginPath();
        ctx.arc(PADDING.left + 10, 20, 5, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = '#94a3b8';
        ctx.fillText('Job Principal', PADDING.left + 22, 24);

        // Subtask legend
        ctx.fillStyle = 'rgba(167, 139, 250, 0.8)';
        ctx.beginPath();
        ctx.arc(PADDING.left + 120, 20, 3, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = '#94a3b8';
        ctx.fillText('Subtasks', PADDING.left + 130, 24);

        // Axis labels
        ctx.fillStyle = '#64748b';
        ctx.font = '12px Inter, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('Horário', width / 2, HEIGHT - 10);
        
        ctx.save();
        ctx.translate(15, HEIGHT / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.fillText('Duração', 0, 0);
        ctx.restore();
    }

    function handleResize(entries: ResizeObserverEntry[]) {
        containerWidth = entries[0].contentRect.width;
        drawChart();
    }
</script>

<div class="chart-container" use:resizeObserver={handleResize}>
    {#if timeline.length === 0}
        <div class="no-data">
            <p>Sem dados de execução para este período</p>
        </div>
    {:else}
        <canvas bind:this={canvas}></canvas>
        <div class="chart-summary">
            <span>{timeline.length} execuções</span>
            <span>•</span>
            <span>{timeline.filter(t => t.status === 'success').length} sucesso</span>
            <span>•</span>
            <span>{timeline.filter(t => t.status === 'error').length} erro</span>
        </div>
    {/if}
</div>

<svelte:options accessors />

<script context="module" lang="ts">
    function resizeObserver(node: HTMLElement, callback: (entries: ResizeObserverEntry[]) => void) {
        const observer = new ResizeObserver(callback);
        observer.observe(node);
        return {
            destroy() {
                observer.disconnect();
            }
        };
    }
</script>

<style>
    .chart-container {
        background: rgba(15, 23, 42, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 0.75rem;
        padding: 1rem;
        min-height: 300px;
    }

    canvas {
        display: block;
        width: 100%;
    }

    .no-data {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 200px;
        color: #64748b;
    }

    .chart-summary {
        display: flex;
        gap: 0.75rem;
        justify-content: center;
        margin-top: 1rem;
        font-size: 0.75rem;
        color: #94a3b8;
    }
</style>
