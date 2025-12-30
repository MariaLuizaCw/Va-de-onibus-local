<script lang="ts">
    import type { ApiRecord } from '$lib/types/api';

    const preferredColumnOrder = [
        'ordem',
        'linha',
        'LineNumber',
        'lineNumber',
        'linha',
        'Speed',
        'Shift',
        'Latitude',
        'Longitude',
        'velocidade',
        'sentido',
        'distancia_metros',
        'RouteType',
        'datahora',
        'datahoraenvio',
        'datahoraservidor',
        'EventDate',
        'EventName',
        'RealDepartureDate',
        'RealArrivalDate',
        'EstimatedDepartureDate',
        'EstimatedArrivalDate',
        'UpdateDate',
        'LicensePlate',
        'LineDescription',
        'RouteDescription',
        'VehicleDescription',
        'LineIntegrationCode',
        'RouteIntegrationCode',
        'VehicleIntegrationCode',
        'ClientBusIntegrationCode',
        'RouteDirection',
        'IsGarage',
        'IsRouteStartPoint',
        'IsRouteEndPoint'
    ];

    const formatCell = (value: unknown) => {
        if (value == null) return '—';
        if (typeof value === 'object') {
            try {
                return JSON.stringify(value);
            } catch {
                return String(value);
            }
        }
        return String(value);
    };

    const normalizeValue = (value: unknown, key?: string) => {
        if (value == null) return '';
        if (typeof value === 'number') return value;
        if (typeof value === 'string') {
            const numeric = Number(value);
            if (!Number.isNaN(numeric)) return numeric;
            const normalizedKey = (key ?? value).toLowerCase();
            if (normalizedKey.includes('event') || normalizedKey.includes('date')) {
                const parsed = Date.parse(value);
                if (!Number.isNaN(parsed)) return parsed;
            }
            return value.toLowerCase();
        }
        return JSON.stringify(value);
    };

    export let records: ApiRecord[] = [];
    export let pageSize = 8;
    export let preferredSortFields: string[] = [];

    let sortField: string | null = null;
    let sortDirection: 'asc' | 'desc' = 'asc';
    let currentPage = 1;
    let lastAppliedDefaultField: string | null = null;

    $: tableColumns = (() => {
        const set = new Set<string>();
        for (const record of records) {
            Object.keys(record).forEach(key => set.add(key));
        }
        const columns = Array.from(set);
        columns.sort((a, b) => {
            const aIndex = preferredColumnOrder.indexOf(a);
            const bIndex = preferredColumnOrder.indexOf(b);
            if (aIndex !== -1 || bIndex !== -1) {
                if (aIndex === -1) return 1;
                if (bIndex === -1) return -1;
                return aIndex - bIndex;
            }
            return a.localeCompare(b);
        });
        return columns;
    })();

    $: if (!sortField && tableColumns.length) {
        sortField = tableColumns[0];
    }

    $: resolvedDefaultSortField = preferredSortFields.find(field => tableColumns.includes(field));
    $: if (resolvedDefaultSortField && resolvedDefaultSortField !== lastAppliedDefaultField) {
        sortField = resolvedDefaultSortField;
        sortDirection = 'desc';
        currentPage = 1;
        lastAppliedDefaultField = resolvedDefaultSortField;
    } else if (!resolvedDefaultSortField && !sortField && tableColumns.length) {
        sortField = tableColumns[0];
    }

    $: sortedData = [...records].sort((a, b) => {
        if (!sortField) return 0;
        const aVal = normalizeValue(a[sortField]);
        const bVal = normalizeValue(b[sortField]);

        if (aVal === bVal) return 0;
        if (sortDirection === 'asc') {
            return aVal > bVal ? 1 : -1;
        }
        return aVal < bVal ? 1 : -1;
    });

    $: totalPages = Math.max(1, Math.ceil(sortedData.length / pageSize));
    $: if (currentPage > totalPages) currentPage = totalPages;
    $: pageStart = (currentPage - 1) * pageSize;
    $: paginatedData = sortedData.slice(pageStart, pageStart + pageSize);

    function handleSort(column: string) {
        if (sortField === column) {
            sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            sortField = column;
            sortDirection = 'asc';
        }
        currentPage = 1;
    }

    function goToPage(page: number) {
        if (page >= 1 && page <= totalPages) {
            currentPage = page;
        }
    }
</script>

{#if records.length === 0}
    <div class="empty-state">
        <p>Faça uma busca para preencher a tabela.</p>
    </div>
{:else}
    <div class="table-wrapper">
        <table>
            <thead>
                <tr>
                    {#each tableColumns as column}
                        <th>
                            <button type="button" class:active={sortField === column} on:click={() => handleSort(column)}>
                                {column}
                                {#if sortField === column}
                                    <span aria-hidden="true">{sortDirection === 'asc' ? '▲' : '▼'}</span>
                                {/if}
                            </button>
                        </th>
                    {/each}
                </tr>
            </thead>
            <tbody>
                {#each paginatedData as record, index}
                    <tr class={index % 2 === 0 ? 'even' : 'odd'}>
                        {#each tableColumns as column}
                            <td>{formatCell(record[column], column)}</td>
                        {/each}
                    </tr>
                {/each}
            </tbody>
        </table>
    </div>

    <div class="pagination">
        <button type="button" on:click={() => goToPage(currentPage - 1)} disabled={currentPage === 1}>
            Anterior
        </button>
        <span>Página {currentPage} de {totalPages} · {sortedData.length} registros</span>
        <button type="button" on:click={() => goToPage(currentPage + 1)} disabled={currentPage === totalPages}>
            Próxima
        </button>
    </div>
{/if}

<style>
    .table-wrapper {
        overflow-x: auto;
    }

    table {
        width: 100%;
        border-collapse: collapse;
        min-width: 700px;
        font-size: 0.95rem;
    }

    th,
    td {
        padding: 0.85rem 0.75rem;
        text-align: left;
    }

    th {
        padding: 0;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.2em;
        color: #94a3b8;
        border-bottom: 1px solid rgba(226, 232, 240, 0.15);
    }

    th button {
        width: 100%;
        background: transparent;
        color: inherit;
        border: none;
        font-size: 0.75rem;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.75rem;
        cursor: pointer;
    }

    th button span {
        font-size: 0.65rem;
        margin-left: 0.35rem;
    }

    th button.active {
        color: #38bdf8;
    }

    td {
        border-bottom: 1px solid rgba(148, 163, 184, 0.15);
    }

    tr.even td {
        background: rgba(59, 130, 246, 0.04);
    }

    .empty-state {
        padding: 3rem;
        text-align: center;
        color: rgba(148, 163, 184, 0.8);
        border: 1px dashed rgba(148, 163, 184, 0.4);
        border-radius: 1rem;
        background: rgba(15, 23, 42, 0.6);
    }

    .pagination {
        margin-top: 1.25rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-size: 0.9rem;
        color: #cbd5f5;
        flex-wrap: wrap;
        gap: 0.75rem;
    }

    .pagination button {
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(59, 130, 246, 0.15);
        color: #fff;
        border-radius: 999px;
        padding: 0.35rem 1rem;
        cursor: pointer;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }

    .pagination button:disabled {
        opacity: 0.4;
        cursor: not-allowed;
    }

    .pagination button:not(:disabled):hover {
        transform: translateY(-2px);
        border-color: rgba(59, 130, 246, 0.6);
    }
</style>
