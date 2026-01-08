# SQL Functions - GPS Backend

Este diretório contém todas as functions SQL utilizadas pelo backend, organizadas por módulo.

## Estrutura dos Arquivos

- `00_master.sql` - Script master que importa todos os módulos
- `itinerarioStore.sql` - Functions para cache de itinerários
- `snapshots.sql` - Functions para gerenciamento de snapshots
- `partitions.sql` - Functions para gerenciamento de partições
- `reports.sql` - Functions para geração de relatórios
- `rio.sql` - Functions para processamento de dados do Rio
- `angra.sql` - Functions para processamento de dados de Angra

## Como Executar

### Executar Todas as Functions
```bash
psql -U usuario -d database -f backend/sql/00_master.sql
```

### Executar Apenas um Módulo Específico
```bash
psql -U usuario -d database -f backend/sql/rio.sql
psql -U usuario -d database -f backend/sql/reports.sql
```

## Functions Disponíveis

### itinerarioStore.sql
- `fn_get_itinerarios_habilitados()` - Retorna itinerários habilitados

### snapshots.sql
- `fn_load_onibus_snapshot(city)` - Carrega snapshot por cidade
- `fn_save_onibus_snapshot(city, data)` - Salva snapshot (delete + insert)

### partitions.sql
- `fn_list_partition_tables(prefix)` - Lista tabelas de partição

### reports.sql
- `fn_generate_sentido_coverage_report(date, timezone, distance)` - Gera relatório de cobertura Rio
- `fn_generate_angra_route_type_report(date, timezone)` - Gera relatório route_type Angra

### rio.sql
- `fn_enrich_gps_batch_with_sentido_json(points, distance)` - Calcula sentido para batch de pontos
- `fn_upsert_gps_onibus_estado_batch_json(points, visit_dist, proximity_dist)` - Atualiza estado dos ônibus
- `fn_insert_gps_posicoes_rio_batch_json(records)` - Insere registros GPS do Rio em batch
- `fn_insert_gps_sentido_rio_batch_json(records)` - Insere registros GPS com sentido do Rio em batch

### angra.sql
- `fn_insert_gps_posicoes_angra_batch_json(records)` - Insere registros GPS de Angra em batch
- `fn_insert_gps_sentido_angra_batch_json(records)` - Insere registros GPS com sentido de Angra em batch

## Queries Mantidas Inline

As seguintes queries usam DDL dinâmico e permanecem no backend:

- **DDL dinâmico**: `createPartitionForDate`, `cleanupOldPartitionsForTable`

## JSON Arrays para Batch Processing

Todas as functions de batch usam JSON arrays. O backend constrói os arrays:

```javascript
const recordsJson = batch.map(record => ({
    ordem: record.ordem,
    latitude: Number(record.latitude),
    longitude: Number(record.longitude),
    // ... outros campos
}));

await dbPool.query(
    'SELECT fn_insert_gps_posicoes_rio_batch_json($1::jsonb)',
    [JSON.stringify(recordsJson)]
);
```

No PostgreSQL, `jsonb_array_elements` converte o array em rows virtuais.
