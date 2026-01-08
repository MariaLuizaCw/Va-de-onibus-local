-- =============================================================================
-- partitions.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo partitions

-- -----------------------------------------------------------------------------
-- fn_list_partition_tables
-- Lista tabelas de partição por prefixo
-- Usado por: partitions.js -> cleanupOldPartitionsForTable
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_list_partition_tables(p_table_prefix text)
RETURNS TABLE (tablename text)
LANGUAGE sql
STABLE
AS $$
    SELECT tablename::text
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename LIKE p_table_prefix || '_%';
$$;

-- =============================================================================
-- QUERIES MANTIDAS INLINE
-- =============================================================================
-- As seguintes queries usam DDL dinâmico e são mantidas inline no backend:
--
-- partitions.js -> createPartitionForDate
--   DDL dinâmico para CREATE TABLE de partições
--
-- partitions.js -> cleanupOldPartitionsForTable
--   DDL dinâmico para DROP TABLE de partições
-- =============================================================================

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
