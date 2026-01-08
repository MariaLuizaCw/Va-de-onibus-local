-- =============================================================================
-- GPS Backend - Master SQL Script
-- =============================================================================
-- Execute este script para criar/atualizar TODAS as functions do backend.
-- Este script importa todos os arquivos SQL individuais.
-- =============================================================================

-- Importar functions do itinerarioStore
\i backend/sql/itinerarioStore.sql

-- Importar functions do snapshots
\i backend/sql/snapshots.sql

-- Importar functions do partitions
\i backend/sql/partitions.sql

-- Importar functions do reports
\i backend/sql/reports.sql

-- Importar functions do rio
\i backend/sql/rio.sql

-- Importar functions do angra
\i backend/sql/angra.sql

-- =============================================================================
-- EXECUÇÃO INDIVIDUAL
-- =============================================================================
-- Para executar apenas um módulo específico:
--   psql -U usuario -d database -f backend/sql/nome_do_arquivo.sql
--
-- Para executar todos os módulos:
--   psql -U usuario -d database -f backend/sql/00_master.sql
-- =============================================================================

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
