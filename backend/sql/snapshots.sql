-- =============================================================================
-- snapshots.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo snapshots

-- -----------------------------------------------------------------------------
-- fn_load_onibus_snapshot
-- Carrega snapshot de ônibus por cidade
-- Usado por: snapshots.js -> loadOnibusSnapshot
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_load_onibus_snapshot(p_city text)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT data
    FROM public.onibus_snapshots
    WHERE city = p_city
    LIMIT 1;
$$;

-- -----------------------------------------------------------------------------
-- fn_save_onibus_snapshot
-- Salva snapshot de ônibus (delete + insert)
-- Usado por: snapshots.js -> saveOnibusSnapshot
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_save_onibus_snapshot(p_city text, p_data jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM public.onibus_snapshots WHERE city = p_city;
    INSERT INTO public.onibus_snapshots (city, data) VALUES (p_city, p_data);
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
