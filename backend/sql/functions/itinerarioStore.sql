-- =============================================================================
-- itinerarioStore.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo itinerarioStore

-- -----------------------------------------------------------------------------
-- fn_get_itinerarios_habilitados
-- Retorna todos os itinerários habilitados para cache em memória
-- Usado por: itinerarioStore.js -> loadItinerarioIntoMemory
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_itinerarios_habilitados()
RETURNS TABLE (
    numero_linha text,
    sentido text,
    itinerario_id integer,
    route_name text,
    geom json
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        numero_linha::text,
        sentido::text,
        id AS itinerario_id,
        route_name,
        ST_AsGeoJSON(the_geom)::json AS geom
    FROM public.itinerario
    WHERE habilitado = true;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
