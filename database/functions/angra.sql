-- =============================================================================
-- angra.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo angra


-- -----------------------------------------------------------------------------
-- fn_upsert_gps_sentido_angra_batch_json
-- Upsert registros GPS com sentido de Angra em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Atualiza apenas se o novo registro for mais recente que o existente
-- Usado por: angra.js -> saveAngraToGpsSentido (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_upsert_gps_sentido_angra_batch_json(p_records jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gps_sentido (
        ordem,
        datahora,
        linha,
        latitude,
        longitude,
        velocidade,
        sentido,
        sentido_itinerario_id,
        route_name,
        token
    )
    SELECT
        (r.value->>'ordem')::text,
        (r.value->>'datahora')::timestamp,
        (r.value->>'linha')::text,
        (r.value->>'latitude')::double precision,
        (r.value->>'longitude')::double precision,
        (r.value->>'velocidade')::double precision,
        (r.value->>'sentido')::text,
        (r.value->>'sentido_itinerario_id')::integer,
        (r.value->>'route_name')::text,
        (r.value->>'token')::text
    FROM jsonb_array_elements(p_records) r
    ON CONFLICT (token, ordem) DO UPDATE SET
        datahora = EXCLUDED.datahora,
        linha = EXCLUDED.linha,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        velocidade = EXCLUDED.velocidade,
        sentido = EXCLUDED.sentido,
        sentido_itinerario_id = EXCLUDED.sentido_itinerario_id,
        route_name = EXCLUDED.route_name
    WHERE gps_sentido.datahora IS NULL 
       OR EXCLUDED.datahora > gps_sentido.datahora;
END;
$$;

-- -----------------------------------------------------------------------------
-- ftdbgps_atualiza_gps_sentido
-- Função vazia que retorna 'OK'
-- Usado para testes/chamadas de API
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gps.ftdbgps_atualiza_gps_sentido()
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN 'OK';
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
