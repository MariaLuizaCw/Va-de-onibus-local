-- =============================================================================
-- angra.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo angra

-- -----------------------------------------------------------------------------
-- fn_insert_gps_posicoes_angra_batch_json
-- Insere registros GPS de Angra em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Usado por: angra.js -> saveAngraRecordsToDb (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_gps_posicoes_angra_batch_json(p_records jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gps_posicoes_angra (
        vehicle_integration_code,
        vehicle_description,
        line_integration_code,
        line_number,
        line_description,
        route_integration_code,
        route_direction,
        route_description,
        estimated_departure_date,
        estimated_arrival_date,
        real_departure_date,
        real_arrival_date,
        shift,
        latitude,
        longitude,
        event_date,
        update_date,
        speed,
        direction,
        event_code,
        event_name,
        is_route_start_point,
        is_route_end_point,
        is_garage,
        license_plate,
        client_bus_integration_code,
        route_type
    )
    SELECT
        (r.value->>'vehicle_integration_code')::text,
        (r.value->>'vehicle_description')::text,
        (r.value->>'line_integration_code')::text,
        (r.value->>'line_number')::text,
        (r.value->>'line_description')::text,
        (r.value->>'route_integration_code')::text,
        (r.value->>'route_direction')::integer,
        (r.value->>'route_description')::text,
        (r.value->>'estimated_departure_date')::timestamp,
        (r.value->>'estimated_arrival_date')::timestamp,
        (r.value->>'real_departure_date')::timestamp,
        (r.value->>'real_arrival_date')::timestamp,
        (r.value->>'shift')::integer,
        (r.value->>'latitude')::double precision,
        (r.value->>'longitude')::double precision,
        (r.value->>'event_date')::timestamp,
        (r.value->>'update_date')::timestamp,
        (r.value->>'speed')::double precision,
        (r.value->>'direction')::double precision,
        (r.value->>'event_code')::integer,
        (r.value->>'event_name')::text,
        (r.value->>'is_route_start_point')::boolean,
        (r.value->>'is_route_end_point')::boolean,
        (r.value->>'is_garage')::boolean,
        (r.value->>'license_plate')::text,
        (r.value->>'client_bus_integration_code')::text,
        (r.value->>'route_type')::text
    FROM jsonb_array_elements(p_records) r
    ON CONFLICT ON CONSTRAINT gps_posicoes_angra_unique_ponto DO NOTHING;
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_insert_gps_sentido_angra_batch_json
-- Insere registros GPS com sentido de Angra em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Usado por: angra.js -> saveAngraToGpsSentido (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_gps_sentido_angra_batch_json(p_records jsonb)
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
    ON CONFLICT (ordem, datahora) DO NOTHING;
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
