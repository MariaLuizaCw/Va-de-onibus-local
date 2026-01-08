-- =============================================================================
-- rio.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo rio

-- -----------------------------------------------------------------------------
-- fn_enrich_gps_batch_with_sentido_json
-- Calcula o sentido mais próximo para múltiplos pontos GPS usando PostGIS
-- Recebe JSON array e usa jsonb_to_recordset para processar
-- Usado por: rio.js -> enrichRecordsWithSentido (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_enrich_gps_batch_with_sentido_json(
    p_points jsonb,
    p_max_snap_distance_meters numeric
)
RETURNS TABLE (
    linha text,
    ordem text,
    sentido text,
    dist_m double precision,
    itinerario_id integer,
    route_name text
)
LANGUAGE sql
STABLE
AS $$
    WITH pts AS (
        SELECT
            (r.value->>'linha')::text AS linha,
            (r.value->>'lon')::double precision AS lon,
            (r.value->>'lat')::double precision AS lat,
            (r.value->>'ordem')::text AS ordem
        FROM jsonb_array_elements(p_points) r
    )
    SELECT
        pts.linha,
        pts.ordem,
        best.sentido,
        best.dist_m,
        best.itinerario_id,
        best.route_name
    FROM pts
    LEFT JOIN LATERAL (
        SELECT
            i.sentido,
            i.id AS itinerario_id,
            i.route_name,
            ST_Distance(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography
            ) AS dist_m
        FROM public.itinerario i
        WHERE i.habilitado = true
          AND i.numero_linha::text = pts.linha
          AND ST_DWithin(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography,
                p_max_snap_distance_meters
          )
        ORDER BY dist_m ASC
        LIMIT 1
    ) best ON true;
$$;

-- -----------------------------------------------------------------------------
-- fn_upsert_gps_onibus_estado_batch_json
-- Atualiza o estado dos ônibus com base na proximidade aos terminais
-- Recebe JSON array e usa jsonb_to_recordset para processar
-- Usado por: rio.js -> saveRioToGpsOnibusEstado (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_upsert_gps_onibus_estado_batch_json(
    p_points jsonb,
    p_terminal_visit_distance_meters numeric,
    p_terminal_proximity_distance_meters numeric
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    WITH pts AS (
        SELECT
            (r.value->>'ordem')::text AS ordem,
            (r.value->>'linha')::text AS linha,
            (r.value->>'lon')::double precision AS lon,
            (r.value->>'lat')::double precision AS lat,
            (r.value->>'datahora')::timestamp AS datahora
        FROM jsonb_array_elements(p_points) r
    ),
    terminal_distances AS (
        SELECT
            pts.ordem,
            pts.linha,
            pts.lon,
            pts.lat,
            pts.datahora,
            i.sentido,
            ST_Distance(
                ST_StartPoint(i.the_geom)::geography,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography
            ) AS dist_m
        FROM pts
        INNER JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha::text = pts.linha
    ),
    best_terminal AS (
        SELECT DISTINCT ON (ordem)
            ordem,
            linha,
            datahora,
            sentido,
            dist_m
        FROM terminal_distances
        ORDER BY ordem, dist_m ASC
    ),
    upsert_data AS (
        SELECT
            bt.ordem,
            bt.linha,
            bt.datahora,
            bt.sentido,
            bt.dist_m,
            CASE
                WHEN bt.dist_m <= p_terminal_visit_distance_meters THEN bt.sentido
                ELSE NULL
            END AS new_ultimo_terminal,
            CASE
                WHEN bt.dist_m <= p_terminal_visit_distance_meters THEN bt.datahora
                ELSE NULL
            END AS new_ultima_passagem,
            CASE
                WHEN bt.dist_m > p_terminal_visit_distance_meters AND bt.dist_m <= p_terminal_proximity_distance_meters THEN bt.sentido
                ELSE NULL
            END AS new_terminal_proximo,
            CASE
                WHEN bt.dist_m > p_terminal_visit_distance_meters AND bt.dist_m <= p_terminal_proximity_distance_meters THEN bt.dist_m
                ELSE NULL
            END AS new_distancia_terminal,
            CASE
                WHEN bt.dist_m > p_terminal_visit_distance_meters AND bt.dist_m <= p_terminal_proximity_distance_meters THEN bt.datahora
                ELSE NULL
            END AS new_desde_terminal_proximo
        FROM best_terminal bt
    )
    INSERT INTO gps_onibus_estado (
        ordem,
        linha,
        token,
        ultimo_terminal,
        ultima_passagem_terminal,
        terminal_proximo,
        distancia_terminal_metros,
        desde_terminal_proximo,
        atualizado_em
    )
    SELECT
        ud.ordem,
        ud.linha,
        'PMRJ',
        COALESCE(ud.new_ultimo_terminal, ''),
        ud.new_ultima_passagem,
        ud.new_terminal_proximo,
        ud.new_distancia_terminal,
        ud.new_desde_terminal_proximo,
        now()
    FROM upsert_data ud
    ON CONFLICT (ordem) DO UPDATE SET
        linha = EXCLUDED.linha,
        token = EXCLUDED.token,
        ultimo_terminal = CASE
            WHEN EXCLUDED.ultimo_terminal != '' THEN EXCLUDED.ultimo_terminal
            ELSE gps_onibus_estado.ultimo_terminal
        END,
        ultima_passagem_terminal = CASE
            WHEN EXCLUDED.ultimo_terminal != '' THEN EXCLUDED.ultima_passagem_terminal
            ELSE gps_onibus_estado.ultima_passagem_terminal
        END,
        terminal_proximo = CASE
            WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
            ELSE EXCLUDED.terminal_proximo
        END,
        distancia_terminal_metros = CASE
            WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
            ELSE EXCLUDED.distancia_terminal_metros
        END,
        desde_terminal_proximo = CASE
            WHEN EXCLUDED.ultimo_terminal != '' THEN NULL
            WHEN EXCLUDED.terminal_proximo IS NOT NULL
                AND gps_onibus_estado.terminal_proximo = EXCLUDED.terminal_proximo
                AND gps_onibus_estado.desde_terminal_proximo IS NOT NULL
            THEN gps_onibus_estado.desde_terminal_proximo
            ELSE EXCLUDED.desde_terminal_proximo
        END,
        atualizado_em = now();
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_insert_gps_posicoes_rio_batch_json
-- Insere registros GPS do Rio em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Usado por: rio.js -> saveRioRecordsToDb (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_gps_posicoes_rio_batch_json(p_records jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gps_posicoes_rio (
        ordem,
        latitude,
        longitude,
        datahora,
        velocidade,
        linha,
        datahoraenvio,
        datahoraservidor
    )
    SELECT
        (r.value->>'ordem')::text,
        (r.value->>'latitude')::double precision,
        (r.value->>'longitude')::double precision,
        (r.value->>'datahora')::bigint,
        (r.value->>'velocidade')::double precision,
        (r.value->>'linha')::text,
        (r.value->>'datahoraenvio')::bigint,
        (r.value->>'datahoraservidor')::bigint
    FROM jsonb_array_elements(p_records) r
    ON CONFLICT ON CONSTRAINT gps_posicoes_rio_unique_ponto DO NOTHING;
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_insert_gps_sentido_rio_batch_json
-- Insere registros GPS com sentido do Rio em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Usado por: rio.js -> saveRioToGpsSentido (batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_gps_sentido_rio_batch_json(p_records jsonb)
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
