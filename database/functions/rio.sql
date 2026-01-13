-- =============================================================================
-- rio.js - Database Functions
-- =============================================================================
-- Execute este script para criar/atualizar as functions utilizadas pelo rio

-- -----------------------------------------------------------------------------
-- fn_enrich_gps_batch_with_sentido_json
-- Calcula o sentido mais próximo para múltiplos pontos GPS usando PostGIS
-- Recebe JSON array e usa jsonb_to_recordset para processar
-- 
-- LÓGICA DE PRIORIDADE:
-- 1. Último terminal (gps_onibus_estado.ultimo_terminal) se dist <= 300m
-- 2. Terminal próximo se janela (ate - desde) <= 10 min e dist <= 300m
-- 3. Fallback: itinerário mais próximo por distância se dist <= 300m
-- 4. INDEFINIDO se dist > 300m de qualquer itinerário
--
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
    ),
    -- Join com estado do ônibus para obter ultimo_terminal e terminal_proximo
    pts_with_estado AS (
        SELECT
            pts.linha,
            pts.lon,
            pts.lat,
            pts.ordem,
            e.ultimo_terminal,
            e.terminal_proximo,
            e.desde_terminal_proximo,
            e.ate_terminal_proximo
        FROM pts
        LEFT JOIN public.gps_onibus_estado e ON e.ordem = pts.ordem AND e.ativo = true
    ),
    -- REGRA 1: Último terminal (prioridade máxima)
    regra1_ultimo_terminal AS (
        SELECT DISTINCT ON (p.ordem)
            p.ordem,
            p.linha,
            i.sentido,
            i.id AS itinerario_id,
            i.route_name,
            ST_Distance(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography
            ) AS dist_m
        FROM pts_with_estado p
        INNER JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha::text = p.linha
            AND i.sentido = p.ultimo_terminal
        WHERE p.ultimo_terminal IS NOT NULL
          AND ST_DWithin(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography,
                p_max_snap_distance_meters
          )
        ORDER BY p.ordem, dist_m ASC
    ),
    -- REGRA 2: Terminal próximo (janela <= 10 min)
    regra2_terminal_proximo AS (
        SELECT DISTINCT ON (p.ordem)
            p.ordem,
            p.linha,
            i.sentido,
            i.id AS itinerario_id,
            i.route_name,
            ST_Distance(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography
            ) AS dist_m
        FROM pts_with_estado p
        INNER JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha::text = p.linha
            AND i.sentido = p.terminal_proximo
        WHERE p.terminal_proximo IS NOT NULL
          AND p.desde_terminal_proximo IS NOT NULL
          AND p.ate_terminal_proximo IS NOT NULL
          AND (p.ate_terminal_proximo - p.desde_terminal_proximo) <= INTERVAL '10 minutes'
          AND ST_DWithin(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography,
                p_max_snap_distance_meters
          )
        ORDER BY p.ordem, dist_m ASC
    ),
    -- REGRA 3: Fallback por distância (comportamento atual)
    regra3_fallback AS (
        SELECT DISTINCT ON (p.ordem)
            p.ordem,
            p.linha,
            i.sentido,
            i.id AS itinerario_id,
            i.route_name,
            ST_Distance(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography
            ) AS dist_m
        FROM pts_with_estado p
        INNER JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha::text = p.linha
        WHERE ST_DWithin(
                i.the_geom::geography,
                ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography,
                p_max_snap_distance_meters
          )
        ORDER BY p.ordem, dist_m ASC
    )
    -- Combinar regras com prioridade: 1 > 2 > 3 > 4 (INDEFINIDO)
    SELECT
        p.linha,
        p.ordem,
        COALESCE(r1.sentido, r2.sentido, r3.sentido, 'garagem') AS sentido,
        COALESCE(r1.dist_m, r2.dist_m, r3.dist_m) AS dist_m,
        COALESCE(r1.itinerario_id, r2.itinerario_id, r3.itinerario_id) AS itinerario_id,
        COALESCE(r1.route_name, r2.route_name, r3.route_name) AS route_name
    FROM pts_with_estado p
    LEFT JOIN regra1_ultimo_terminal r1 ON r1.ordem = p.ordem
    LEFT JOIN regra2_terminal_proximo r2 ON r2.ordem = p.ordem AND r1.ordem IS NULL
    LEFT JOIN regra3_fallback r3 ON r3.ordem = p.ordem AND r1.ordem IS NULL AND r2.ordem IS NULL;
$$;

-- -----------------------------------------------------------------------------
-- fn_upsert_gps_onibus_estado_batch_json
-- Atualiza o estado dos ônibus com base na proximidade aos terminais
-- Recebe JSON array e usa jsonb_to_recordset para processar
-- Registra intervalo de permanência: desde_terminal_proximo (início) e ate_terminal_proximo (fim)
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
            -- Visita ao terminal (dist <= visit_distance)
            CASE
                WHEN bt.dist_m <= p_terminal_visit_distance_meters THEN bt.sentido
                ELSE NULL
            END AS new_ultimo_terminal,
            CASE
                WHEN bt.dist_m <= p_terminal_visit_distance_meters THEN bt.datahora
                ELSE NULL
            END AS new_ultima_passagem,
            -- Proximidade ao terminal (visit_distance < dist <= proximity_distance)
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
            END AS new_desde_terminal_proximo,
            CASE
                WHEN bt.dist_m > p_terminal_visit_distance_meters AND bt.dist_m <= p_terminal_proximity_distance_meters THEN bt.datahora
                ELSE NULL
            END AS new_ate_terminal_proximo
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
        ate_terminal_proximo,
        atualizado_em,
        ativo
    )
    SELECT
        ud.ordem,
        ud.linha,
        'PMRJ',
        ud.new_ultimo_terminal,
        ud.new_ultima_passagem,
        ud.new_terminal_proximo,
        ud.new_distancia_terminal,
        ud.new_desde_terminal_proximo,
        ud.new_ate_terminal_proximo,
        now(),
        true
    FROM upsert_data ud
    ON CONFLICT (ordem) DO UPDATE SET
        linha = EXCLUDED.linha,
        token = EXCLUDED.token,
        ativo = true,
        -- Regra 1: Visita ao terminal - atualiza terminal e limpa proximidade
        ultimo_terminal = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN EXCLUDED.ultimo_terminal
            ELSE gps_onibus_estado.ultimo_terminal
        END,
        ultima_passagem_terminal = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN EXCLUDED.ultima_passagem_terminal
            ELSE gps_onibus_estado.ultima_passagem_terminal
        END,
        -- Regra 1: Limpa terminal_proximo e timestamps quando visita
        terminal_proximo = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN NULL
            ELSE EXCLUDED.terminal_proximo
        END,
        distancia_terminal_metros = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN NULL
            ELSE EXCLUDED.distancia_terminal_metros
        END,
        -- Regra 2: desde_terminal_proximo - mantém se mesmo terminal, senão atualiza
        desde_terminal_proximo = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN NULL
            WHEN EXCLUDED.terminal_proximo IS NULL THEN NULL
            WHEN EXCLUDED.terminal_proximo IS NOT NULL
                AND gps_onibus_estado.terminal_proximo = EXCLUDED.terminal_proximo
                AND gps_onibus_estado.desde_terminal_proximo IS NOT NULL
            THEN gps_onibus_estado.desde_terminal_proximo
            ELSE EXCLUDED.desde_terminal_proximo
        END,
        -- Regra 2: ate_terminal_proximo - sempre atualiza quando no raio de proximidade
        ate_terminal_proximo = CASE
            WHEN EXCLUDED.ultimo_terminal IS NOT NULL THEN NULL
            WHEN EXCLUDED.terminal_proximo IS NULL THEN NULL
            WHEN EXCLUDED.terminal_proximo IS NOT NULL THEN EXCLUDED.ate_terminal_proximo
            ELSE gps_onibus_estado.ate_terminal_proximo
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

-- -----------------------------------------------------------------------------
-- fn_deactivate_gps_onibus_estado_by_ordens
-- Marca registros como inativos (ativo=false) na tabela gps_onibus_estado
-- Recebe JSON array de ordens e atualiza todos em batch
-- Usado por: rio.js -> deactivateInactiveOnibusEstado
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_deactivate_gps_onibus_estado_by_ordens(p_ordens jsonb)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    updated_count integer;
BEGIN
    UPDATE gps_onibus_estado
    SET ativo = false, atualizado_em = now()
    WHERE ordem IN (
        SELECT jsonb_array_elements_text(p_ordens)
    )
    AND ativo = true;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
