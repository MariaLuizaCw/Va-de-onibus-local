

-- -----------------------------------------------------------------------------
-- fn_insert_gps_proximidade_terminal_evento_json
-- Insere eventos de proximidade de GPS de ônibus com terminais
-- Para cada ponto GPS, encontra o terminal mais próximo da mesma linha
-- Insere apenas se a distância for menor ou igual a p_max_distance_meters
-- Usado por: rioFetcher.js -> processamento de eventos de proximidade
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_gps_proximidade_terminal_evento_json(
    p_points jsonb,
    p_max_distance_meters numeric DEFAULT 300
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    WITH pts AS (
        SELECT
            (r.value->>'ordem')::text AS ordem,
            (r.value->>'datahora')::timestamp AS datahora,
            (r.value->>'lon')::double precision AS lon,
            (r.value->>'lat')::double precision AS lat,
            (r.value->>'linha')::text AS linha
        FROM jsonb_array_elements(p_points) r
    ),
    -- Calcular distâncias para itinerários da mesma linha
    terminal_distances AS (
        SELECT
            pts.ordem,
            pts.datahora,
            i.id AS itinerario_id,
            i.sentido,
            ST_Distance(
                ST_StartPoint(i.the_geom)::geography,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography
            ) AS dist_m
        FROM pts
        JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha = pts.linha
    ),
    -- Encontrar o terminal mais próximo para cada ponto GPS
    closest_terminal AS (
        SELECT DISTINCT ON (td.ordem, td.datahora)
            td.ordem,
            td.datahora,
            pts.linha,
            td.itinerario_id,
            td.sentido,
            td.dist_m
        FROM terminal_distances td
        JOIN pts ON pts.ordem = td.ordem AND pts.datahora = td.datahora
        ORDER BY td.ordem, td.datahora, td.dist_m ASC
    )
    -- Inserir apenas se distância <= p_max_distance_meters
    INSERT INTO public.gps_proximidade_terminal_evento (ordem, datahora, linha, itinerario_id, sentido, distancia_metros)
    SELECT
        ct.ordem,
        ct.datahora,
        ct.linha,
        ct.itinerario_id,
        ct.sentido,
        ROUND(ct.dist_m::numeric, 2)
    FROM closest_terminal ct
    WHERE ct.dist_m <= p_max_distance_meters
    ON CONFLICT (ordem, datahora) DO UPDATE SET
        linha = EXCLUDED.linha,
        itinerario_id = EXCLUDED.itinerario_id,
        sentido = EXCLUDED.sentido,
        distancia_metros = EXCLUDED.distancia_metros;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_enrich_gps_batch_with_sentido_json
-- Infere o sentido atual de cada ônibus combinando:
-- - Evidência temporal (proximidade com terminais)
-- - Coerência espacial atual (projeção do GPS na trajetória)
-- Usado por: rio.js -> enrichRecordsWithSentido
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_enrich_gps_batch_with_sentido_json(
    p_points jsonb,
    p_max_snap_distance_meters numeric DEFAULT 300,
    p_terminal_passage_distance_meters numeric DEFAULT 20,
    p_terminal_proximity_distance_meters numeric DEFAULT 100,
    p_proximity_window_minutes numeric DEFAULT 15,
    p_proximity_min_duration_minutes numeric DEFAULT 10
)
RETURNS TABLE (
    ordem text,
    linha text,
    sentido text,
    itinerario_id integer,
    route_name text,
    dist_m numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH pts AS (
        SELECT
            (r.value->>'ordem')::text AS ordem,
            (r.value->>'linha')::text AS linha,
            (r.value->>'lon')::double precision AS lon,
            (r.value->>'lat')::double precision AS lat
        FROM jsonb_array_elements(p_points) r
    ),
    
    -- =========================================================================
    -- REGRA A: Passagem pelo terminal (distância <= 20m)
    -- Busca o registro mais recente onde o ônibus passou a <= 20m do terminal
    -- =========================================================================
    regra_a AS (
        SELECT DISTINCT ON (pts.ordem)
            pts.ordem,
            pts.linha,
            e.itinerario_id,
            e.sentido,
            e.datahora AS timestamp_evidencia
        FROM pts
        JOIN public.gps_proximidade_terminal_evento e
            ON e.ordem = pts.ordem
            AND e.linha = pts.linha
            AND e.distancia_metros <= p_terminal_passage_distance_meters
        ORDER BY pts.ordem, e.datahora DESC
    ),
    
    -- =========================================================================
    -- REGRA B: Permanência próxima ao terminal (100m por >= 10 min em janela de 20 min)
    -- =========================================================================
    regra_b_registros AS (
        SELECT
            pts.ordem,
            pts.linha,
            e.itinerario_id,
            e.sentido,
            e.datahora
        FROM pts
        JOIN public.gps_proximidade_terminal_evento e
            ON e.ordem = pts.ordem
            AND e.linha = pts.linha
            AND e.distancia_metros <= p_terminal_proximity_distance_meters
    ),
    regra_b_max_datahora AS (
        SELECT
            rb.ordem,
            rb.linha,
            rb.itinerario_id,
            rb.sentido,
            MAX(rb.datahora) AS max_datahora
        FROM regra_b_registros rb
        GROUP BY rb.ordem, rb.linha, rb.itinerario_id, rb.sentido
    ),
    regra_b_janela AS (
        SELECT
            rbd.ordem,
            rbd.linha,
            rbd.itinerario_id,
            rbd.sentido,
            rbd.max_datahora,
            MIN(rb.datahora) AS min_datahora_janela
        FROM regra_b_max_datahora rbd
        JOIN regra_b_registros rb ON rb.ordem = rbd.ordem AND rb.linha = rbd.linha AND rb.itinerario_id = rbd.itinerario_id AND rb.sentido = rbd.sentido
        WHERE rb.datahora >= rbd.max_datahora - (p_proximity_window_minutes || ' minutes')::INTERVAL
        GROUP BY rbd.ordem, rbd.linha, rbd.itinerario_id, rbd.sentido, rbd.max_datahora
    ),
    regra_b AS (
        SELECT DISTINCT ON (rbj.ordem)
            rbj.ordem,
            rbj.linha,
            rbj.itinerario_id,
            rbj.sentido,
            rbj.max_datahora AS timestamp_evidencia
        FROM regra_b_janela rbj
        WHERE rbj.max_datahora - rbj.min_datahora_janela >= (p_proximity_min_duration_minutes || ' minutes')::INTERVAL
        ORDER BY rbj.ordem, rbj.max_datahora DESC
    ),
    
    -- =========================================================================
    -- CANDIDATOS: União das regras A e B, escolher o mais recente
    -- =========================================================================
    candidatos AS (
        SELECT ra.ordem, ra.linha, ra.itinerario_id, ra.sentido, ra.timestamp_evidencia FROM regra_a ra
        UNION ALL
        SELECT rb.ordem, rb.linha, rb.itinerario_id, rb.sentido, rb.timestamp_evidencia FROM regra_b rb
    ),
    candidato_escolhido AS (
        SELECT DISTINCT ON (cand.ordem)
            cand.ordem,
            cand.linha,
            cand.itinerario_id,
            cand.sentido
        FROM candidatos cand
        ORDER BY cand.ordem, cand.timestamp_evidencia DESC
    ),
    
    -- =========================================================================
    -- VALIDAÇÃO ESPACIAL: Projetar GPS na LineString do sentido preliminar
    -- =========================================================================
    validacao_espacial AS (
        SELECT
            c.ordem,
            c.linha,
            c.itinerario_id,
            c.sentido,
            i.route_name,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography,
                ST_ClosestPoint(ST_SetSRID(i.the_geom, 4326), ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326))::geography
            ) AS dist_proj
        FROM candidato_escolhido c
        JOIN pts ON pts.ordem = c.ordem
        JOIN public.itinerario i ON i.id = c.itinerario_id
    ),
    
    -- =========================================================================
    -- FALLBACK ESPACIAL: Se projeção > 300m, buscar melhor sentido espacialmente
    -- =========================================================================
    fallback_espacial AS (
        SELECT
            pts.ordem,
            pts.linha,
            i.id AS itinerario_id,
            i.sentido,
            i.route_name,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)::geography,
                ST_ClosestPoint(ST_SetSRID(i.the_geom, 4326), ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326))::geography
            ) AS dist_proj
        FROM pts
        JOIN public.itinerario i
            ON i.habilitado = true
            AND i.numero_linha = pts.linha
    ),
    fallback_melhor AS (
        SELECT DISTINCT ON (fe.ordem)
            fe.ordem,
            fe.linha,
            fe.itinerario_id,
            fe.sentido,
            fe.route_name,
            fe.dist_proj
        FROM fallback_espacial fe
        ORDER BY fe.ordem, fe.dist_proj ASC
    ),
    
    -- =========================================================================
    -- RESULTADO FINAL: Combinar validação espacial com fallback
    -- =========================================================================
    resultado AS (
        SELECT
            pts.ordem AS ordem,
            pts.linha AS linha,
            CASE
                -- Sentido preliminar válido (projeção <= 300m)
                WHEN v.dist_proj <= p_max_snap_distance_meters THEN v.sentido
                -- Fallback válido (projeção <= 300m)
                WHEN f.dist_proj <= p_max_snap_distance_meters THEN f.sentido
                -- Garagem (nenhum sentido válido)
                ELSE NULL
            END AS sentido,
            CASE
                WHEN v.dist_proj <= p_max_snap_distance_meters THEN v.itinerario_id
                WHEN f.dist_proj <= p_max_snap_distance_meters THEN f.itinerario_id
                ELSE NULL
            END AS itinerario_id,
            CASE
                WHEN v.dist_proj <= p_max_snap_distance_meters THEN v.route_name
                WHEN f.dist_proj <= p_max_snap_distance_meters THEN f.route_name
                ELSE NULL
            END AS route_name,
            CASE
                WHEN v.dist_proj <= p_max_snap_distance_meters THEN ROUND(v.dist_proj::numeric, 2)
                WHEN f.dist_proj <= p_max_snap_distance_meters THEN ROUND(f.dist_proj::numeric, 2)
                ELSE NULL
            END AS dist_m
        FROM pts
        LEFT JOIN validacao_espacial v ON v.ordem = pts.ordem
        LEFT JOIN fallback_melhor f ON f.ordem = pts.ordem
    )
    
    SELECT
        r.ordem,
        r.linha,
        r.sentido,
        r.itinerario_id,
        r.route_name,
        r.dist_m
    FROM resultado r;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_cleanup_gps_proximidade_terminal_evento
-- Remove eventos de proximidade antigos de forma segura e controlada
-- Usado por: cleanup.js -> cleanupProximityEvents
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_cleanup_gps_proximidade_terminal_evento(
    p_retention_hours integer DEFAULT 8
)
RETURNS TABLE (
    deleted_count bigint,
    retention_hours integer,
    cleanup_timestamp timestamp
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff_time timestamp;
    v_deleted_count bigint;
BEGIN
    -- Calcula o tempo de corte
    v_cutoff_time := NOW() - (p_retention_hours || ' hours')::interval;
    
    -- Remove os registros antigos
    DELETE FROM gps_proximidade_terminal_evento 
    WHERE datahora < v_cutoff_time;
    
    -- Contabiliza os registros removidos
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    -- Retorna informações sobre a operação
    RETURN QUERY SELECT 
        v_deleted_count::bigint,
        p_retention_hours::integer,
        NOW()::timestamp;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_processar_viagens_rio
-- Processa mudanças de sentido e gerencia viagens abertas/fechadas
-- Função dedicada para lógica stateful de viagens
-- Usado por: fn_upsert_gps_sentido_rio_batch_json
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_processar_viagens_rio(
    p_records jsonb,
    p_min_duration_minutes integer DEFAULT 25
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1️⃣ Fechar SOMENTE a última viagem aberta
    WITH novos AS (
        SELECT 
            (r.value->>'ordem')::text      AS ordem,
            (r.value->>'token')::text      AS token,
            (r.value->>'linha')::text      AS linha,
            (r.value->>'sentido')::text    AS sentido,
            (r.value->>'sentido_itinerario_id')::int AS itinerario_id,
            (r.value->>'datahora')::timestamp AS datahora
        FROM jsonb_array_elements(p_records) r
    ),
    mudanca AS (
        SELECT
            n.*,
            gs.sentido AS sentido_anterior,
            gs.sentido_itinerario_id AS itinerario_anterior
        FROM novos n
        JOIN gps_sentido gs
          ON gs.ordem = n.ordem
         AND gs.token = n.token
        JOIN public.itinerario i_ant
          ON i_ant.id = gs.sentido_itinerario_id
        JOIN public.itinerario i_novo
          ON i_novo.id = n.itinerario_id
        WHERE
            gs.sentido IS NOT NULL
            AND n.sentido IS NOT NULL
            AND LOWER(gs.sentido) <> LOWER(n.sentido)
            AND gs.sentido_itinerario_id <> n.itinerario_id
            AND NOT ST_Equals(
                ST_StartPoint(i_ant.the_geom),
                ST_StartPoint(i_novo.the_geom)
            )
            -- AND n.datahora - gs.datahora > (p_min_duration_minutes || ' minutes')::INTERVAL
    ),
    ultima_viagem AS (
        SELECT DISTINCT ON (hv.ordem, hv.token)
            hv.id,
            m.datahora,
            m.itinerario_id    AS itinerario_destino,
            m.sentido          AS nome_terminal_destino
        FROM mudanca m
        JOIN gps_historico_viagens hv
        ON hv.ordem = m.ordem
        AND hv.token = m.token
        AND hv.timestamp_fim IS NULL
        AND m.datahora > hv.timestamp_inicio   -- 🔥 ESSENCIAL
        ORDER BY
            hv.ordem,
            hv.token,
            hv.timestamp_inicio DESC,
            m.datahora DESC
    )
    UPDATE gps_historico_viagens hv
    SET
        timestamp_fim = uv.datahora,
        duracao_viagem = uv.datahora - hv.timestamp_inicio,
        itinerario_id_destino = uv.itinerario_destino,
        nome_terminal_destino = uv.nome_terminal_destino
    FROM ultima_viagem uv
    WHERE hv.id = uv.id;

    -- 2️⃣ Abrir nova viagem no novo sentido
    WITH novos AS (
        SELECT 
            (r.value->>'ordem')::text      AS ordem,
            (r.value->>'token')::text      AS token,
            (r.value->>'linha')::text      AS linha,
            (r.value->>'sentido')::text    AS sentido,
            (r.value->>'sentido_itinerario_id')::int AS itinerario_id,
            (r.value->>'datahora')::timestamp AS datahora
        FROM jsonb_array_elements(p_records) r
    ),
    mudanca AS (
        SELECT
            n.*
        FROM novos n
        JOIN gps_sentido gs
          ON gs.ordem = n.ordem
         AND gs.token = n.token
        JOIN public.itinerario i_ant
          ON i_ant.id = gs.sentido_itinerario_id
        JOIN public.itinerario i_novo
          ON i_novo.id = n.itinerario_id
        WHERE
            gs.sentido IS NOT NULL
            AND n.sentido IS NOT NULL
            AND LOWER(gs.sentido) <> LOWER(n.sentido)
            AND gs.sentido_itinerario_id <> n.itinerario_id
            AND NOT ST_Equals(
                ST_StartPoint(i_ant.the_geom),
                ST_StartPoint(i_novo.the_geom)
            )
            -- AND n.datahora - gs.datahora > (p_min_duration_minutes || ' minutes')::INTERVAL
    )
    INSERT INTO gps_historico_viagens (
        ordem,
        token,
        linha,
        itinerario_id_origem,
        nome_terminal_origem,
        timestamp_inicio,
        timestamp_fim,
        duracao_viagem
    )
    SELECT
        m.ordem,
        m.token,
        m.linha,
        m.itinerario_id,
        m.sentido,
        m.datahora,
        NULL,
        NULL
    FROM mudanca m
    ON CONFLICT (ordem, token, timestamp_inicio) DO NOTHING;
END;
$$;

-- -----------------------------------------------------------------------------
-- fn_upsert_gps_sentido_rio_batch_json
-- Upsert registros GPS com sentido do Rio em batch
-- Recebe JSON array e usa jsonb_array_elements para processar
-- Chama função dedicada para processar viagens
-- Usado por: rio.js -> saveRioToGpsSentido (batch)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_upsert_gps_sentido_rio_batch_json(
    p_records jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Processar viagens com lógica stateful
    PERFORM fn_processar_viagens_rio(p_records);

    -- Upsert normal dos registros GPS (mantido inalterado)
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
    WHERE gps_sentido.datahora IS NULL OR EXCLUDED.datahora > gps_sentido.datahora;
END;
$$;




-- -----------------------------------------------------------------------------
-- fn_cleanup_historico_viagens
-- Remove registros de histórico de viagens mais antigos que o período de retenção
-- Usado por: rio.js -> cleanupHistoricoViagens
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_cleanup_historico_viagens(
    p_retention_days integer DEFAULT 30
)
RETURNS TABLE (
    deleted_count bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count bigint;
    v_cutoff_date timestamp;
BEGIN
    -- Calcular data de corte
    v_cutoff_date := NOW() - (p_retention_days || ' days')::INTERVAL;
    
    -- Remover registros antigos
    DELETE FROM public.gps_historico_viagens
    WHERE created_at < v_cutoff_date;
    
    -- Contabiliza os registros removidos
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    -- Retorna informações sobre a operação
    RETURN QUERY SELECT 
        v_deleted_count::bigint;
END;
$$;


-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
