

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
-- gps_ultima_passagem
-- Tabela que armazena a última passagem identificada de cada ônibus por linha
-- Chave primária composta: (ordem, linha)
-- -----------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS gps_ultima_passagem (
--     ordem TEXT NOT NULL,
--     linha TEXT NOT NULL,
--     label_ultima_passagem TEXT,
--     datahora_atualizacao TIMESTAMP WITH TIME ZONE NOT NULL,
--     datahora_identificacao TIMESTAMP WITH TIME ZONE,
--     PRIMARY KEY (ordem, linha)
-- );

-- -----------------------------------------------------------------------------
-- fn_atualizar_ultima_passagem
-- Atualiza a tabela gps_ultima_passagem com base na regra B:
-- - Ônibus que permaneceu >= 8 minutos a <= 150m do início de um itinerário
-- - Carrega o sentido correspondente no label_ultima_passagem
-- Usado por: rioFetcher.js -> processamento de última passagem
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_atualizar_ultima_passagem(
    p_points jsonb,
    p_terminal_proximity_distance_meters numeric DEFAULT 150,
    p_proximity_window_minutes numeric DEFAULT 30,
    p_proximity_min_duration_minutes numeric DEFAULT 8
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    WITH pts AS (
        SELECT
            (r.value->>'ordem')::text AS ordem,
            (r.value->>'linha')::text AS linha,
            (r.value->>'datahora')::timestamp with time zone AS datahora
        FROM jsonb_array_elements(p_points) r
    ),
    
    -- =========================================================================
    -- REGRA B: Permanência próxima ao terminal (150m por >= 8 min em janela de 30 min)
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
        JOIN regra_b_registros rb 
            ON rb.ordem = rbd.ordem 
            AND rb.linha = rbd.linha 
            AND rb.itinerario_id = rbd.itinerario_id 
            AND rb.sentido = rbd.sentido
        WHERE rb.datahora >= rbd.max_datahora - (p_proximity_window_minutes || ' minutes')::INTERVAL
        GROUP BY rbd.ordem, rbd.linha, rbd.itinerario_id, rbd.sentido, rbd.max_datahora
    ),
    regra_b AS (
        SELECT DISTINCT ON (rbj.ordem, rbj.linha)
            rbj.ordem,
            rbj.linha,
            rbj.sentido,
            rbj.max_datahora AS timestamp_identificacao
        FROM regra_b_janela rbj
        WHERE rbj.max_datahora - rbj.min_datahora_janela >= (p_proximity_min_duration_minutes || ' minutes')::INTERVAL
        ORDER BY rbj.ordem, rbj.linha, rbj.max_datahora DESC
    ),
    
    -- =========================================================================
    -- ATUALIZAÇÃO: Todos os pontos recebidos atualizam datahora_atualizacao
    -- Apenas os que passaram na regra B atualizam label e datahora_identificacao
    -- =========================================================================
    dados_para_upsert AS (
        SELECT
            pts.ordem,
            pts.linha,
            rb.sentido AS label_ultima_passagem,
            pts.datahora AS datahora_atualizacao,
            rb.timestamp_identificacao AS datahora_identificacao
        FROM pts
        LEFT JOIN regra_b rb ON rb.ordem = pts.ordem AND rb.linha = pts.linha
    )
    
    INSERT INTO gps_ultima_passagem (
        ordem,
        linha,
        label_ultima_passagem,
        datahora_atualizacao,
        datahora_identificacao
    )
    SELECT
        d.ordem,
        d.linha,
        d.label_ultima_passagem,
        d.datahora_atualizacao,
        d.datahora_identificacao
    FROM dados_para_upsert d
    ON CONFLICT (ordem, linha) DO UPDATE SET
        datahora_atualizacao = EXCLUDED.datahora_atualizacao,
        -- Só atualiza label e datahora_identificacao se houver nova identificação
        label_ultima_passagem = COALESCE(EXCLUDED.label_ultima_passagem, gps_ultima_passagem.label_ultima_passagem),
        datahora_identificacao = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL THEN EXCLUDED.datahora_identificacao
            ELSE gps_ultima_passagem.datahora_identificacao
        END;
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
            (r.value->>'datahora')::timestamp AS datahora,
            (r.value->>'metodo_inferencia')::text AS metodo_inferencia
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
            AND LOWER(TRIM(gs.sentido)) <> LOWER(TRIM(n.sentido))
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
            m.sentido          AS nome_terminal_destino,
            m.metodo_inferencia AS metodo_inferencia_destino
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
        nome_terminal_destino = uv.nome_terminal_destino,
        metodo_inferencia_destino = uv.metodo_inferencia_destino
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
            (r.value->>'datahora')::timestamp AS datahora,
            (r.value->>'metodo_inferencia')::text AS metodo_inferencia
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
        metodo_inferencia_origem,
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
        m.metodo_inferencia,
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


-- -----------------------------------------------------------------------------
-- fn_insert_rio_gps_api_history
-- Insere registros brutos da API Rio GPS em batch
-- Dados salvos exatamente como chegam da API, antes de qualquer transformação
-- Usado por: rioFetcher.js -> saveRawHistory
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_insert_rio_gps_api_history(
    p_records jsonb
)
RETURNS TABLE (
    inserted_count bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted_count bigint;
BEGIN
    INSERT INTO rio_gps_api_history (
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
        (r.value->>'latitude')::text,
        (r.value->>'longitude')::text,
        (r.value->>'datahora')::bigint,
        (r.value->>'velocidade')::integer,
        (r.value->>'linha')::text,
        (r.value->>'datahoraenvio')::bigint,
        (r.value->>'datahoraservidor')::bigint
    FROM jsonb_array_elements(p_records) r;
    
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_inserted_count::bigint;
END;
$$;


-- -----------------------------------------------------------------------------
-- fn_cleanup_rio_gps_api_history
-- Remove registros de histórico bruto mais antigos que 7 dias
-- Usado por: scheduler.js -> cleanupRioGpsApiHistory job
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_cleanup_rio_gps_api_history(
    p_retention_days integer DEFAULT 7
)
RETURNS TABLE (
    deleted_count bigint,
    retention_days integer,
    cleanup_timestamp timestamp
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count bigint;
BEGIN
    DELETE FROM rio_gps_api_history
    WHERE created_at < NOW() - (p_retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    RETURN QUERY SELECT 
        v_deleted_count::bigint,
        p_retention_days::integer,
        NOW()::timestamp;
END;
$$;


-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
