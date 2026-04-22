

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
-- Atualiza a tabela gps_ultima_passagem com base em:
-- - REGRA TERMINAL (via clusters): Ponto dentro do buffer de cluster "Terminal"
-- - REGRA GARAGEM: Ponto dentro do buffer de cluster "Garagem"
-- Invalidação por distância é feita em fn_processar_sentido_batch (garagem_por_distancia)
-- Usado por: rioFetcher.js -> processamento de última passagem
-- -----------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION fn_atualizar_ultima_passagem(
    p_points jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- =========================================================================
    -- Processar novas detecções de terminal/garagem
    -- A invalidação por distância à rota é feita em fn_processar_sentido_batch
    -- (garagem_por_distancia quando dist > 300m)
    -- =========================================================================
    WITH pts AS (
        SELECT
            (r.value->>'ordem')::text AS ordem,
            (r.value->>'linha')::text AS linha,
            (r.value->>'lat')::double precision AS lat,
            (r.value->>'lon')::double precision AS lon,
            (r.value->>'datahora')::timestamp with time zone AS datahora
        FROM jsonb_array_elements(p_points) r
    ),
    
    -- =========================================================================
    -- REGRA GARAGEM: Qualquer ponto dentro do buffer de cluster tipo "Garagem"
    -- =========================================================================
    regra_garagem AS (
        SELECT DISTINCT ON (pts.ordem, pts.linha)
            pts.ordem,
            pts.linha,
            'Garagem' AS label,
            NULL::text AS sentido,
            NULL::integer AS itinerario_id,
            'Cluster_Garagem' AS metodo_detecao,
            pts.lat AS lat_detecao,
            pts.lon AS lon_detecao,
            pts.datahora AS timestamp_identificacao
        FROM pts
        JOIN clusters_parada_resultado cpr
            ON cpr.linha_analisada = pts.linha
            AND cpr.tipo_cluster = 'Garagem'
            AND ST_Contains(
                cpr.geom_cluster::geometry,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)
            )
        ORDER BY pts.ordem, pts.linha, pts.datahora DESC
    ),
    
    -- =========================================================================
    -- REGRA TERMINAL VIA CLUSTERS: Ponto dentro do buffer de cluster "Terminal"
    -- Para terminais ambíguos (2+ itinerários), usa correlação de avanço
    -- =========================================================================
    
    -- 1. Identificar todos os clusters "Terminal" que o ônibus está dentro
    terminais_candidatos AS (
        SELECT DISTINCT
            pts.ordem,
            pts.linha,
            pts.lat,
            pts.lon,
            pts.datahora,
            cpr.cluster_id,
            cpr.geom_cluster
        FROM pts
        JOIN clusters_parada_resultado cpr
            ON cpr.linha_analisada = pts.linha
            AND cpr.tipo_cluster = 'Terminal'
            AND ST_Contains(
                cpr.geom_cluster::geometry,
                ST_SetSRID(ST_MakePoint(pts.lon, pts.lat), 4326)
            )
    ),
    
    -- 2. Usar sentido e itinerario_id diretamente do cluster
    regra_cluster_terminal AS (
        SELECT DISTINCT ON (tc.ordem, tc.linha)
            tc.ordem,
            tc.linha,
            'Terminal' AS label,
            cpr.sentido,
            cpr.itinerario_id,
            'Cluster_Terminal' AS metodo_detecao,
            tc.lat AS lat_detecao,
            tc.lon AS lon_detecao,
            tc.datahora AS timestamp_identificacao
        FROM terminais_candidatos tc
        JOIN clusters_parada_resultado cpr
            ON cpr.cluster_id = tc.cluster_id
            AND cpr.linha_analisada = tc.linha
        ORDER BY tc.ordem, tc.linha, tc.datahora DESC
    ),
    
    -- =========================================================================
    -- COMBINAÇÃO: Garagem tem prioridade, depois cluster terminal
    -- em_terminal = TRUE se detectou terminal/garagem, FALSE caso contrário
    -- =========================================================================
    resultado_final AS (
        SELECT
            pts.ordem,
            pts.linha,
            COALESCE(rg.label, rct.label) AS label_ultima_passagem,
            COALESCE(rg.sentido, rct.sentido) AS sentido,
            COALESCE(rg.itinerario_id, rct.itinerario_id) AS itinerario_id,
            COALESCE(rg.metodo_detecao, rct.metodo_detecao) AS metodo_detecao,
            COALESCE(rg.lat_detecao, rct.lat_detecao) AS lat_detecao,
            COALESCE(rg.lon_detecao, rct.lon_detecao) AS lon_detecao,
            pts.datahora AS datahora_atualizacao,
            COALESCE(rg.timestamp_identificacao, rct.timestamp_identificacao) AS datahora_identificacao,
            -- em_terminal: TRUE se detectou, FALSE se não detectou (saiu do terminal)
            (rg.ordem IS NOT NULL OR rct.ordem IS NOT NULL) AS em_terminal
        FROM pts
        LEFT JOIN regra_garagem rg ON rg.ordem = pts.ordem AND rg.linha = pts.linha
        LEFT JOIN regra_cluster_terminal rct ON rct.ordem = pts.ordem AND rct.linha = pts.linha
    )
    
    INSERT INTO gps_ultima_passagem (
        ordem,
        linha,
        label_ultima_passagem,
        sentido,
        itinerario_id,
        metodo_detecao,
        lat_detecao,
        lon_detecao,
        datahora_atualizacao,
        datahora_identificacao,
        em_terminal
    )
    SELECT
        rf.ordem,
        rf.linha,
        rf.label_ultima_passagem,
        rf.sentido,
        rf.itinerario_id,
        rf.metodo_detecao,
        rf.lat_detecao,
        rf.lon_detecao,
        rf.datahora_atualizacao,
        rf.datahora_identificacao,
        rf.em_terminal
    FROM resultado_final rf
    ON CONFLICT (ordem, linha) DO UPDATE SET
        -- Sempre atualiza: datahora_atualizacao e em_terminal
        datahora_atualizacao = EXCLUDED.datahora_atualizacao,
        em_terminal = EXCLUDED.em_terminal,
        -- Campos imutáveis: só atualiza se for um NOVO terminal
        -- Novo terminal = label/sentido/itinerario_id diferente do armazenado, ou registro anterior estava vazio
        label_ultima_passagem = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.label_ultima_passagem
            ELSE COALESCE(gps_ultima_passagem.label_ultima_passagem, EXCLUDED.label_ultima_passagem)
        END,
        sentido = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.sentido
            ELSE gps_ultima_passagem.sentido
        END,
        itinerario_id = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.itinerario_id
            ELSE gps_ultima_passagem.itinerario_id
        END,
        metodo_detecao = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.metodo_detecao
            ELSE gps_ultima_passagem.metodo_detecao
        END,
        lat_detecao = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.lat_detecao
            ELSE gps_ultima_passagem.lat_detecao
        END,
        lon_detecao = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.lon_detecao
            ELSE gps_ultima_passagem.lon_detecao
        END,
        datahora_identificacao = CASE 
            WHEN EXCLUDED.label_ultima_passagem IS NOT NULL AND (
                gps_ultima_passagem.label_ultima_passagem IS NULL
                OR gps_ultima_passagem.itinerario_id IS DISTINCT FROM EXCLUDED.itinerario_id
                OR gps_ultima_passagem.sentido IS DISTINCT FROM EXCLUDED.sentido
            ) THEN EXCLUDED.datahora_identificacao
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
-- GARAGEM é tratado como sentido válido (fecha viagem anterior, abre nova)
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
            (r.value->>'metodo_inferencia')::text AS metodo_inferencia,
            (r.value->'metadados')::jsonb AS metadados
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
        LEFT JOIN public.itinerario i_ant
          ON i_ant.id = gs.sentido_itinerario_id
        LEFT JOIN public.itinerario i_novo
          ON i_novo.id = n.itinerario_id
        WHERE
            gs.sentido IS NOT NULL
            AND n.sentido IS NOT NULL
            AND LOWER(TRIM(gs.sentido)) <> LOWER(TRIM(n.sentido))
            -- Mudança válida se:
            -- 1. Um dos dois é GARAGEM → sempre válido
            -- 2. Nenhum é GARAGEM → itinerários diferentes com pontos de partida diferentes
            AND (
                UPPER(gs.sentido) = 'GARAGEM'
                OR UPPER(n.sentido) = 'GARAGEM'
                OR (
                    gs.sentido_itinerario_id <> n.itinerario_id
                    AND NOT ST_Equals(
                        ST_StartPoint(i_ant.the_geom),
                        ST_StartPoint(i_novo.the_geom)
                    )
                )
            )
    ),
    ultima_viagem AS (
        SELECT DISTINCT ON (hv.ordem, hv.token, hv.linha)
            hv.id,
            m.datahora,
            m.itinerario_id    AS itinerario_destino,
            m.sentido          AS nome_terminal_destino,
            m.metodo_inferencia AS metodo_inferencia_destino,
            m.metadados        AS metadados_destino
        FROM mudanca m
        JOIN gps_historico_viagens hv
        ON hv.ordem = m.ordem
        AND hv.token = m.token
        AND hv.linha = m.linha
        AND hv.timestamp_fim IS NULL
        AND m.datahora > hv.timestamp_inicio
        ORDER BY
            hv.ordem,
            hv.token,
            hv.linha,
            hv.timestamp_inicio DESC,
            m.datahora DESC
    )
    UPDATE gps_historico_viagens hv
    SET
        timestamp_fim = uv.datahora,
        duracao_viagem = uv.datahora - hv.timestamp_inicio,
        itinerario_id_destino = uv.itinerario_destino,
        nome_terminal_destino = uv.nome_terminal_destino,
        metodo_inferencia_destino = uv.metodo_inferencia_destino,
        metadados_destino = uv.metadados_destino
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
            (r.value->>'metodo_inferencia')::text AS metodo_inferencia,
            (r.value->'metadados')::jsonb AS metadados
        FROM jsonb_array_elements(p_records) r
    ),
    mudanca AS (
        SELECT
            n.*
        FROM novos n
        JOIN gps_sentido gs
          ON gs.ordem = n.ordem
         AND gs.token = n.token
        LEFT JOIN public.itinerario i_ant
          ON i_ant.id = gs.sentido_itinerario_id
        LEFT JOIN public.itinerario i_novo
          ON i_novo.id = n.itinerario_id
        WHERE
            gs.sentido IS NOT NULL
            AND n.sentido IS NOT NULL
            AND LOWER(TRIM(gs.sentido)) <> LOWER(TRIM(n.sentido))
            -- Mudança válida se:
            -- 1. Um dos dois é GARAGEM → sempre válido
            -- 2. Nenhum é GARAGEM → itinerários diferentes com pontos de partida diferentes
            AND (
                UPPER(gs.sentido) = 'GARAGEM'
                OR UPPER(n.sentido) = 'GARAGEM'
                OR (
                    gs.sentido_itinerario_id <> n.itinerario_id
                    AND NOT ST_Equals(
                        ST_StartPoint(i_ant.the_geom),
                        ST_StartPoint(i_novo.the_geom)
                    )
                )
            )
    )
    INSERT INTO gps_historico_viagens (
        ordem,
        token,
        linha,
        itinerario_id_origem,
        nome_terminal_origem,
        metodo_inferencia_origem,
        metadados_origem,
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
        m.metadados,
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
-- NOTA: Viagens são processadas separadamente pelo backend (processarViagensRio)
-- Usado por: rio.js -> saveRioToGpsSentido (batch)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_upsert_gps_sentido_rio_batch_json(
    p_records jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Upsert normal dos registros GPS
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
