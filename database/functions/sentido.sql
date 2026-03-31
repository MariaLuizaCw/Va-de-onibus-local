-- =============================================================================
-- FUNÇÕES PARA DETECÇÃO DE SENTIDO DE ÔNIBUS
-- Sistema de detecção em 2 etapas: ultima_passagem + fallback por histórico
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fn_atualizar_ultimas_posicoes
-- Atualiza a tabela auxiliar de últimas 5 posições por ônibus/linha
-- Recebe JSON array com posições GPS e mantém apenas as 5 mais recentes distintas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_atualizar_ultimas_posicoes(
    p_records jsonb,
    p_precisao_decimal integer DEFAULT 4  -- 4 casas = ~10m de precisão
)
RETURNS TABLE (
    registros_inseridos bigint,
    registros_removidos bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inseridos bigint := 0;
    v_removidos bigint := 0;
BEGIN
    -- 1. Inserir novas posições (ignorando duplicatas e posições muito próximas)
    -- Usa arredondamento para considerar posições próximas como iguais
    -- Ex: -22.99499 e -22.99498 com 4 casas → ambas viram -22.9950 (iguais)
    INSERT INTO gps_ultimas_posicoes (ordem, linha, datahora, latitude, longitude, velocidade)
    SELECT
        (r.value->>'ordem')::text,
        (r.value->>'linha')::text,
        (r.value->>'datahora')::timestamp with time zone,
        (r.value->>'latitude')::double precision,
        (r.value->>'longitude')::double precision,
        (r.value->>'velocidade')::double precision
    FROM jsonb_array_elements(p_records) r
    WHERE (r.value->>'ordem') IS NOT NULL
      AND (r.value->>'linha') IS NOT NULL
      AND (r.value->>'datahora') IS NOT NULL
      AND (r.value->>'latitude') IS NOT NULL
      AND (r.value->>'longitude') IS NOT NULL
      -- Filtrar posições que já existem com coordenadas muito próximas
      AND NOT EXISTS (
          SELECT 1 FROM gps_ultimas_posicoes gup
          WHERE gup.ordem = (r.value->>'ordem')::text
            AND gup.linha = (r.value->>'linha')::text
            AND ROUND(gup.latitude::numeric, p_precisao_decimal) = ROUND((r.value->>'latitude')::numeric, p_precisao_decimal)
            AND ROUND(gup.longitude::numeric, p_precisao_decimal) = ROUND((r.value->>'longitude')::numeric, p_precisao_decimal)
      )
    ON CONFLICT (ordem, linha, datahora, latitude, longitude) DO NOTHING;
    
    GET DIAGNOSTICS v_inseridos = ROW_COUNT;
    
    -- 2. Remover posições antigas, mantendo apenas as 5 mais recentes DISTINTAS por ordem+linha
    -- Agrupa por coordenadas arredondadas para garantir distinção espacial
    WITH ranked AS (
        SELECT 
            id,
            ROW_NUMBER() OVER (
                PARTITION BY ordem, linha 
                ORDER BY datahora DESC
            ) AS rn
        FROM (
            SELECT DISTINCT ON (
                ordem, 
                linha, 
                ROUND(latitude::numeric, p_precisao_decimal), 
                ROUND(longitude::numeric, p_precisao_decimal)
            )
                id, ordem, linha, datahora, latitude, longitude
            FROM gps_ultimas_posicoes
            ORDER BY ordem, linha, 
                ROUND(latitude::numeric, p_precisao_decimal), 
                ROUND(longitude::numeric, p_precisao_decimal),
                datahora DESC
        ) AS posicoes_distintas
    ),
    ids_to_keep AS (
        SELECT id FROM ranked WHERE rn <= 5
    )
    DELETE FROM gps_ultimas_posicoes gup
    WHERE EXISTS (
        SELECT 1 FROM gps_ultimas_posicoes g2
        WHERE g2.ordem = gup.ordem AND g2.linha = gup.linha
    )
    AND gup.id NOT IN (SELECT id FROM ids_to_keep);
    
    GET DIAGNOSTICS v_removidos = ROW_COUNT;
    
    RETURN QUERY SELECT v_inseridos, v_removidos;
END;
$$;


-- ============================================================================
-- fn_processar_sentido_batch
-- Processa sentido em batch para múltiplos ônibus
-- VERSÃO SIMPLIFICADA: Usa APENAS score baseado nas últimas posições GPS
-- Sem dependência de última passagem em terminal
-- ============================================================================

CREATE OR REPLACE FUNCTION fn_processar_sentido_batch(
    p_records jsonb,
    p_max_distancia_rota_metros numeric DEFAULT 200,
    p_min_pontos_para_score integer DEFAULT 2
) RETURNS TABLE (
    ordem text,
    linha text,
    sentido text,
    itinerario_id integer,
    route_name text,
    metodo_detecao text,
    distancia_atual_rota_metros numeric,
    datahora_identificacao timestamp with time zone,
    score_confianca numeric,
    json_pontos_avaliados jsonb
) LANGUAGE sql AS $$
WITH

-- 1. Parse dos registros de entrada
registros AS (
    SELECT
        (r.value->>'ordem')::text AS ordem,
        (r.value->>'linha')::text AS linha,
        (r.value->>'datahora')::timestamp with time zone AS datahora,
        ST_SetSRID(
            ST_MakePoint(
                (r.value->>'longitude')::double precision,
                (r.value->>'latitude')::double precision
            ),
            0
        ) AS geom
    FROM jsonb_array_elements(p_records) r
    WHERE (r.value->>'ordem') IS NOT NULL
        AND (r.value->>'linha') IS NOT NULL
        AND (r.value->>'latitude') IS NOT NULL
        AND (r.value->>'longitude') IS NOT NULL
),

-- 2. Últimas posições com número sequencial
ultimas_posicoes AS (
    SELECT
        gup.ordem,
        gup.linha,
        gup.geom,
        ROW_NUMBER() OVER (PARTITION BY gup.ordem, gup.linha ORDER BY gup.datahora DESC) AS rn
    FROM gps_ultimas_posicoes gup
    INNER JOIN registros reg ON reg.ordem = gup.ordem AND reg.linha = gup.linha
),

-- 3. Calcular scores em uma única agregação
scores AS (
    SELECT
        up.ordem,
        up.linha,
        i.id AS itinerario_id,
        i.sentido,
        i.route_name,
        COUNT(*) AS num_pontos,
        AVG(ST_Distance(up.geom::geography, i.the_geom::geography))::numeric AS dist_media,
        COALESCE(STDDEV(ST_Distance(up.geom::geography, i.the_geom::geography)), 0)::numeric AS dist_stddev,
        COALESCE(CORR((-up.rn)::numeric, ST_LineLocatePoint(i.the_geom, up.geom)::numeric), 0)::numeric AS corr_avanco,
        ROW_NUMBER() OVER (
            PARTITION BY up.ordem, up.linha
            ORDER BY 
                -- Score final direto aqui
                (GREATEST(COALESCE(CORR((-up.rn)::numeric, ST_LineLocatePoint(i.the_geom, up.geom)::numeric), 0), 0) * 0.4
                + CASE WHEN AVG(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 20 THEN 1.0
                       WHEN AVG(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 50 THEN 0.7
                       WHEN AVG(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 100 THEN 0.4
                       ELSE 0.1 END * 0.4
                + CASE WHEN STDDEV(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 5 THEN 1.0
                       WHEN STDDEV(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 15 THEN 0.7
                       WHEN STDDEV(ST_Distance(up.geom::geography, i.the_geom::geography)) <= 30 THEN 0.4
                       ELSE 0.1 END * 0.2) DESC,
                AVG(ST_Distance(up.geom::geography, i.the_geom::geography)) ASC
        ) AS rank
    FROM ultimas_posicoes up
    INNER JOIN public.itinerario i ON i.numero_linha = up.linha AND i.habilitado = true
    GROUP BY up.ordem, up.linha, i.id, i.sentido, i.route_name
),

-- 4. Distância do ponto atual mais próximo
dist_atual AS (
    SELECT
        reg.ordem,
        reg.linha,
        MIN(ST_Distance(reg.geom::geography, i.the_geom::geography))::numeric AS dist_metros
    FROM registros reg
    INNER JOIN public.itinerario i ON i.numero_linha = reg.linha AND i.habilitado = true
    GROUP BY reg.ordem, reg.linha
),

-- 5. Apenas o melhor sentido
melhor_sentido AS (
    SELECT * FROM scores WHERE rank = 1
),

-- 6. Agregar pontos avaliados por ordem/linha
pontos_avaliados AS (
    SELECT
        up.ordem,
        up.linha,
        jsonb_agg(
            jsonb_build_object(
                'seq', up.rn,
                'latitude', ROUND(ST_Y(up.geom)::numeric, 6),
                'longitude', ROUND(ST_X(up.geom)::numeric, 6)
            ) ORDER BY up.rn
        ) AS pontos
    FROM ultimas_posicoes up
    GROUP BY up.ordem, up.linha
),

-- 7. Agregar métricas de todos os sentidos candidatos
metricas_por_sentido AS (
    SELECT
        s.ordem,
        s.linha,
        jsonb_agg(
            jsonb_build_object(
                'sentido', s.sentido,
                'itinerario_id', s.itinerario_id,
                'route_name', s.route_name,
                'num_pontos', s.num_pontos,
                'dist_media', ROUND(s.dist_media, 2),
                'dist_stddev', ROUND(s.dist_stddev, 2),
                'corr_avanco', ROUND(s.corr_avanco, 4),
                'score', ROUND((
                    GREATEST(s.corr_avanco, 0) * 0.4
                    + CASE WHEN s.dist_media <= 20 THEN 1.0
                           WHEN s.dist_media <= 50 THEN 0.7
                           WHEN s.dist_media <= 100 THEN 0.4
                           ELSE 0.1 END * 0.4
                    + CASE WHEN s.dist_stddev <= 5 THEN 1.0
                           WHEN s.dist_stddev <= 15 THEN 0.7
                           WHEN s.dist_stddev <= 30 THEN 0.4
                           ELSE 0.1 END * 0.2
                )::numeric, 4),
                'rank', s.rank
            ) ORDER BY s.rank
        ) AS sentidos
    FROM scores s
    GROUP BY s.ordem, s.linha
)

-- RESULTADO
SELECT
    reg.ordem,
    reg.linha,
    
    -- Lógica de sentido em um CASE simples
    CASE
        WHEN ms.ordem IS NULL THEN NULL
        WHEN ms.num_pontos < p_min_pontos_para_score THEN NULL
        WHEN d.dist_metros > p_max_distancia_rota_metros THEN 'GARAGEM'::text
        ELSE ms.sentido
    END,
    
    CASE WHEN d.dist_metros <= p_max_distancia_rota_metros AND ms.num_pontos >= p_min_pontos_para_score 
         THEN ms.itinerario_id ELSE NULL END,
    
    COALESCE(ms.route_name, reg.linha),
    
    CASE
        WHEN ms.ordem IS NULL THEN 'sem_historico'::text
        WHEN ms.num_pontos < p_min_pontos_para_score THEN 'poucos_pontos'::text
        WHEN d.dist_metros > p_max_distancia_rota_metros THEN 'garagem_por_distancia'::text
        ELSE 'score'::text
    END,
    
    ROUND(COALESCE(d.dist_metros, 0), 2),
    reg.datahora,
    CASE WHEN ms.ordem IS NULL THEN 0::numeric
    ELSE ROUND((
        GREATEST(ms.corr_avanco, 0) * 0.4
        + CASE WHEN ms.dist_media <= 20 THEN 1.0
               WHEN ms.dist_media <= 50 THEN 0.7
               WHEN ms.dist_media <= 100 THEN 0.4
               ELSE 0.1 END * 0.4
        + CASE WHEN ms.dist_stddev <= 5 THEN 1.0
               WHEN ms.dist_stddev <= 15 THEN 0.7
               WHEN ms.dist_stddev <= 30 THEN 0.4
               ELSE 0.1 END * 0.2
    )::numeric, 4) END,
    
    jsonb_build_object(
        'metodo_detecao', CASE
            WHEN ms.ordem IS NULL THEN 'sem_historico'
            WHEN ms.num_pontos < p_min_pontos_para_score THEN 'poucos_pontos'
            WHEN d.dist_metros > p_max_distancia_rota_metros THEN 'garagem_por_distancia'
            ELSE 'score'
        END,
        'ponto_atual', jsonb_build_object(
            'latitude', ROUND(ST_Y(reg.geom)::numeric, 6),
            'longitude', ROUND(ST_X(reg.geom)::numeric, 6)
        ),
        'distancia_ponto_atual_metros', ROUND(COALESCE(d.dist_metros, 0), 2),
        'score_total', CASE WHEN ms.ordem IS NULL THEN 0::numeric
            ELSE ROUND((
                GREATEST(ms.corr_avanco, 0) * 0.4
                + CASE WHEN ms.dist_media <= 20 THEN 1.0
                       WHEN ms.dist_media <= 50 THEN 0.7
                       WHEN ms.dist_media <= 100 THEN 0.4
                       ELSE 0.1 END * 0.4
                + CASE WHEN ms.dist_stddev <= 5 THEN 1.0
                       WHEN ms.dist_stddev <= 15 THEN 0.7
                       WHEN ms.dist_stddev <= 30 THEN 0.4
                       ELSE 0.1 END * 0.2
            )::numeric, 4) END,
        'metricas_sentido_escolhido', jsonb_build_object(
            'sentido', ms.sentido,
            'itinerario_id', ms.itinerario_id,
            'num_pontos', COALESCE(ms.num_pontos, 0),
            'dist_media', ROUND(COALESCE(ms.dist_media, 0), 2),
            'dist_stddev', ROUND(COALESCE(ms.dist_stddev, 0), 2),
            'corr_avanco', ROUND(COALESCE(ms.corr_avanco, 0), 4)
        ),
        'pontos_avaliados', COALESCE(pa.pontos, '[]'::jsonb),
        'sentidos_candidatos', COALESCE(mps.sentidos, '[]'::jsonb)
    )

FROM registros reg
LEFT JOIN melhor_sentido ms ON ms.ordem = reg.ordem AND ms.linha = reg.linha
LEFT JOIN dist_atual d ON d.ordem = reg.ordem AND d.linha = reg.linha
LEFT JOIN pontos_avaliados pa ON pa.ordem = reg.ordem AND pa.linha = reg.linha
LEFT JOIN metricas_por_sentido mps ON mps.ordem = reg.ordem AND mps.linha = reg.linha;

$$;

-- -----------------------------------------------------------------------------
-- fn_upsert_gps_sentido_batch
-- Faz upsert em gps_sentido recebendo registros JÁ ENRIQUECIDOS
-- Recebe JSON array com: ordem, linha, datahora, latitude, longitude, velocidade,
--                        sentido, sentido_itinerario_id, route_name, token, metodo_detecao
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_upsert_gps_sentido_batch(p_records jsonb)
RETURNS TABLE (
    registros_processados bigint
)
LANGUAGE sql
AS $$
WITH dados AS (
    SELECT
        (r.value->>'ordem')::text AS ordem,
        (r.value->>'datahora')::timestamp AS datahora,
        (r.value->>'linha')::text AS linha,
        (r.value->>'latitude')::double precision AS latitude,
        (r.value->>'longitude')::double precision AS longitude,
        (r.value->>'velocidade')::double precision AS velocidade,
        (r.value->>'sentido')::text AS sentido,
        (r.value->>'sentido_itinerario_id')::integer AS sentido_itinerario_id,
        (r.value->>'route_name')::text AS route_name,
        (r.value->>'token')::text AS token
    FROM jsonb_array_elements(p_records) r
),
upserted AS (
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
        d.ordem,
        d.datahora,
        d.linha,
        d.latitude,
        d.longitude,
        d.velocidade,
        d.sentido,
        d.sentido_itinerario_id,
        d.route_name,
        d.token
    FROM dados d
    ON CONFLICT (ordem, token) DO UPDATE SET
        datahora = EXCLUDED.datahora,
        linha = EXCLUDED.linha,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        velocidade = EXCLUDED.velocidade,
        sentido = EXCLUDED.sentido,
        sentido_itinerario_id = EXCLUDED.sentido_itinerario_id,
        route_name = EXCLUDED.route_name
    WHERE gps_sentido.datahora IS NULL OR EXCLUDED.datahora > gps_sentido.datahora
    RETURNING 1
)
SELECT COUNT(*)::bigint FROM upserted;
$$;


-- -----------------------------------------------------------------------------
-- fn_cleanup_ultimas_posicoes
-- Remove registros antigos da tabela auxiliar
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_cleanup_ultimas_posicoes(
    p_retention_hours integer DEFAULT 2
)
RETURNS TABLE (
    deleted_count bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted bigint;
BEGIN
    DELETE FROM gps_ultimas_posicoes
    WHERE created_at < NOW() - (p_retention_hours || ' hours')::INTERVAL;
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    RETURN QUERY SELECT v_deleted;
END;
$$;


-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
