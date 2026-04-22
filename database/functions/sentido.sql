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
-- Detecção em 2 etapas com prioridade:
--   1. Terminal: cluster de parada ou início de itinerário (prioridade máxima)
--   2. Score: correlação de avanço + distância à rota (fallback)
-- ============================================================================
CREATE OR REPLACE FUNCTION fn_processar_sentido_batch(
    p_records jsonb,
    p_max_distancia_rota_metros numeric DEFAULT 300,
    p_min_pontos_para_score integer DEFAULT 2,
    p_raio_inicio_itinerario_metros numeric DEFAULT 100,
    p_fallback_diff_min numeric DEFAULT 0.01,
    p_fallback_diff_max numeric DEFAULT 0.2
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

-- ============================================================================
-- ETAPA 1: PARSING DOS REGISTROS DE ENTRADA
-- ============================================================================

registros_entrada AS (
    SELECT
        (r.value->>'ordem')::text AS ordem,
        (r.value->>'linha')::text AS linha,
        (r.value->>'datahora')::timestamp with time zone AS datahora,
        (r.value->>'latitude')::double precision AS latitude,
        (r.value->>'longitude')::double precision AS longitude,
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

-- ============================================================================
-- ETAPA 2: HISTÓRICO DE POSIÇÕES GPS (ÚLTIMOS PONTOS EM 15 MIN)
-- ============================================================================

historico_gps AS (
    SELECT
        gup.ordem,
        gup.linha,
        gup.latitude,
        gup.longitude,
        gup.geom,
        ROW_NUMBER() OVER (PARTITION BY gup.ordem, gup.linha ORDER BY gup.datahora DESC) AS seq_posicao
    FROM gps_ultimas_posicoes gup
    INNER JOIN registros_entrada reg ON reg.ordem = gup.ordem AND reg.linha = gup.linha
    WHERE gup.geom IS NOT NULL
      AND gup.datahora >= reg.datahora - INTERVAL '15 minutes'
),

-- ============================================================================
-- ETAPA 3: DETECÇÃO POR TERMINAL (PRIORIDADE MÁXIMA)
-- Verifica se o ponto atual OU algum ponto histórico recente está:
--   a) Dentro de um cluster de terminal
--   b) A menos de p_raio_inicio_itinerario_metros do início de um itinerário
-- Ponto atual tem prioridade sobre histórico; cluster sobre início de itinerário.
-- ============================================================================

-- Une ponto atual + pontos históricos para verificar terminal
pontos_terminal_check AS (
    SELECT ordem, linha, geom, 0 AS prioridade_ponto FROM registros_entrada
    UNION ALL
    SELECT ordem, linha, geom, seq_posicao AS prioridade_ponto FROM historico_gps
),

deteccao_terminal AS (
    SELECT DISTINCT ON (ptc.ordem, ptc.linha)
        ptc.ordem,
        ptc.linha,
        COALESCE(cpr.sentido, it.sentido) AS sentido_terminal,
        COALESCE(cpr.itinerario_id, it.id) AS itinerario_id_terminal,
        COALESCE(it_cluster.route_name, it.route_name) AS route_name_terminal,
        CASE 
            WHEN cpr.cluster_unique_id IS NOT NULL THEN 'cluster_terminal'
            WHEN it.id IS NOT NULL THEN 'inicio_itinerario'
        END AS tipo_deteccao_terminal,
        CASE 
            WHEN cpr.cluster_unique_id IS NOT NULL 
                THEN ST_Distance(ptc.geom::geography, cpr.geom_cluster)::numeric
            ELSE ST_Distance(ptc.geom::geography, ST_StartPoint(it.the_geom)::geography)::numeric
        END AS dist_terminal_metros,
        cpr.cluster_unique_id,
        cpr.max_distance_metros AS raio_cluster_metros,
        CASE 
            WHEN cpr.cluster_unique_id IS NOT NULL 
                THEN ROUND(cpr.lat_cluster, 6)
            WHEN it.the_geom IS NOT NULL 
                THEN ROUND(ST_Y(ST_StartPoint(it.the_geom))::numeric, 6)
            ELSE NULL
        END AS terminal_latitude,
        CASE 
            WHEN cpr.cluster_unique_id IS NOT NULL
                THEN ROUND(cpr.lon_cluster, 6)
            WHEN it.the_geom IS NOT NULL 
                THEN ROUND(ST_X(ST_StartPoint(it.the_geom))::numeric, 6)
            ELSE NULL
        END AS terminal_longitude
    FROM pontos_terminal_check ptc
    LEFT JOIN clusters_parada_resultado cpr 
        ON cpr.linha_analisada = ptc.linha
        AND cpr.tipo_cluster = 'Terminal'
        AND cpr.sentido IS NOT NULL
        AND ST_DWithin(ptc.geom::geography, cpr.geom_cluster, cpr.max_distance_metros)
    -- Busca route_name do itinerário associado ao cluster
    LEFT JOIN public.itinerario it_cluster
        ON it_cluster.id = cpr.itinerario_id
    -- Fallback: início do itinerário (só se não achou cluster)
    LEFT JOIN public.itinerario it
        ON it.numero_linha = ptc.linha
        AND it.habilitado = true
        AND cpr.cluster_unique_id IS NULL
        AND ST_DWithin(
            ptc.geom::geography,
            ST_StartPoint(it.the_geom)::geography,
            p_raio_inicio_itinerario_metros
        )
    WHERE cpr.cluster_unique_id IS NOT NULL OR it.id IS NOT NULL
    ORDER BY ptc.ordem, ptc.linha,
        -- Ponto atual (0) tem prioridade sobre históricos (1..N)
        ptc.prioridade_ponto,
        -- Cluster tem prioridade sobre início de itinerário
        CASE WHEN cpr.cluster_unique_id IS NOT NULL THEN 0 ELSE 1 END,
        -- Menor distância
        CASE 
            WHEN cpr.cluster_unique_id IS NOT NULL 
                THEN ST_Distance(ptc.geom::geography, cpr.geom_cluster)
            ELSE ST_Distance(ptc.geom::geography, ST_StartPoint(it.the_geom)::geography)
        END ASC
),

-- ============================================================================
-- ETAPA 4: MÉTRICAS POR SENTIDO CANDIDATO
-- Para cada ônibus × sentido, calcula:
--   - Distância média dos pontos GPS à linestring do itinerário
--   - Desvio padrão dessa distância (consistência)
--   - Correlação entre avanço temporal e progresso na rota
-- ============================================================================

metricas_por_sentido AS (
    SELECT
        gps.ordem,
        gps.linha,
        it.id AS itinerario_id,
        it.sentido,
        it.route_name,
        COUNT(*) AS num_pontos,
        AVG(ST_Distance(gps.geom::geography, it.the_geom::geography))::numeric AS dist_media_metros,
        COALESCE(STDDEV(ST_Distance(gps.geom::geography, it.the_geom::geography)), 0)::numeric AS dist_stddev_metros,
        -- Correlação: 1.0 = avança na rota, -1.0 = contrário, 0 = sem relação
        COALESCE(
            CORR(
                (-gps.seq_posicao)::numeric,
                ST_LineLocatePoint(it.the_geom, gps.geom)::numeric
            ),
            0
        )::numeric AS corr_avanco
    FROM historico_gps gps
    INNER JOIN public.itinerario it 
        ON it.numero_linha = gps.linha AND it.habilitado = true
    GROUP BY gps.ordem, gps.linha, it.id, it.sentido, it.route_name
),

-- ============================================================================
-- ETAPA 5: SCORE DE CONFIANÇA
-- Calcula score (0..1) e elege o melhor sentido por ônibus
--   40% correlação de avanço (capped >= 0)
--   40% proximidade à rota (escalonada)
--   20% consistência de distância (escalonada)
-- ============================================================================

metricas_com_score AS (
    SELECT
        *,
        (
            GREATEST(corr_avanco, 0) * 0.4
            + CASE WHEN dist_media_metros <= 20  THEN 1.0
                   WHEN dist_media_metros <= 50  THEN 0.7
                   WHEN dist_media_metros <= 100 THEN 0.4
                   ELSE 0.1
              END * 0.4
            + CASE WHEN dist_stddev_metros <= 5  THEN 1.0
                   WHEN dist_stddev_metros <= 15 THEN 0.7
                   WHEN dist_stddev_metros <= 30 THEN 0.4
                   ELSE 0.1
              END * 0.2
        )::numeric AS score_final
    FROM metricas_por_sentido
),

melhor_score AS (
    SELECT DISTINCT ON (ordem, linha) *
    FROM metricas_com_score
    ORDER BY ordem, linha, score_final DESC, dist_media_metros ASC, corr_avanco DESC, itinerario_id ASC
),

-- ============================================================================
-- ETAPA 5b: DIFERENÇA ENTRE OS 2 MELHORES SCORES
-- Se diff < 0.2 → scores ambíguos, usar fallback da ultima_passagem
-- ============================================================================

scores_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ordem, linha
            ORDER BY score_final DESC, dist_media_metros ASC, corr_avanco DESC, itinerario_id ASC
        ) AS rn
    FROM metricas_com_score
),

score_diferenca AS (
    SELECT
        s1.ordem,
        s1.linha,
        s1.score_final - COALESCE(s2.score_final, 0) AS diff_scores
    FROM scores_ranked s1
    LEFT JOIN scores_ranked s2
        ON s1.ordem = s2.ordem AND s1.linha = s2.linha AND s2.rn = 2
    WHERE s1.rn = 1
),

-- ============================================================================
-- ETAPA 5c: FALLBACK DA ÚLTIMA PASSAGEM (gps_ultima_passagem)
-- Válido se: atualização <= 15min E identificação <= 5h
-- Usado quando score diff < 0.2 (ambiguidade entre sentidos)
-- ============================================================================

fallback_ultima_passagem AS (
    SELECT
        gup.ordem,
        gup.linha,
        gup.sentido,
        gup.itinerario_id,
        gup.metodo_detecao AS metodo_detecao_fallback,
        gup.label_ultima_passagem,
        gup.datahora_identificacao,
        gup.datahora_atualizacao,
        it.route_name AS route_name_fallback
    FROM gps_ultima_passagem gup
    INNER JOIN registros_entrada reg ON reg.ordem = gup.ordem AND reg.linha = gup.linha
    LEFT JOIN public.itinerario it ON it.id = gup.itinerario_id
    WHERE gup.sentido IS NOT NULL
      AND gup.label_ultima_passagem = 'Terminal'
      AND NOW() - gup.datahora_atualizacao <= INTERVAL '15 minutes'
      AND NOW() - gup.datahora_identificacao <= INTERVAL '5 hours'
),

-- ============================================================================
-- ETAPA 6: DISTÂNCIA DO PONTO ATUAL À ROTA MAIS PRÓXIMA
-- ============================================================================

distancia_ponto_atual AS (
    SELECT
        reg.ordem,
        reg.linha,
        MIN(ST_Distance(reg.geom::geography, it.the_geom::geography))::numeric AS dist_metros
    FROM registros_entrada reg
    INNER JOIN public.itinerario it 
        ON it.numero_linha = reg.linha AND it.habilitado = true
    GROUP BY reg.ordem, reg.linha
),

-- ============================================================================
-- ETAPA 7: RESULTADO CONSOLIDADO
-- Une terminal + score + distância + fallback e determina método e sentido final
-- Prioridade: em_terminal > sem_historico > poucos_pontos > garagem
--             > fallback_ultima_passagem (se diff < 0.2) > score
-- ============================================================================

resultado AS (
    SELECT
        reg.ordem,
        reg.linha,
        reg.datahora,
        reg.latitude,
        reg.longitude,
        reg.geom,
        
        -- Método de detecção
        CASE
            WHEN dt.tipo_deteccao_terminal IS NOT NULL  THEN 'em_terminal'
            WHEN sc.ordem IS NULL                       THEN 'sem_historico'
            WHEN sc.num_pontos < p_min_pontos_para_score THEN 'poucos_pontos'
            WHEN dpa.dist_metros > p_max_distancia_rota_metros THEN 'garagem_por_distancia'
            WHEN sd.diff_scores < p_fallback_diff_max AND sd.diff_scores > p_fallback_diff_min AND fup.ordem IS NOT NULL THEN 'fallback_ultima_passagem'
            ELSE 'score'
        END AS metodo_deteccao,
        
        -- Sentido determinado
        CASE
            WHEN dt.tipo_deteccao_terminal IS NOT NULL  THEN dt.sentido_terminal
            WHEN sc.ordem IS NULL                       THEN NULL
            WHEN sc.num_pontos < p_min_pontos_para_score THEN NULL
            WHEN dpa.dist_metros > p_max_distancia_rota_metros THEN 'GARAGEM'
            WHEN sd.diff_scores < p_fallback_diff_max AND sd.diff_scores > p_fallback_diff_min AND fup.ordem IS NOT NULL THEN fup.sentido
            ELSE sc.sentido
        END AS sentido_determinado,
        
        -- Itinerário final
        CASE
            WHEN dt.tipo_deteccao_terminal IS NOT NULL  THEN dt.itinerario_id_terminal
            WHEN sc.ordem IS NULL                       THEN NULL
            WHEN sc.num_pontos < p_min_pontos_para_score THEN NULL
            WHEN dpa.dist_metros > p_max_distancia_rota_metros THEN NULL
            WHEN sd.diff_scores < p_fallback_diff_max AND sd.diff_scores > p_fallback_diff_min AND fup.ordem IS NOT NULL THEN fup.itinerario_id
            ELSE sc.itinerario_id
        END AS itinerario_id_final,
        
        -- Route name (terminal > fallback > score > linha)
        COALESCE(
            CASE WHEN dt.tipo_deteccao_terminal IS NOT NULL THEN dt.route_name_terminal END,
            CASE WHEN sd.diff_scores < p_fallback_diff_max AND sd.diff_scores > p_fallback_diff_min AND fup.ordem IS NOT NULL THEN fup.route_name_fallback END,
            sc.route_name,
            reg.linha
        ) AS route_name,
        
        -- Score de confiança
        CASE
            WHEN dt.tipo_deteccao_terminal IS NOT NULL THEN 1.0::numeric
            ELSE COALESCE(sc.score_final, 0)
        END AS score_final,
        
        COALESCE(dpa.dist_metros, 0) AS dist_rota_metros,
        
        -- Campos do terminal (para metadado)
        dt.tipo_deteccao_terminal,
        dt.dist_terminal_metros,
        dt.cluster_unique_id,
        dt.raio_cluster_metros,
        dt.terminal_latitude,
        dt.terminal_longitude,
        
        -- Campos do score (para metadado)
        sc.num_pontos,
        sc.dist_media_metros,
        sc.dist_stddev_metros,
        sc.corr_avanco,
        
        -- Campos do fallback (para metadado)
        fup.sentido AS fallback_sentido,
        fup.itinerario_id AS fallback_itinerario_id,
        fup.metodo_detecao_fallback,
        fup.datahora_identificacao AS fallback_datahora_identificacao,
        sd.diff_scores
        
    FROM registros_entrada reg
    LEFT JOIN deteccao_terminal dt ON dt.ordem = reg.ordem AND dt.linha = reg.linha
    LEFT JOIN melhor_score sc ON sc.ordem = reg.ordem AND sc.linha = reg.linha
    LEFT JOIN distancia_ponto_atual dpa ON dpa.ordem = reg.ordem AND dpa.linha = reg.linha
    LEFT JOIN score_diferenca sd ON sd.ordem = reg.ordem AND sd.linha = reg.linha
    LEFT JOIN fallback_ultima_passagem fup ON fup.ordem = reg.ordem AND fup.linha = reg.linha
),

-- ============================================================================
-- ETAPA 8: AGREGAÇÃO DE PONTOS GPS (METADADO)
-- ============================================================================

pontos_gps_avaliados AS (
    SELECT
        gps.ordem,
        gps.linha,
        jsonb_agg(
            jsonb_build_object(
                'seq', gps.seq_posicao,
                'latitude', ROUND(gps.latitude::numeric, 6),
                'longitude', ROUND(gps.longitude::numeric, 6)
            ) ORDER BY gps.seq_posicao
        ) AS pontos_json
    FROM historico_gps gps
    GROUP BY gps.ordem, gps.linha
),

-- ============================================================================
-- ETAPA 9: AGREGAÇÃO DOS SENTIDOS CANDIDATOS (METADADO)
-- ============================================================================

sentidos_candidatos_agregados AS (
    SELECT
        ordem,
        linha,
        jsonb_agg(
            jsonb_build_object(
                'rank', rn,
                'sentido', sentido,
                'itinerario_id', itinerario_id,
                'route_name', route_name,
                'num_pontos', num_pontos,
                'dist_media', ROUND(dist_media_metros, 2),
                'dist_stddev', ROUND(dist_stddev_metros, 2),
                'corr_avanco', ROUND(corr_avanco, 4),
                'score', ROUND(score_final, 4)
            ) ORDER BY rn
        ) AS candidatos_json
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ordem, linha
                ORDER BY score_final DESC, dist_media_metros ASC, corr_avanco DESC, itinerario_id ASC
            ) AS rn
        FROM metricas_com_score
    ) ranked
    GROUP BY ordem, linha
)

-- ============================================================================
-- RESULTADO FINAL
-- ============================================================================

SELECT
    res.ordem,
    res.linha,
    res.sentido_determinado,
    res.itinerario_id_final,
    res.route_name,
    res.metodo_deteccao,
    ROUND(res.dist_rota_metros, 2),
    res.datahora,
    CASE WHEN res.sentido_determinado IS NULL THEN 0::numeric
         ELSE ROUND(res.score_final, 4) END,
    
    -- JSON com metadados específicos por método de detecção
    jsonb_build_object(
        'metodo_deteccao', res.metodo_deteccao,
        'ponto_atual', jsonb_build_object(
            'latitude', ROUND(res.latitude::numeric, 6),
            'longitude', ROUND(res.longitude::numeric, 6)
        ),
        'distancia_rota_metros', ROUND(res.dist_rota_metros, 2),
        'detalhes_metodo', CASE
            -- Terminal: informações da detecção por cluster/início de itinerário
            WHEN res.metodo_deteccao = 'em_terminal' THEN jsonb_build_object(
                'tipo', res.tipo_deteccao_terminal,
                'cluster_unique_id', res.cluster_unique_id,
                'distancia_terminal_metros', ROUND(COALESCE(res.dist_terminal_metros, 0), 2),
                'raio_cluster_metros', res.raio_cluster_metros,
                'terminal_latitude', res.terminal_latitude,
                'terminal_longitude', res.terminal_longitude
            )
            -- Score: métricas da análise de posições GPS
            WHEN res.metodo_deteccao = 'score' THEN jsonb_build_object(
                'num_pontos', COALESCE(res.num_pontos, 0),
                'dist_media_metros', ROUND(COALESCE(res.dist_media_metros, 0), 2),
                'dist_stddev_metros', ROUND(COALESCE(res.dist_stddev_metros, 0), 2),
                'corr_avanco', ROUND(COALESCE(res.corr_avanco, 0), 4),
                'score', ROUND(res.score_final, 4)
            )
            -- Fallback: scores ambíguos, usando última passagem em terminal
            WHEN res.metodo_deteccao = 'fallback_ultima_passagem' THEN jsonb_build_object(
                'ultima_passagem_sentido', res.fallback_sentido,
                'ultima_passagem_itinerario_id', res.fallback_itinerario_id,
                'ultima_passagem_metodo', res.metodo_detecao_fallback,
                'datahora_identificacao', res.fallback_datahora_identificacao,
                'diff_scores', ROUND(COALESCE(res.diff_scores, 0), 4),
                'score_melhor', ROUND(res.score_final, 4)
            )
            -- Garagem: ônibus longe de qualquer rota
            WHEN res.metodo_deteccao = 'garagem_por_distancia' THEN jsonb_build_object(
                'distancia_rota_metros', ROUND(res.dist_rota_metros, 2),
                'limite_metros', p_max_distancia_rota_metros
            )
            -- Poucos pontos ou sem histórico: informação mínima
            ELSE jsonb_build_object(
                'num_pontos', COALESCE(res.num_pontos, 0),
                'min_pontos_necessarios', p_min_pontos_para_score
            )
        END,
        'pontos_avaliados', COALESCE(pga.pontos_json, '[]'::jsonb),
        'sentidos_candidatos', COALESCE(sca.candidatos_json, '[]'::jsonb)
    ) AS json_pontos_avaliados

FROM resultado res
LEFT JOIN pontos_gps_avaliados pga ON pga.ordem = res.ordem AND pga.linha = res.linha
LEFT JOIN sentidos_candidatos_agregados sca ON sca.ordem = res.ordem AND sca.linha = res.linha;

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
