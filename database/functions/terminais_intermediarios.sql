-- =============================================================================
-- FUNÇÕES PARA DETECÇÃO DE TERMINAIS INTERMEDIÁRIOS
-- Identifica terminais que aparecem no meio de rotas (não como ponto inicial/final)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fn_detectar_terminais_intermediarios
-- Analisa todos os itinerários habilitados e clusters de terminal
-- Identifica quando um terminal está no meio da rota (posição 0.05..0.95)
-- e não próximo do início ou fim
--
-- Parâmetros:
--   p_raio_busca_metros: raio para considerar que o terminal está "na rota"
--   p_margem_inicio_fim: fração da rota considerada como início/fim (ex: 0.05 = 5%)
--   p_truncate: se TRUE, limpa a tabela antes de inserir
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_detectar_terminais_intermediarios(
    p_raio_busca_metros numeric DEFAULT 200,
    p_margem_inicio_fim numeric DEFAULT 0.05,
    p_truncate boolean DEFAULT TRUE
)
RETURNS TABLE (
    total_encontrados bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total bigint;
BEGIN
    -- Limpar tabela se solicitado
    IF p_truncate THEN
        TRUNCATE itinerario_terminal_intermediario;
    END IF;

    -- =========================================================================
    -- PARTE 1: Clusters de terminal no meio de rotas
    -- Critérios:
    --   1. Cluster está a menos de p_raio_busca_metros da linestring
    --   2. Posição na rota entre p_margem e (1 - p_margem) → NÃO é início/fim
    --   3. Cluster pertence à mesma linha do itinerário
    --   4. Cluster NÃO é o terminal associado ao itinerário
    -- =========================================================================
    INSERT INTO itinerario_terminal_intermediario (
        itinerario_id,
        numero_linha,
        sentido,
        tipo_origem,
        cluster_unique_id,
        cluster_id,
        linha_cluster,
        posicao_na_rota,
        distancia_rota_metros,
        distancia_inicio_metros,
        distancia_fim_metros,
        lat_terminal,
        lon_terminal
    )
    SELECT
        it.id AS itinerario_id,
        it.numero_linha,
        it.sentido,
        'cluster_terminal' AS tipo_origem,
        cpr.cluster_unique_id,
        cpr.cluster_id,
        cpr.linha_analisada AS linha_cluster,
        ST_LineLocatePoint(
            it.the_geom,
            ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)
        )::numeric AS posicao_na_rota,
        ST_Distance(
            ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)::geography,
            it.the_geom::geography
        )::numeric AS distancia_rota_metros,
        ST_Distance(
            ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)::geography,
            ST_StartPoint(it.the_geom)::geography
        )::numeric AS distancia_inicio_metros,
        ST_Distance(
            ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)::geography,
            ST_EndPoint(it.the_geom)::geography
        )::numeric AS distancia_fim_metros,
        cpr.lat_cluster,
        cpr.lon_cluster
    FROM public.itinerario it
    CROSS JOIN clusters_parada_resultado cpr
    WHERE it.habilitado = true
      AND cpr.tipo_cluster = 'Terminal'
      AND cpr.sentido IS NOT NULL
      AND cpr.linha_analisada = it.numero_linha
      AND ST_Distance(
          ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)::geography,
          it.the_geom::geography
      ) <= p_raio_busca_metros
      AND ST_LineLocatePoint(
          it.the_geom,
          ST_SetSRID(ST_MakePoint(cpr.lon_cluster, cpr.lat_cluster), 4326)
      ) BETWEEN p_margem_inicio_fim AND (1.0 - p_margem_inicio_fim)
      AND cpr.itinerario_id IS DISTINCT FROM it.id
    ON CONFLICT (itinerario_id, tipo_origem, cluster_unique_id) DO UPDATE SET
        posicao_na_rota = EXCLUDED.posicao_na_rota,
        distancia_rota_metros = EXCLUDED.distancia_rota_metros,
        distancia_inicio_metros = EXCLUDED.distancia_inicio_metros,
        distancia_fim_metros = EXCLUDED.distancia_fim_metros,
        data_analise = NOW();

    GET DIAGNOSTICS v_total = ROW_COUNT;

    -- =========================================================================
    -- PARTE 2: Início de outro itinerário no meio desta rota
    -- Detecta quando o ST_StartPoint de itinerário B (mesma linha)
    -- cai no meio da rota do itinerário A
    -- Critérios:
    --   1. Mesma linha, itinerários diferentes
    --   2. StartPoint de B está a menos de p_raio_busca_metros da linestring de A
    --   3. Posição na rota A está entre p_margem e (1 - p_margem)
    -- =========================================================================
    INSERT INTO itinerario_terminal_intermediario (
        itinerario_id,
        numero_linha,
        sentido,
        tipo_origem,
        itinerario_id_origem,
        sentido_origem,
        posicao_na_rota,
        distancia_rota_metros,
        distancia_inicio_metros,
        distancia_fim_metros,
        lat_terminal,
        lon_terminal
    )
    SELECT
        it_afetado.id AS itinerario_id,
        it_afetado.numero_linha,
        it_afetado.sentido,
        'inicio_itinerario' AS tipo_origem,
        it_origem.id AS itinerario_id_origem,
        it_origem.sentido AS sentido_origem,
        ST_LineLocatePoint(
            it_afetado.the_geom,
            ST_StartPoint(it_origem.the_geom)
        )::numeric AS posicao_na_rota,
        ST_Distance(
            ST_StartPoint(it_origem.the_geom)::geography,
            it_afetado.the_geom::geography
        )::numeric AS distancia_rota_metros,
        ST_Distance(
            ST_StartPoint(it_origem.the_geom)::geography,
            ST_StartPoint(it_afetado.the_geom)::geography
        )::numeric AS distancia_inicio_metros,
        ST_Distance(
            ST_StartPoint(it_origem.the_geom)::geography,
            ST_EndPoint(it_afetado.the_geom)::geography
        )::numeric AS distancia_fim_metros,
        ROUND(ST_Y(ST_StartPoint(it_origem.the_geom))::numeric, 6),
        ROUND(ST_X(ST_StartPoint(it_origem.the_geom))::numeric, 6)
    FROM public.itinerario it_afetado
    INNER JOIN public.itinerario it_origem
        ON it_origem.numero_linha = it_afetado.numero_linha
        AND it_origem.habilitado = true
        AND it_origem.id <> it_afetado.id
    WHERE it_afetado.habilitado = true
      -- StartPoint de it_origem está próximo da rota de it_afetado
      AND ST_Distance(
          ST_StartPoint(it_origem.the_geom)::geography,
          it_afetado.the_geom::geography
      ) <= p_raio_busca_metros
      -- StartPoint cai NO MEIO da rota (não no início nem no fim)
      AND ST_LineLocatePoint(
          it_afetado.the_geom,
          ST_StartPoint(it_origem.the_geom)
      ) BETWEEN p_margem_inicio_fim AND (1.0 - p_margem_inicio_fim)
    ON CONFLICT (itinerario_id, tipo_origem, itinerario_id_origem) DO UPDATE SET
        posicao_na_rota = EXCLUDED.posicao_na_rota,
        distancia_rota_metros = EXCLUDED.distancia_rota_metros,
        distancia_inicio_metros = EXCLUDED.distancia_inicio_metros,
        distancia_fim_metros = EXCLUDED.distancia_fim_metros,
        sentido_origem = EXCLUDED.sentido_origem,
        data_analise = NOW();

    v_total := v_total + (SELECT COUNT(*) FROM itinerario_terminal_intermediario WHERE tipo_origem = 'inicio_itinerario');

    RETURN QUERY SELECT v_total::bigint;
END;
$$;


-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================
