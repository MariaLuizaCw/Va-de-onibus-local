-- =====================================================================
-- TESTE PRÁTICO: Determinar sentido usando CORR() com 5 últimos pontos
-- =====================================================================
-- Substitua 'ORDEM_AQUI' pela ordem real do ônibus que quer testar
-- Exemplo: 'ORD123456'

-- Versão simples (para rodar e testar rápido):
WITH ultimas_posicoes AS (
    SELECT 
        rgh.ordem,
        rgh.linha,
        rgh.datahora,
        ST_SetSRID(
            ST_Point(
                REPLACE(rgh.longitude, ',', '.')::NUMERIC,
                REPLACE(rgh.latitude, ',', '.')::NUMERIC
            ), 
            4326
        ) as geom,
        ROW_NUMBER() OVER (PARTITION BY rgh.ordem ORDER BY rgh.datahora DESC) as rn
    FROM rio_gps_api_history rgh
    WHERE rgh.ordem = 'ORDEM_AQUI'  -- ← MUDE PARA A ORDEM QUE QUER TESTAR
),
ultimas_5 AS (
    SELECT 
        ordem, 
        linha, 
        datahora, 
        geom,
        ROW_NUMBER() OVER (ORDER BY datahora ASC) as seq
    FROM ultimas_posicoes
    WHERE rn <= 5
),
com_sentidos AS (
    SELECT 
        u5.ordem,
        u5.linha,
        it.sentido,
        it.id as itinerario_id,
        u5.seq,
        u5.datahora,
        u5.geom,
        ST_LineLocatePoint(it.the_geom, u5.geom) as posicao_relativa,
        ST_Distance(u5.geom::geography, it.the_geom::geography) as dist_metros,
        ROW_NUMBER() OVER (PARTITION BY u5.ordem, it.id ORDER BY ST_Distance(u5.geom::geography, it.the_geom::geography)) as rank
    FROM ultimas_5 u5
    CROSS JOIN itinerario it
    WHERE it.numero_linha = u5.linha
        AND it.habilitado = true
        AND ST_DWithin(it.the_geom::geography, u5.geom::geography, 1000)
)
SELECT 
    DISTINCT ON (ordem)
    ordem,
    linha,
    sentido,
    itinerario_id,
    -- Mostrar as 5 posições progressivas
    MAX(CASE WHEN seq = 1 THEN ROUND(posicao_relativa::NUMERIC, 3) END) as pos_1,
    MAX(CASE WHEN seq = 2 THEN ROUND(posicao_relativa::NUMERIC, 3) END) as pos_2,
    MAX(CASE WHEN seq = 3 THEN ROUND(posicao_relativa::NUMERIC, 3) END) as pos_3,
    MAX(CASE WHEN seq = 4 THEN ROUND(posicao_relativa::NUMERIC, 3) END) as pos_4,
    MAX(CASE WHEN seq = 5 THEN ROUND(posicao_relativa::NUMERIC, 3) END) as pos_5,
    -- Método 1: Simples (último - primeiro)
    ROUND(
        (MAX(CASE WHEN seq = 5 THEN posicao_relativa END) - 
         MAX(CASE WHEN seq = 1 THEN posicao_relativa END))::NUMERIC, 
        3
    ) as tendencia_simples,
    -- Método 2: Correlação (mais robusto)
    ROUND(CORR(seq::NUMERIC, posicao_relativa)::NUMERIC, 3) as corr_coeff,
    -- Decisão final
    CASE 
        WHEN CORR(seq::NUMERIC, posicao_relativa) > 0.3 THEN '➜ IDA'
        WHEN CORR(seq::NUMERIC, posicao_relativa) < -0.3 THEN '⬅ VOLTA'
        ELSE '◉ INDETERMINADO'
    END as direcao_corr,
    CASE 
        WHEN (MAX(CASE WHEN seq = 5 THEN posicao_relativa END) - 
              MAX(CASE WHEN seq = 1 THEN posicao_relativa END)) > 0.05 THEN '➜ IDA'
        WHEN (MAX(CASE WHEN seq = 5 THEN posicao_relativa END) - 
              MAX(CASE WHEN seq = 1 THEN posicao_relativa END)) < -0.05 THEN '⬅ VOLTA'
        ELSE '◉ INDETERMINADO'
    END as direcao_simples,
    -- Qualidade do match
    ROUND(AVG(dist_metros)::NUMERIC, 0) as dist_media_metros,
    -- Timestampos para debug
    MAX(CASE WHEN seq = 1 THEN datahora END) as datahora_primeiro,
    MAX(CASE WHEN seq = 5 THEN datahora END) as datahora_ultimo,
    ROUND(
        (MAX(CASE WHEN seq = 5 THEN datahora END) - 
         MAX(CASE WHEN seq = 1 THEN datahora END))::NUMERIC / 1000, 
        0
    ) as segundos_decorridos
FROM com_sentidos
WHERE rank = 1  -- Apenas o sentido mais próximo
GROUP BY ordem, linha, sentido, itinerario_id
ORDER BY ordem, dist_media_metros;


-- =====================================================================
-- VERSÃO COM ANÁLISE DETALHADA (se quiser ver cada ponto)
-- =====================================================================
/*
SELECT 
    DISTINCT ON (ordem)
    ordem,
    linha,
    sentido,
    seq,
    ROUND(posicao_relativa::NUMERIC, 3) as pos,
    ROUND(dist_metros::NUMERIC, 0) as dist_m,
    datahora
FROM com_sentidos
WHERE rank = 1
ORDER BY ordem, sentido, seq;
*/


-- =====================================================================
-- DICAS DE USO:
-- =====================================================================
-- 1. Copie a query acima (primeira versão)
-- 2. No seu cliente SQL, substitua 'ORDEM_AQUI' pela ordem real
--    Exemplo: WHERE rgh.ordem = 'ORD2025031912345'
-- 3. Execute
-- 4. Veja os resultados:
--
--    ordem      | linha | sentido | pos_1 | pos_2 | pos_3 | pos_4 | pos_5 | tendencia_simples | corr_coeff | direcao_corr | direcao_simples | dist_media_metros
--    -----------|-------|---------|-------|-------|-------|-------|-------|-------------------|------------|--------------|-----------------|------------------
--    ORD123456  | 123   | IDA     | 0.150 | 0.320 | 0.450 | 0.620 | 0.820 |       0.670       |    0.997   |  ➜ IDA      |  ➜ IDA         |        45
--
-- 5. Compare:
--    - tendencia_simples vs corr_coeff
--    - direcao_corr vs direcao_simples
--    - Eles devem concordar na maioria dos casos
--
-- 6. Se discordarem, analise:
--    - corr_coeff próximo de 0? → Há muito ruído GPS
--    - dist_media_metros > 500? → Sentido está distante do ônibus
--    - segundos_decorridos muito grande? → Pontos são velhos, pode haver viagem
-- =====================================================================