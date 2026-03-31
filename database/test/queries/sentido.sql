WITH posicoes_unicas_raw AS (
    SELECT DISTINCT ON (
        rgh.ordem,
        REPLACE(rgh.latitude, ',', '.')::NUMERIC,
        REPLACE(rgh.longitude, ',', '.')::NUMERIC
    )
        rgh.ordem,
        rgh.linha,
        rgh.datahora,
        ST_SetSRID(
            ST_Point(
                REPLACE(rgh.longitude, ',', '.')::NUMERIC,
                REPLACE(rgh.latitude, ',', '.')::NUMERIC
            ), 
            4326
        ) AS geom
    FROM rio_gps_api_history rgh
    WHERE rgh.ordem = 'B11615' 
      AND rgh.linha = '669'
    ORDER BY rgh.ordem,
             REPLACE(rgh.latitude, ',', '.')::NUMERIC,
             REPLACE(rgh.longitude, ',', '.')::NUMERIC,
             rgh.datahora DESC
),
posicoes_unicas AS (
    SELECT 
        ordem,
        linha,
        datahora,
        geom,
        ROW_NUMBER() OVER (
            PARTITION BY ordem 
            ORDER BY datahora DESC
        ) AS rn
    FROM posicoes_unicas_raw
),
ultimos_5_pontos AS (
    SELECT 
        ordem,
        linha,
        rn,
        datahora,
        geom
    FROM posicoes_unicas
    WHERE rn <= 5
),
pontos_com_analise AS (
    SELECT 
        u.ordem,
        u.linha,
        u.rn,
        u.datahora,
        TO_CHAR(
            TO_TIMESTAMP(u.datahora::BIGINT / 1000), 
            'YYYY-MM-DD HH24:MI:SS'
        ) AS datahora_formatada,
        u.geom,
        ST_Y(u.geom) AS latitude,
        ST_X(u.geom) AS longitude,
        it.sentido,
        it.id AS itinerario_id,
        ST_LineLocatePoint(ST_SetSRID(it.the_geom, 4326), u.geom) AS posicao_relativa,
        ROUND(
            ST_Distance(
                u.geom::geography, 
                ST_SetSRID(it.the_geom, 4326)::geography
            )::NUMERIC, 
            2
        ) AS dist_metros,
        ST_LineInterpolatePoint(
            ST_SetSRID(it.the_geom, 4326),
            ST_LineLocatePoint(ST_SetSRID(it.the_geom, 4326), u.geom)
        ) AS ponto_projetado_na_rota
    FROM ultimos_5_pontos u
    CROSS JOIN itinerario it
    WHERE it.numero_linha = u.linha
      AND it.habilitado = true
      AND ST_DWithin(it.the_geom::geography, u.geom::geography, 1000)
),
metricas_por_rota AS (
    SELECT 
        ordem,
        linha,
        sentido,

        -- invertendo o rn para a correlação ficar positiva quando o ônibus avança no tempo
        ROUND(
            CORR((-rn)::NUMERIC, posicao_relativa::NUMERIC)::NUMERIC,
            3
        ) AS corr_tempo_posicao_relativa,

        ROUND(AVG(dist_metros)::NUMERIC, 2) AS dist_media_5pontos,
        ROUND(MAX(dist_metros)::NUMERIC, 2) AS dist_max_5pontos,
        ROUND(STDDEV(dist_metros)::NUMERIC, 2) AS dist_stddev_5pontos,
        COUNT(*) AS total_pontos,
        ROUND(MIN(posicao_relativa)::NUMERIC, 4) AS pos_relativa_min,
        ROUND(MAX(posicao_relativa)::NUMERIC, 4) AS pos_relativa_max,

        jsonb_agg(
            jsonb_build_object(
                'rn', rn,
                'sentido', sentido,
                'datahora', datahora,
                'datahora_formatada', datahora_formatada,
                'latitude', ROUND(latitude::NUMERIC, 6),
                'longitude', ROUND(longitude::NUMERIC, 6),
                'dist_metros', dist_metros,
                'itinerario_id', itinerario_id,
                'posicao_relativa', ROUND(posicao_relativa::NUMERIC, 6),
                'ponto_projetado', jsonb_build_object(
                    'latitude', ROUND(ST_Y(ponto_projetado_na_rota)::NUMERIC, 6),
                    'longitude', ROUND(ST_X(ponto_projetado_na_rota)::NUMERIC, 6)
                )
            )
            ORDER BY rn
        ) AS pontos_avaliados_json
    FROM pontos_com_analise
    GROUP BY ordem, linha, sentido
),
scores_confianca AS (
    SELECT 
        ordem,
        linha,
        sentido,
        corr_tempo_posicao_relativa,
        dist_media_5pontos,
        dist_max_5pontos,
        dist_stddev_5pontos,
        total_pontos,
        pos_relativa_min,
        pos_relativa_max,
        pontos_avaliados_json,

        ROUND(
            CASE 
                WHEN corr_tempo_posicao_relativa > 0 THEN 
                    LEAST(ABS(corr_tempo_posicao_relativa), 1.0)
                ELSE 0
            END::NUMERIC,
            3
        ) AS score_posicao_relativa,

        ROUND(
            CASE 
                WHEN dist_media_5pontos <= 20 THEN 1.0
                WHEN dist_media_5pontos <= 50 THEN 0.7
                WHEN dist_media_5pontos <= 100 THEN 0.4
                ELSE 0.1
            END::NUMERIC,
            3
        ) AS score_distancia,

        ROUND(
            CASE 
                WHEN dist_stddev_5pontos IS NULL THEN 0
                WHEN dist_stddev_5pontos <= 5 THEN 1.0
                WHEN dist_stddev_5pontos <= 15 THEN 0.7
                WHEN dist_stddev_5pontos <= 30 THEN 0.4
                ELSE 0.1
            END::NUMERIC,
            3
        ) AS score_consistencia
    FROM metricas_por_rota
),
score_final AS (
    SELECT 
        *,
        ROUND(
            (
                score_posicao_relativa * 0.4 +
                score_distancia * 0.4 +
                score_consistencia * 0.2
            )::NUMERIC,
            3
        ) AS score_confianca_rota,
        ROW_NUMBER() OVER (
            PARTITION BY ordem
            ORDER BY (
                score_posicao_relativa * 0.4 +
                score_distancia * 0.4 +
                score_consistencia * 0.2
            ) DESC
        ) AS rank_confianca
    FROM scores_confianca
)
SELECT 
    ordem,
    linha,
    sentido,
    total_pontos || ' pontos' AS periodo_analise,
    pos_relativa_min || ' → ' || pos_relativa_max AS amplitude_posicao_relativa,
    dist_media_5pontos || ' m' AS distancia_media,
    dist_stddev_5pontos || ' m' AS variacao_distancia,
    '---' AS separador,
    score_posicao_relativa AS score_posicao,
    score_distancia AS score_dist,
    score_consistencia AS score_consist,
    score_confianca_rota,
    CASE 
        WHEN score_confianca_rota >= 0.8 THEN '🟢 ALTA CONFIANÇA'
        WHEN score_confianca_rota >= 0.6 THEN '🟡 MÉDIA CONFIANÇA'
        ELSE '🔴 BAIXA CONFIANÇA'
    END AS nivel_confianca,
    rank_confianca,
    pontos_avaliados_json
FROM score_final
ORDER BY ordem, rank_confianca;