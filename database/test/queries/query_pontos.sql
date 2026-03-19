-- ============================================================================
-- QUERY PARAMETRIZADA - Ajuste os valores das variáveis abaixo conforme necessário
-- ============================================================================

-- PARÂMETROS CONFIGURÁVEIS
WITH parametros AS (
    SELECT
        '416'::text AS linha_numero,              -- Número da linha (ex: '220', '123')
        50::integer AS dbscan_eps_metros,         -- DBSCAN eps em metros (raio de vizinhança)
        5::integer AS dbscan_minpoints,           -- DBSCAN minpoints (mínimo de pontos por cluster)
        480::integer AS duracao_minima_segundos,  -- Duração mínima de parada (em segundos, 480 = 8 min)
        20::integer AS min_paradas_cluster,       -- Mínimo de paradas para validar cluster (< isso = indefinido)
        30::numeric AS duracao_garagem_minutos    -- Duração mediana para classificar como garagem (minutos)
),

-- ============================================================================
-- PROCESSAMENTO DOS DADOS GPS
-- ============================================================================

base AS (
    SELECT DISTINCT ON (ordem, ts)
        ordem,
        linha,
        REPLACE(latitude, ',', '.')::float AS lat,
        REPLACE(longitude, ',', '.')::float AS lon,
        to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo' AS ts
    FROM rio_gps_api_history
    WHERE linha = (SELECT linha_numero FROM parametros)
    ORDER BY ordem, ts, datahora DESC
),

ordenado AS (
    SELECT
        *,
        LAG(lat) OVER (PARTITION BY ordem ORDER BY ts) AS lat_prev,
        LAG(lon) OVER (PARTITION BY ordem ORDER BY ts) AS lon_prev,
        LAG(ts)  OVER (PARTITION BY ordem ORDER BY ts) AS ts_prev
    FROM base
),

parado_flag AS (
    SELECT
        *,
        ST_Distance(
            ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
            ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
        ) AS dist_metros,
        EXTRACT(EPOCH FROM (ts - ts_prev)) AS delta_t,
        CASE 
            WHEN EXTRACT(EPOCH FROM (ts - ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (ts - ts_prev)) = 0
            THEN NULL
            ELSE ST_Distance(
                    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
                 ) / EXTRACT(EPOCH FROM (ts - ts_prev))
        END AS velocidade_ms,
        CASE 
            WHEN EXTRACT(EPOCH FROM (ts - ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (ts - ts_prev)) = 0
            THEN 0
            WHEN (ST_Distance(
                    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
                 ) / EXTRACT(EPOCH FROM (ts - ts_prev))) < 0.556  -- 2 km/h em m/s
            THEN 1 ELSE 0
        END AS parado,
        LAG(
            CASE 
                WHEN EXTRACT(EPOCH FROM (ts - ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (ts - ts_prev)) = 0
                THEN 0
                WHEN (ST_Distance(
                        ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
                     ) / EXTRACT(EPOCH FROM (ts - ts_prev))) < 0.556
                THEN 1 ELSE 0
            END
        ) OVER (PARTITION BY ordem ORDER BY ts) AS parado_prev
    FROM ordenado
),

grupos AS (
    SELECT
        *,
        SUM(
            CASE 
                WHEN parado = 1 AND parado_prev = 1
                THEN 0 ELSE 1
            END
        ) OVER (PARTITION BY ordem ORDER BY ts) AS grupo
    FROM parado_flag
),

duracao AS (
    SELECT
        ordem,
        grupo,
        MIN(ts) AS inicio,
        MAX(ts) AS fim,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) AS duracao_segundos,
        AVG(lat) AS lat,
        AVG(lon) AS lon,
        COUNT(*) AS num_pontos
    FROM grupos
    WHERE parado = 1
    GROUP BY ordem, grupo
),

paradas_validas AS (
    SELECT
        ordem,
        grupo,
        inicio,
        fim,
        duracao_segundos,
        num_pontos,
        lat,
        lon,
        ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS geom
    FROM duracao
    WHERE duracao_segundos >= (SELECT duracao_minima_segundos FROM parametros)
),

-- ============================================================================
-- CLUSTERING COM DBSCAN
-- ============================================================================

clusters AS (
    SELECT
        *,
        ST_ClusterDBSCAN(
            ST_Transform(geom::geometry, 31983),
            eps := (SELECT dbscan_eps_metros FROM parametros),
            minpoints := (SELECT dbscan_minpoints FROM parametros)
        ) OVER () AS cluster_id
    FROM paradas_validas
),

horas_cluster AS (
    SELECT
        cluster_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM inicio)) AS hora_mediana
    FROM clusters
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
),

duracao_cluster AS (
    SELECT
        cluster_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duracao_segundos) AS mediana_duracao_segundos
    FROM clusters
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
),

-- ============================================================================
-- AGREGAÇÃO E CÁLCULOS DOS CLUSTERS
-- ============================================================================

resultado_temp AS (
    SELECT
        c.cluster_id,
        COUNT(*) AS num_paradas,
        MIN(c.inicio) AS primeira_parada,
        MAX(c.fim) AS ultima_parada,
        SUM(c.duracao_segundos) AS tempo_total_parado_segundos,
        ROUND(SUM(c.duracao_segundos)::numeric / 60, 1) AS tempo_total_parado_minutos,
        ROUND(d.mediana_duracao_segundos::numeric / 60, 1) AS mediana_duracao_minutos,
        ROUND(AVG(c.lat)::numeric, 6) AS lat_cluster,
        ROUND(AVG(c.lon)::numeric, 6) AS lon_cluster,
        ROUND(
            ST_MaxDistance(
                ST_Transform(ST_Centroid(ST_Collect(c.geom::geometry)), 31983),
                ST_Transform(ST_Collect(c.geom::geometry), 31983)
            )::numeric,
            2
        ) AS max_distance_metros,
        ROUND(h.hora_mediana::numeric, 0)::int AS hora_mediana_cluster,
        ROUND(d.mediana_duracao_segundos::numeric / 60, 1) AS mediana_duracao_minutos_temp,
        ST_Collect(c.geom::geometry) AS geom_coletado
    FROM clusters c
    LEFT JOIN horas_cluster h ON c.cluster_id = h.cluster_id
    LEFT JOIN duracao_cluster d ON c.cluster_id = d.cluster_id
    WHERE c.cluster_id IS NOT NULL
    GROUP BY c.cluster_id, h.hora_mediana, d.mediana_duracao_segundos
),

-- ============================================================================
-- CLASSIFICAÇÃO INICIAL (PRÉ-VALIDAÇÃO)
-- ============================================================================

classificacao_inicial AS (
    SELECT
        *,
        CASE 
            WHEN num_paradas < (SELECT min_paradas_cluster FROM parametros) THEN 'Indefinido'
            WHEN mediana_duracao_minutos_temp > (SELECT duracao_garagem_minutos FROM parametros) THEN 'Garagem'
            ELSE 'Potencial_Terminal'
        END AS tipo_temp
    FROM resultado_temp
),

-- ============================================================================
-- BUFFER DOS CLUSTERS (PARA VISUALIZAÇÃO E VALIDAÇÃO)
-- ============================================================================

clusters_com_buffer AS (
    SELECT
        c.cluster_id,
        ST_Buffer(
            ST_Centroid(c.geom_coletado), 
            ST_MaxDistance(
                ST_Centroid(c.geom_coletado), 
                c.geom_coletado
            )
        )::geography AS geom_cluster
    FROM classificacao_inicial c
),

-- ============================================================================
-- INÍCIO DOS ITINERÁRIOS (VALIDAÇÃO CONTRA MAPA)
-- ============================================================================

itinerarios_linha AS (
    SELECT
        ST_SetSRID(ST_StartPoint(the_geom), 4326) AS ponto_inicio,
        numero_linha,
        sentido,
        route_id
    FROM public.itinerario
    WHERE numero_linha = (SELECT linha_numero FROM parametros)
        AND habilitado = true
),

-- ============================================================================
-- VALIDAÇÃO: CLUSTER CONTÉM INÍCIO DE ITINERÁRIO?
-- ============================================================================

clusters_validados AS (
    SELECT DISTINCT ON (cb.cluster_id)
        cb.cluster_id,
        TRUE AS eh_terminal_validado,
        il.sentido,
        il.route_id
    FROM clusters_com_buffer cb
    INNER JOIN itinerarios_linha il ON 
        ST_Contains(cb.geom_cluster::geometry, il.ponto_inicio)
    ORDER BY cb.cluster_id, il.route_id
)

-- ============================================================================
-- RESULTADO FINAL: TODOS OS CLUSTERS
-- ============================================================================

SELECT
    c.cluster_id,
    c.num_paradas,
    c.primeira_parada,
    c.ultima_parada,
    c.tempo_total_parado_minutos,
    c.mediana_duracao_minutos,
    c.lat_cluster,
    c.lon_cluster,
    c.max_distance_metros,
    c.hora_mediana_cluster,
    CASE 
        WHEN c.tipo_temp = 'Garagem' THEN 'Garagem'
        WHEN c.tipo_temp = 'Potencial_Terminal' 
            AND cv.eh_terminal_validado = true THEN 'Terminal'
        ELSE 'Indefinido'
    END AS tipo_cluster,
    cv.sentido,
    cv.route_id,
    ccb.geom_cluster,
    -- Informações adicionais para auditoria
    (SELECT linha_numero FROM parametros) AS linha_analisada,
    (SELECT dbscan_eps_metros FROM parametros) AS dbscan_eps_metros_usado,
    (SELECT dbscan_minpoints FROM parametros) AS dbscan_minpoints_usado,
    (SELECT duracao_minima_segundos FROM parametros) AS duracao_minima_segundos_usado,
    (SELECT min_paradas_cluster FROM parametros) AS min_paradas_cluster_usado,
    (SELECT duracao_garagem_minutos FROM parametros) AS duracao_garagem_minutos_usado
FROM classificacao_inicial c
LEFT JOIN clusters_com_buffer ccb ON c.cluster_id = ccb.cluster_id
LEFT JOIN clusters_validados cv ON c.cluster_id = cv.cluster_id
ORDER BY c.tempo_total_parado_segundos DESC;