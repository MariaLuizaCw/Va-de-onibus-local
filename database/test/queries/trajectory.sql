-- ============================================================================
-- FUNÇÃO: analyze_bus_clusters
-- Descrição: Analisa paradas de ônibus e classifica clusters baseado em critérios
-- Parâmetros:
--   p_linha: número da linha de ônibus
--   p_dist_metros: distância máxima em metros para considerar parado (padrão: 20)
--   p_delta_t_min: tempo mínimo em segundos para considerar parado (padrão: 20)
--   p_duracao_min_parada: duração mínima em segundos de uma parada válida (padrão: 480 = 8min)
--   p_eps_cluster: distância máxima em metros para agrupar clusters DBSCAN (padrão: 50)
--   p_minpoints_cluster: mínimo de pontos para formar um cluster DBSCAN (padrão: 3)
--   p_min_paradas_cluster: mínimo de paradas por cluster (padrão: 100)
--   p_duracao_garagem_min: duração mínima em minutos para ser garagem (padrão: 30)
--   p_hora_comercial_inicio: hora início do horário comercial (padrão: 9)
--   p_hora_comercial_fim: hora fim do horário comercial (padrão: 17)
-- ============================================================================

CREATE OR REPLACE FUNCTION analyze_bus_clusters(
    p_linha VARCHAR,
    p_dist_metros FLOAT DEFAULT 20,
    p_delta_t_min INT DEFAULT 20,
    p_duracao_min_parada INT DEFAULT 480,
    p_eps_cluster FLOAT DEFAULT 50,
    p_minpoints_cluster INT DEFAULT 3,
    p_min_paradas_cluster INT DEFAULT 100,
    p_duracao_garagem_min FLOAT DEFAULT 30,
    p_hora_comercial_inicio INT DEFAULT 9,
    p_hora_comercial_fim INT DEFAULT 17
)
RETURNS TABLE (
    linha VARCHAR,
    cluster_id INT,
    num_paradas BIGINT,
    primeira_parada TIMESTAMP,
    ultima_parada TIMESTAMP,
    tempo_total_parado_segundos NUMERIC,
    tempo_total_parado_minutos NUMERIC,
    media_duracao_minutos NUMERIC,
    lat_cluster NUMERIC,
    lon_cluster NUMERIC,
    max_distance_metros NUMERIC,
    hora_mediana_cluster INT,
    tipo_cluster VARCHAR
) AS $$
WITH base AS (
    SELECT DISTINCT ON (ordem, ts)
        ordem,
        linha,
        REPLACE(latitude, ',', '.')::float AS lat,
        REPLACE(longitude, ',', '.')::float AS lon,
        to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo' AS ts
    FROM rio_gps_api_history
    WHERE linha = p_linha
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
            WHEN ST_Distance(
                    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
                 ) < p_dist_metros
             AND EXTRACT(EPOCH FROM (ts - ts_prev)) >= p_delta_t_min
            THEN 1 ELSE 0
        END AS parado,
        LAG(
            CASE 
                WHEN ST_Distance(
                        ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(lon_prev, lat_prev), 4326)::geography
                     ) < p_dist_metros
                 AND EXTRACT(EPOCH FROM (ts - ts_prev)) >= p_delta_t_min
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
        SUM(delta_t) AS duracao_segundos,
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
    WHERE duracao_segundos >= p_duracao_min_parada
),
clusters AS (
    SELECT
        *,
        ST_ClusterDBSCAN(
            ST_Transform(geom::geometry, 31983),
            eps := p_eps_cluster,
            minpoints := p_minpoints_cluster
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
resultado_clusters AS (
    SELECT
        p_linha AS linha,
        c.cluster_id,
        COUNT(*) AS num_paradas,
        MIN(c.inicio) AS primeira_parada,
        MAX(c.fim) AS ultima_parada,
        SUM(c.duracao_segundos) AS tempo_total_parado_segundos,
        ROUND(SUM(c.duracao_segundos)::numeric / 60, 1) AS tempo_total_parado_minutos,
        ROUND(AVG(c.duracao_segundos)::numeric / 60, 1) AS media_duracao_minutos,
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
        CASE 
            WHEN COUNT(*) < p_min_paradas_cluster THEN 'Pouco Ponto'
            WHEN COUNT(*) >= p_min_paradas_cluster 
                 AND ROUND(AVG(c.duracao_segundos)::numeric / 60, 1) > p_duracao_garagem_min
                 AND (ROUND(h.hora_mediana::numeric, 0)::int < p_hora_comercial_inicio 
                      OR ROUND(h.hora_mediana::numeric, 0)::int >= p_hora_comercial_fim)
            THEN 'Garagem'
            WHEN COUNT(*) >= p_min_paradas_cluster THEN 'Terminal'
            ELSE 'Indefinido'
        END AS tipo_cluster
    FROM clusters c
    LEFT JOIN horas_cluster h ON c.cluster_id = h.cluster_id
    WHERE c.cluster_id IS NOT NULL
    GROUP BY c.cluster_id, h.hora_mediana
)
SELECT * FROM resultado_clusters
ORDER BY tempo_total_parado_segundos DESC;
$$ LANGUAGE SQL;

-- ============================================================================
-- Testar a função com a linha 422 (com parâmetros padrão)
-- ============================================================================
-- SELECT * FROM analyze_bus_clusters('422');

-- ============================================================================
-- Testar a função com parâmetros customizados
-- ============================================================================
-- SELECT * FROM analyze_bus_clusters(
--     p_linha := '422',
--     p_dist_metros := 25,
--     p_delta_t_min := 30,
--     p_duracao_min_parada := 600,
--     p_eps_cluster := 80,
--     p_minpoints_cluster := 5,
--     p_min_paradas_cluster := 120,
--     p_duracao_garagem_min := 40,
--     p_hora_comercial_inicio := 8,
--     p_hora_comercial_fim := 18
-- );