-- Avaliar dados da linha SP918 na rio_gps_api_history

-- 0. Investigar fragmentação de paradas por ruído GPS
-- Se há muitas paradas curtas consecutivas no mesmo local, o GPS está fragmentando
WITH base AS (
    SELECT DISTINCT ON (r.ordem, (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp)
        r.ordem,
        REPLACE(r.latitude, ',', '.')::float AS lat,
        REPLACE(r.longitude, ',', '.')::float AS lon,
        (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp AS ts
    FROM rio_gps_api_history r
    WHERE r.linha = 'SP918'
    ORDER BY r.ordem, ts, r.datahora DESC
),
ordenado AS (
    SELECT b.*,
        LAG(b.lat) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lat_prev,
        LAG(b.lon) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lon_prev,
        LAG(b.ts) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS ts_prev
    FROM base b
),
parado_flag AS (
    SELECT o.*,
        ST_Distance(
            ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
            ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
        ) AS dist_metros,
        EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) AS delta_t,
        CASE 
            WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0 THEN 0
            WHEN (ST_Distance(
                    ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                 ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))) < 0.556
            THEN 1 ELSE 0
        END AS parado
    FROM ordenado o
),
-- Amostra: ver transições parado->movendo->parado em curto intervalo
transicoes AS (
    SELECT 
        pf.*,
        LAG(pf.parado) OVER (PARTITION BY pf.ordem ORDER BY pf.ts) AS parado_anterior,
        LEAD(pf.parado) OVER (PARTITION BY pf.ordem ORDER BY pf.ts) AS parado_proximo
    FROM parado_flag pf
)
-- Casos onde estava parado, "moveu" por 1 ponto, e voltou a parar (fragmentação)
SELECT 
    COUNT(*) AS fragmentacoes_detectadas,
    AVG(dist_metros) AS media_distancia_metros,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dist_metros) AS mediana_distancia_metros,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY dist_metros) AS p90_distancia_metros
FROM transicoes
WHERE parado = 0 
  AND parado_anterior = 1 
  AND parado_proximo = 1
  AND dist_metros < 50;  -- "movimento" de menos de 50m entre duas paradas

-- 1. Período e volume de dados
SELECT 
    linha,
    COUNT(DISTINCT ordem) AS total_veiculos,
    COUNT(DISTINCT DATE(to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')) AS dias_com_dados,
    MIN(to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo') AS primeira_data,
    MAX(to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo') AS ultima_data,
    COUNT(*) AS total_pontos_gps
FROM rio_gps_api_history
WHERE linha = 'SP918'
GROUP BY linha;

-- 2. Pontos GPS por dia
SELECT 
    DATE(to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(DISTINCT ordem) AS veiculos_ativos,
    COUNT(*) AS pontos_gps
FROM rio_gps_api_history
WHERE linha = 'SP918'
GROUP BY dia
ORDER BY dia;

-- 3. Quantas paradas de 8+ min existem por dia (critério atual)
WITH base AS (
    SELECT DISTINCT ON (r.ordem, (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp)
        r.ordem,
        REPLACE(r.latitude, ',', '.')::float AS lat,
        REPLACE(r.longitude, ',', '.')::float AS lon,
        (to_timestamp(r.datahora / 1000) AT TIME ZONE 'America/Sao_Paulo')::timestamp AS ts
    FROM rio_gps_api_history r
    WHERE r.linha = 'SP918'
    ORDER BY r.ordem, ts, r.datahora DESC
),
ordenado AS (
    SELECT b.*,
        LAG(b.lat) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lat_prev,
        LAG(b.lon) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS lon_prev,
        LAG(b.ts) OVER (PARTITION BY b.ordem ORDER BY b.ts) AS ts_prev
    FROM base b
),
parado_flag AS (
    SELECT o.*,
        CASE 
            WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0 THEN 0
            WHEN (ST_Distance(
                    ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                 ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))) < 0.556
            THEN 1 ELSE 0
        END AS parado,
        LAG(CASE 
            WHEN EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) IS NULL OR EXTRACT(EPOCH FROM (o.ts - o.ts_prev)) = 0 THEN 0
            WHEN (ST_Distance(
                    ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(o.lon_prev, o.lat_prev), 4326)::geography
                 ) / EXTRACT(EPOCH FROM (o.ts - o.ts_prev))) < 0.556
            THEN 1 ELSE 0
        END) OVER (PARTITION BY o.ordem ORDER BY o.ts) AS parado_prev
    FROM ordenado o
),
grupos AS (
    SELECT pf.*,
        SUM(CASE WHEN pf.parado = 1 AND pf.parado_prev = 1 THEN 0 ELSE 1 END) 
            OVER (PARTITION BY pf.ordem ORDER BY pf.ts) AS grupo
    FROM parado_flag pf
),
duracao AS (
    SELECT 
        g.ordem,
        g.grupo,
        DATE(MIN(g.ts)) AS dia,
        MIN(g.ts) AS inicio,
        MAX(g.ts) AS fim,
        EXTRACT(EPOCH FROM (MAX(g.ts) - MIN(g.ts))) AS duracao_segundos,
        AVG(g.lat) AS lat,
        AVG(g.lon) AS lon
    FROM grupos g
    WHERE g.parado = 1
    GROUP BY g.ordem, g.grupo
)
SELECT 
    dia,
    COUNT(*) AS total_paradas,
    COUNT(*) FILTER (WHERE duracao_segundos >= 480) AS paradas_8min_ou_mais,
    COUNT(*) FILTER (WHERE duracao_segundos >= 300) AS paradas_5min_ou_mais,
    COUNT(*) FILTER (WHERE duracao_segundos >= 180) AS paradas_3min_ou_mais
FROM duracao
GROUP BY dia
ORDER BY dia;
