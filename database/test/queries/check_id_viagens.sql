-- Substitua o ID da viagem desejada
WITH viagem AS (
    SELECT * FROM public.gps_historico_viagens WHERE id = 384731
),
pontos_origem AS (
    SELECT 
        (p->>'seq')::int AS seq,
        (p->>'latitude')::float AS lat,
        (p->>'longitude')::float AS lon
    FROM viagem, 
         jsonb_array_elements(metadados_origem->'pontos_avaliados') AS p
),
pontos_destino AS (
    SELECT 
        (p->>'seq')::int AS seq,
        (p->>'latitude')::float AS lat,
        (p->>'longitude')::float AS lon
    FROM viagem, 
         jsonb_array_elements(metadados_destino->'pontos_avaliados') AS p
)

-- 1. Itinerário de origem
SELECT 
    'Itinerário Origem: ' || i.sentido AS descricao,
    ST_SetSRID(i.the_geom, 4326) AS geom
FROM viagem v
JOIN public.itinerario i ON i.id = v.itinerario_id_origem

UNION ALL

-- 2. Itinerário de destino
SELECT 
    'Itinerário Destino: ' || i.sentido AS descricao,
    ST_SetSRID(i.the_geom, 4326) AS geom
FROM viagem v
JOIN public.itinerario i ON i.id = v.itinerario_id_destino

UNION ALL

-- 3. Pontos avaliados na origem
SELECT 
    'Ponto Origem #' || seq AS descricao,
    ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS geom
FROM pontos_origem

UNION ALL

-- 4. Pontos avaliados no destino
SELECT 
    'Ponto Destino #' || seq AS descricao,
    ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS geom
FROM pontos_destino;