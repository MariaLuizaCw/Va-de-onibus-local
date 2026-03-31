WITH viagem AS (
    SELECT 
        h.ordem,
        h.linha,
        h.nome_terminal_origem,
        h.nome_terminal_destino,
        h.metodo_inferencia_origem,
        h.metodo_inferencia_destino,
        -- Ponto 1: do metadados_origem (ponto_atual)
        COALESCE(
            (h.metadados_origem->'ponto_atual'->>'longitude')::numeric,
            (h.metadados_origem->>'longitude')::numeric
        ) AS lon1,
        COALESCE(
            (h.metadados_origem->'ponto_atual'->>'latitude')::numeric,
            (h.metadados_origem->>'latitude')::numeric
        ) AS lat1,
        -- Ponto 2: do metadados_destino (latitude/longitude direto ou ponto_atual)
        COALESCE(
            (h.metadados_destino->'ponto_atual'->>'longitude')::numeric,
            (h.metadados_destino->>'longitude')::numeric
        ) AS lon2,
        COALESCE(
            (h.metadados_destino->'ponto_atual'->>'latitude')::numeric,
            (h.metadados_destino->>'latitude')::numeric
        ) AS lat2,
        -- Distância até a rota (dos metadados)
        COALESCE(
            (h.metadados_origem->>'distancia_ponto_atual_metros')::numeric,
            (h.metadados_origem->>'distancia_rota')::numeric
        ) AS dist_rota_origem,
        COALESCE(
            (h.metadados_destino->>'distancia_ponto_atual_metros')::numeric,
            (h.metadados_destino->>'distancia_rota')::numeric
        ) AS dist_rota_destino
    FROM public.gps_historico_viagens h
    WHERE h.id = 127290  -- <-- TROQUE O ID AQUI
)

SELECT 
    'ponto_origem' AS tipo,
    ST_Buffer(ST_SetSRID(ST_MakePoint(v.lon1, v.lat1), 4326)::geography, 50)::geometry AS geom,
    'ORIGEM: ' || v.nome_terminal_origem || ' (' || v.metodo_inferencia_origem || ')' AS info,
    v.ordem,
    v.linha,
    v.dist_rota_origem AS distancia_ate_rota,
    '#00FF00' AS cor  -- verde para origem
FROM viagem v

UNION ALL

SELECT 
    'ponto_destino' AS tipo,
    ST_Buffer(ST_SetSRID(ST_MakePoint(v.lon2, v.lat2), 4326)::geography, 50)::geometry AS geom,
    'DESTINO: ' || v.nome_terminal_destino || ' (' || v.metodo_inferencia_destino || ')' AS info,
    v.ordem,
    v.linha,
    v.dist_rota_destino AS distancia_ate_rota,
    '#FF0000' AS cor  -- vermelho para destino
FROM viagem v

UNION ALL

-- Clusters da linha
SELECT 
    'cluster_' || c.tipo_cluster AS tipo,
    ST_SetSRID(c.geom_cluster::geometry, 4326) AS geom,
    c.tipo_cluster || ' - ' || COALESCE(c.sentido, 'sem sentido') || ' (paradas: ' || c.num_paradas || ')' AS info,
    NULL AS ordem,
    v.linha,
    NULL AS distancia_ate_rota,
    CASE c.tipo_cluster 
        WHEN 'Terminal' THEN '#FFD700'  -- amarelo
        WHEN 'Garagem' THEN '#808080'   -- cinza
        ELSE '#FFA500'                   -- laranja
    END AS cor
FROM public.clusters_parada_resultado c, viagem v
WHERE c.linha_analisada = v.linha

UNION ALL

-- Itinerários da linha
SELECT 
    'itinerario_' || i.id || '_' || COALESCE(i.route_name, i.sentido) AS tipo,
    ST_SetSRID(i.the_geom, 4326) AS geom,
    i.route_name AS info,
    NULL AS ordem,
    v.linha,
    NULL AS distancia_ate_rota,
    CASE (ROW_NUMBER() OVER (ORDER BY i.id))::int % 6
        WHEN 0 THEN '#0000FF'  -- azul
        WHEN 1 THEN '#FF00FF'  -- magenta
        WHEN 2 THEN '#00FFFF'  -- ciano
        WHEN 3 THEN '#FF6600'  -- laranja
        WHEN 4 THEN '#9900CC'  -- roxo
        WHEN 5 THEN '#00CC00'  -- verde
    END AS cor
FROM public.itinerario i, viagem v
WHERE i.numero_linha = v.linha 
  AND i.habilitado = true

UNION ALL

-- Início de cada itinerário (primeiro ponto da LineString)
SELECT 
    'inicio_itinerario_' || i.id AS tipo,
    ST_Buffer(ST_StartPoint(ST_SetSRID(i.the_geom, 4326))::geography, 30)::geometry AS geom,
    'INÍCIO: ' || COALESCE(i.route_name, i.sentido) AS info,
    NULL AS ordem,
    v.linha,
    NULL AS distancia_ate_rota,
    '#FFFF00' AS cor  -- amarelo para início
FROM public.itinerario i, viagem v
WHERE i.numero_linha = v.linha 
  AND i.habilitado = true;