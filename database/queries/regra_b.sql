-- Query para encontrar ordens que caem na REGRA B (permanência prolongada)
-- REGRA B: Permanência próxima ao terminal (100m por >= 10 min em janela de 15 min)
-- Esta query identifica ônibus que passam muito tempo próximos aos terminais

SELECT 
    e.ordem,
    e.linha,
    e.datahora,
    e.distancia_metros,
    i.numero_linha,
    i.route_name,
    t.nome_terminal,
    -- Tempo de permanência (em minutos)
    EXTRACT(EPOCH FROM (MAX(e.datahora) - MIN(e.datahora))) / 60 AS permanencia_minutos,
    -- Data mais recente e mais antiga no período
    MAX(e.datahora) AS datahora_max,
    MIN(e.datahora) AS datahora_min,
    -- Contador de eventos no período
    COUNT(*) AS total_eventos
FROM gps_proximidade_terminal_evento e
JOIN public.itinerario i ON i.numero_linha = e.linha
JOIN public.terminais t ON ST_DWithin(
    ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)::geography,
    ST_SetSRID(ST_MakePoint(e.longitude, e.latitude), 4326)::geography,
    100
)
WHERE 
    -- Filtro de proximidade (100m)
    e.distancia_metros <= 100
    -- Período de análise (últimas X horas)
    AND e.datahora >= NOW() - INTERVAL '8 hours'
GROUP BY 
    e.ordem, 
    e.linha, 
    i.numero_linha,
    i.route_name,
    t.nome_terminal
HAVING 
    -- Critério da REGRA B: permanência >= 10 minutos
    EXTRACT(EPOCH FROM (MAX(e.datahora) - MIN(e.datahora))) / 60 >= 10
    -- Pelo menos 2 eventos (para ter janela de tempo)
    COUNT(*) >= 2
ORDER BY 
    e.ordem,
    e.linha,
    datahora_max DESC;
