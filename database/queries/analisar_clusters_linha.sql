SELECT * FROM fn_analisar_clusters_linha(
    '774',                    -- p_linha_numero
    30,                       -- p_dbscan_eps_metros (raio do cluster)
    5,                        -- p_dbscan_minpoints (mín pontos para formar cluster)
    480,                      -- p_duracao_minima_segundos (8 min parado)
    20,                       -- p_min_paradas_cluster (mín paradas para considerar)
    30,                       -- p_duracao_garagem_minutos (>30min = garagem)
    0.9                       -- p_percentil_hull (90% dos pontos no convex hull)
);