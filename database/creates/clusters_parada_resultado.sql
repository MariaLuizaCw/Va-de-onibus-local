-- -----------------------------------------------------------------------------
-- clusters_parada_resultado
-- Tabela para armazenar os resultados da análise de clusters de parada
-- Gerada pela função fn_analisar_clusters_todas_linhas
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clusters_parada_resultado (
    cluster_unique_id serial PRIMARY KEY,
    cluster_id integer NOT NULL,
    linha_analisada text NOT NULL,
    num_paradas bigint NOT NULL,
    primeira_parada timestamp with time zone NOT NULL,
    ultima_parada timestamp with time zone NOT NULL,
    tempo_total_parado_minutos numeric NOT NULL,
    mediana_duracao_minutos numeric NOT NULL,
    lat_cluster numeric NOT NULL,
    lon_cluster numeric NOT NULL,
    max_distance_metros numeric NOT NULL,
    hora_mediana_cluster integer NOT NULL,
    tipo_cluster text NOT NULL,
    sentido text,
    itinerario_id integer,
    geom_cluster geography,
    
    -- Parâmetros usados na análise
    dbscan_eps_metros_usado integer NOT NULL,
    dbscan_minpoints_usado integer NOT NULL,
    duracao_minima_segundos_usado integer NOT NULL,
    min_paradas_cluster_usado integer NOT NULL,
    duracao_garagem_minutos_usado numeric NOT NULL,
    
    -- Metadados
    data_analise timestamp with time zone DEFAULT NOW(),
    
    -- Índice único para evitar duplicatas do mesmo cluster na mesma linha
    UNIQUE (cluster_id, linha_analisada)
);

-- Índices para consultas otimizadas
CREATE INDEX IF NOT EXISTS idx_clusters_parada_linha ON clusters_parada_resultado(linha_analisada);
CREATE INDEX IF NOT EXISTS idx_clusters_parada_tipo ON clusters_parada_resultado(tipo_cluster);
CREATE INDEX IF NOT EXISTS idx_clusters_parada_data_analise ON clusters_parada_resultado(data_analise);
CREATE INDEX IF NOT EXISTS idx_clusters_parada_geom ON clusters_parada_resultado USING GIST(geom_cluster);

-- Índice composto para consultas frequentes
CREATE INDEX IF NOT EXISTS idx_clusters_parada_linha_tipo ON clusters_parada_resultado(linha_analisada, tipo_cluster);

-- Comentários para documentação
COMMENT ON TABLE clusters_parada_resultado IS 'Resultados da análise de clusters de parada para identificação de terminais e garagens';
COMMENT ON COLUMN clusters_parada_resultado.cluster_unique_id IS 'ID único auto-incrementado para cada registro';
COMMENT ON COLUMN clusters_parada_resultado.cluster_id IS 'Identificador do cluster gerado pelo DBSCAN (pode repetir entre linhas diferentes)';
COMMENT ON COLUMN clusters_parada_resultado.linha_analisada IS 'Número da linha de ônibus analisada';
COMMENT ON COLUMN clusters_parada_resultado.tipo_cluster IS 'Classificação: Terminal, Garagem ou Indefinido';
COMMENT ON COLUMN clusters_parada_resultado.sentido IS 'Sentido do itinerário se validado como terminal';
COMMENT ON COLUMN clusters_parada_resultado.itinerario_id IS 'ID do itinerário se validado como terminal';
COMMENT ON COLUMN clusters_parada_resultado.geom_cluster IS 'Geometria do buffer do cluster para visualização';
COMMENT ON COLUMN clusters_parada_resultado.data_analise IS 'Timestamp de quando a análise foi executada';
