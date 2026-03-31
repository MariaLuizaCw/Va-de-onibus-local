-- -----------------------------------------------------------------------------
-- gps_ultima_passagem
-- Tabela que armazena a última passagem identificada de cada ônibus por linha
-- Chave primária composta: (ordem, linha)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gps_ultima_passagem (
    ordem TEXT NOT NULL,
    linha TEXT NOT NULL,
    label_ultima_passagem TEXT,
    sentido TEXT,
    itinerario_id INTEGER,
    metodo_detecao TEXT,
    lat_detecao DOUBLE PRECISION,
    lon_detecao DOUBLE PRECISION,
    datahora_atualizacao TIMESTAMP WITH TIME ZONE NOT NULL,
    datahora_identificacao TIMESTAMP WITH TIME ZONE,
    em_terminal BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ordem, linha)
);


-- Índice para consultas por linha
CREATE INDEX IF NOT EXISTS idx_gps_ultima_passagem_linha ON gps_ultima_passagem(linha);

-- Índice para consultas por datahora_atualizacao (útil para cleanup)
CREATE INDEX IF NOT EXISTS idx_gps_ultima_passagem_datahora ON gps_ultima_passagem(datahora_atualizacao);
