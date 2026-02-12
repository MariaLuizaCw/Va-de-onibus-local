-- Table: public.gps_historico_viagens
-- Armazena viagens completas inferidas a partir de passagens por terminais

-- DROP TABLE IF EXISTS public.gps_historico_viagens;

CREATE TABLE IF NOT EXISTS public.gps_historico_viagens (
    id SERIAL PRIMARY KEY,
    ordem TEXT NOT NULL,
    token TEXT NOT NULL,
    linha TEXT NOT NULL,
    
    itinerario_id_origem INTEGER,
    itinerario_id_destino INTEGER,
    
    nome_terminal_origem TEXT,
    nome_terminal_destino TEXT,
    
    metodo_inferencia_origem TEXT,
    metodo_inferencia_destino TEXT,
    
    timestamp_inicio TIMESTAMP NOT NULL,
    timestamp_fim TIMESTAMP,
    
    duracao_viagem INTERVAL,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índice único para evitar viagens duplicadas
CREATE UNIQUE INDEX IF NOT EXISTS idx_gps_historico_viagens_unique_trip 
    ON public.gps_historico_viagens (ordem, token, timestamp_inicio);

-- Índice para encontrar viagens abertas por ônibus
CREATE INDEX IF NOT EXISTS idx_gps_historico_viagens_open_trips 
    ON public.gps_historico_viagens (ordem, token, timestamp_fim) 
    WHERE timestamp_fim IS NULL;
