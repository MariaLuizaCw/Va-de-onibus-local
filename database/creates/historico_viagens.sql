-- Table: public.gps_historico_viagens
-- Armazena viagens completas inferidas a partir de passagens por terminais

-- DROP TABLE IF EXISTS public.gps_historico_viagens;

CREATE TABLE IF NOT EXISTS public.gps_historico_viagens (
    id SERIAL PRIMARY KEY,
    ordem TEXT NOT NULL,
    linha TEXT NOT NULL,
    
    itinerario_id_origem INTEGER NOT NULL,
    itinerario_id_destino INTEGER NOT NULL,
    
    nome_terminal_origem TEXT NOT NULL,
    nome_terminal_destino TEXT NOT NULL,
    
    timestamp_chegada_origem TIMESTAMP NOT NULL,
    timestamp_chegada_destino TIMESTAMP NOT NULL,
    
    duracao_viagem INTERVAL NOT NULL,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
