-- Tabela para armazenar histórico bruto da API Rio GPS
-- Dados salvos exatamente como chegam da API, antes de qualquer transformação

CREATE TABLE IF NOT EXISTS rio_gps_api_history (
    id BIGSERIAL PRIMARY KEY,
    ordem TEXT,
    latitude TEXT,                -- TEXT pois a API usa vírgula como separador decimal
    longitude TEXT,               -- TEXT pois a API usa vírgula como separador decimal
    datahora BIGINT,              -- Timestamp em milissegundos
    velocidade INTEGER,
    linha TEXT,
    datahoraenvio BIGINT,         -- Timestamp em milissegundos
    datahoraservidor BIGINT,      -- Timestamp em milissegundos
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Índice para limpeza automática por data
CREATE INDEX IF NOT EXISTS idx_rio_gps_api_history_created_at 
ON rio_gps_api_history(created_at);

-- Índice para consultas por ordem e linha
CREATE INDEX IF NOT EXISTS idx_rio_gps_api_history_ordem 
ON rio_gps_api_history(ordem);

CREATE INDEX IF NOT EXISTS idx_rio_gps_api_history_linha 
ON rio_gps_api_history(linha);
