-- -----------------------------------------------------------------------------
-- itinerario_terminal_intermediario
-- Identifica terminais que aparecem NO MEIO de uma rota (não como ponto inicial/final)
-- Usado para evitar falsos positivos na detecção de última passagem
-- Ex: Terminal Gentiliza que faz parte do trajeto de algumas linhas
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS itinerario_terminal_intermediario (
    id SERIAL PRIMARY KEY,
    -- Itinerário afetado (a rota que tem o terminal no meio)
    itinerario_id INTEGER NOT NULL,
    numero_linha TEXT NOT NULL,
    sentido TEXT NOT NULL,
    -- Tipo de origem: 'cluster_terminal' ou 'inicio_itinerario'
    tipo_origem TEXT NOT NULL,
    -- Campos para tipo 'cluster_terminal' (nullable para inicio_itinerario)
    cluster_unique_id INTEGER,
    cluster_id INTEGER,
    linha_cluster TEXT,
    -- Campo para tipo 'inicio_itinerario': qual itinerário tem o StartPoint no meio desta rota
    itinerario_id_origem INTEGER,
    sentido_origem TEXT,
    -- Posição do terminal na rota (0.0 = início, 1.0 = fim)
    posicao_na_rota NUMERIC NOT NULL,
    -- Distância do terminal ao trecho mais próximo da rota (metros)
    distancia_rota_metros NUMERIC NOT NULL,
    -- Distância do terminal ao início da rota (metros)
    distancia_inicio_metros NUMERIC NOT NULL,
    -- Distância do terminal ao fim da rota (metros)
    distancia_fim_metros NUMERIC NOT NULL,
    -- Coordenadas do terminal
    lat_terminal NUMERIC NOT NULL,
    lon_terminal NUMERIC NOT NULL,
    -- Metadados
    data_analise TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Constraints únicas por tipo
    UNIQUE (itinerario_id, tipo_origem, cluster_unique_id),
    UNIQUE (itinerario_id, tipo_origem, itinerario_id_origem)
);

-- Índices para consultas frequentes
CREATE INDEX IF NOT EXISTS idx_itin_term_inter_linha 
ON itinerario_terminal_intermediario(numero_linha);

CREATE INDEX IF NOT EXISTS idx_itin_term_inter_cluster 
ON itinerario_terminal_intermediario(cluster_unique_id) WHERE cluster_unique_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_itin_term_inter_itinerario 
ON itinerario_terminal_intermediario(itinerario_id);

CREATE INDEX IF NOT EXISTS idx_itin_term_inter_tipo 
ON itinerario_terminal_intermediario(tipo_origem);

CREATE INDEX IF NOT EXISTS idx_itin_term_inter_itin_origem 
ON itinerario_terminal_intermediario(itinerario_id_origem) WHERE itinerario_id_origem IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_itin_term_inter_linha_tipo 
ON itinerario_terminal_intermediario(numero_linha, tipo_origem);

COMMENT ON TABLE itinerario_terminal_intermediario IS 'Terminais que aparecem no meio de uma rota, não como ponto inicial/final. Usados para filtrar falsos positivos na detecção de última passagem.';
COMMENT ON COLUMN itinerario_terminal_intermediario.posicao_na_rota IS 'Fração da rota onde o terminal está (0.0=início, 1.0=fim). Valores entre ~0.05 e ~0.95 indicam terminal intermediário.';
