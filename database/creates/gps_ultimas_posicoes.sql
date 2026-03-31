-- -----------------------------------------------------------------------------
-- gps_ultimas_posicoes
-- Tabela auxiliar que armazena as últimas 5 posições por ônibus/ordem + linha
-- Usada para detecção de sentido via fallback (análise de histórico curto)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.gps_ultimas_posicoes (
    id SERIAL PRIMARY KEY,
    ordem TEXT NOT NULL,
    linha TEXT NOT NULL,
    datahora TIMESTAMP WITH TIME ZONE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    velocidade DOUBLE PRECISION,
    geom GEOMETRY(Point, 0) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 0)
    ) STORED,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Constraint para evitar duplicatas exatas
    CONSTRAINT gps_ultimas_posicoes_unique UNIQUE (ordem, linha, datahora, latitude, longitude)
);

-- Índice principal para busca por ônibus/linha
CREATE INDEX IF NOT EXISTS idx_gps_ultimas_posicoes_ordem_linha 
    ON public.gps_ultimas_posicoes (ordem, linha);

-- Índice para ordenação por datahora (mais recente primeiro)
CREATE INDEX IF NOT EXISTS idx_gps_ultimas_posicoes_datahora 
    ON public.gps_ultimas_posicoes (ordem, linha, datahora DESC);

-- Índice espacial para consultas geográficas
CREATE INDEX IF NOT EXISTS idx_gps_ultimas_posicoes_geom 
    ON public.gps_ultimas_posicoes USING GIST (geom);

-- Índice para cleanup por created_at
CREATE INDEX IF NOT EXISTS idx_gps_ultimas_posicoes_created_at 
    ON public.gps_ultimas_posicoes (created_at);

-- Comentários
COMMENT ON TABLE public.gps_ultimas_posicoes IS 'Armazena as últimas 5 posições distintas por ônibus/linha para detecção de sentido via fallback';
COMMENT ON COLUMN public.gps_ultimas_posicoes.geom IS 'Geometria do ponto GPS gerada automaticamente a partir de latitude/longitude';
