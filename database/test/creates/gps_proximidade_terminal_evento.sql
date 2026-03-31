-- Table: public.gps_proximidade_terminal_evento

-- DROP TABLE IF EXISTS public.gps_proximidade_terminal_evento;

CREATE TABLE IF NOT EXISTS public.gps_proximidade_terminal_evento (
    ordem TEXT NOT NULL,
    datahora TIMESTAMP NOT NULL,
    linha TEXT NOT NULL,

    itinerario_id integer NOT NULL,
    sentido TEXT NOT NULL,

    distancia_metros NUMERIC NOT NULL,

    PRIMARY KEY (ordem, datahora)
);

-- Índices essenciais para performance
CREATE INDEX IF NOT EXISTS idx_gps_proximidade_terminal_evento_ordem_datahora 
    ON public.gps_proximidade_terminal_evento (ordem, datahora DESC);

CREATE INDEX IF NOT EXISTS idx_gps_proximidade_terminal_evento_linha 
    ON public.gps_proximidade_terminal_evento (linha);

CREATE INDEX IF NOT EXISTS idx_gps_proximidade_terminal_evento_distancia 
    ON public.gps_proximidade_terminal_evento (distancia_metros);
