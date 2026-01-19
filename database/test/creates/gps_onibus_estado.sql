-- Table: public.gps_onibus_estado

-- DROP TABLE IF EXISTS public.gps_onibus_estado;

CREATE TABLE IF NOT EXISTS public.gps_onibus_estado
(
    ordem text COLLATE pg_catalog."default" NOT NULL,
    linha text COLLATE pg_catalog."default" NOT NULL,
    token text COLLATE pg_catalog."default",
    ultimo_terminal text COLLATE pg_catalog."default" NOT NULL,
    ultima_passagem_terminal timestamp without time zone,
    terminal_proximo text COLLATE pg_catalog."default",
    distancia_terminal_metros numeric,
    desde_terminal_proximo timestamp without time zone,
    ate_terminal_proximo timestamp without time zone,
    atualizado_em timestamp without time zone DEFAULT now(),
    ativo boolean NOT NULL DEFAULT true,
    CONSTRAINT gps_onibus_estado_pkey PRIMARY KEY (ordem)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.gps_onibus_estado
    OWNER to postgres;

GRANT ALL ON TABLE public.gps_onibus_estado TO anon;

GRANT ALL ON TABLE public.gps_onibus_estado TO authenticated;

GRANT ALL ON TABLE public.gps_onibus_estado TO postgres;

GRANT ALL ON TABLE public.gps_onibus_estado TO service_role;