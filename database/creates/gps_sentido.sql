-- Table: public.gps_sentido

-- DROP TABLE IF EXISTS public.gps_sentido;

CREATE TABLE IF NOT EXISTS public.gps_sentido
(
    ordem text COLLATE pg_catalog."default" NOT NULL,
    datahora timestamp without time zone NOT NULL,
    linha text COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    velocidade double precision,
    sentido text COLLATE pg_catalog."default",
    sentido_itinerario_id integer,
    route_name text COLLATE pg_catalog."default",
    token text COLLATE pg_catalog."default",
    CONSTRAINT gps_sentido_pkey PRIMARY KEY (ordem, datahora)
) PARTITION BY RANGE (datahora);

ALTER TABLE IF EXISTS public.gps_sentido
    OWNER to postgres;

GRANT ALL ON TABLE public.gps_sentido TO anon;

GRANT ALL ON TABLE public.gps_sentido TO authenticated;

GRANT ALL ON TABLE public.gps_sentido TO postgres;

GRANT ALL ON TABLE public.gps_sentido TO service_role;