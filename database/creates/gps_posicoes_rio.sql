-- Table: public.gps_posicoes_rio

-- DROP TABLE IF EXISTS public.gps_posicoes_rio;

CREATE TABLE IF NOT EXISTS public.gps_posicoes_rio
(
    id bigint NOT NULL DEFAULT nextval('gps_posicoes_rio_id_seq'::regclass),
    ordem character varying(20) COLLATE pg_catalog."default" NOT NULL,
    latitude numeric(10,6) NOT NULL,
    longitude numeric(10,6) NOT NULL,
    datahora bigint NOT NULL,
    velocidade integer NOT NULL,
    linha character varying(20) COLLATE pg_catalog."default" NOT NULL,
    datahoraenvio bigint NOT NULL,
    datahoraservidor bigint NOT NULL,
    CONSTRAINT gps_posicoes_rio_pkey PRIMARY KEY (id, datahoraenvio),
    CONSTRAINT gps_posicoes_rio_unique_ponto UNIQUE (ordem, datahora, latitude, longitude, datahoraenvio)
) PARTITION BY RANGE (datahoraenvio);

ALTER TABLE IF EXISTS public.gps_posicoes_rio
    OWNER to postgres;

GRANT ALL ON TABLE public.gps_posicoes_rio TO anon;

GRANT ALL ON TABLE public.gps_posicoes_rio TO authenticated;

GRANT ALL ON TABLE public.gps_posicoes_rio TO postgres;

GRANT ALL ON TABLE public.gps_posicoes_rio TO service_role;
-- Index: idx_gps_posicoes_rio_linha

-- DROP INDEX IF EXISTS public.idx_gps_posicoes_rio_linha;

CREATE INDEX IF NOT EXISTS idx_gps_posicoes_rio_linha
    ON public.gps_posicoes_rio USING btree
    (linha COLLATE pg_catalog."default" ASC NULLS LAST)
;