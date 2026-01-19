-- Table: public.onibus_snapshots

-- DROP TABLE IF EXISTS public.onibus_snapshots;

CREATE TABLE IF NOT EXISTS public.onibus_snapshots
(
    id bigint NOT NULL DEFAULT nextval('rio_onibus_snapshots_id_seq'::regclass),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    data jsonb NOT NULL,
    city character varying COLLATE pg_catalog."default",
    CONSTRAINT rio_onibus_snapshots_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.onibus_snapshots
    OWNER to postgres;

GRANT ALL ON TABLE public.onibus_snapshots TO anon;

GRANT ALL ON TABLE public.onibus_snapshots TO authenticated;

GRANT ALL ON TABLE public.onibus_snapshots TO postgres;

GRANT ALL ON TABLE public.onibus_snapshots TO service_role;