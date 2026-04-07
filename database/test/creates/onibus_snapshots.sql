-- Table: public.onibus_snapshots

-- DROP TABLE IF EXISTS public.onibus_snapshots;

CREATE TABLE IF NOT EXISTS public.onibus_snapshots
(
    data jsonb NOT NULL,
    city character varying COLLATE pg_catalog."default",
    CONSTRAINT onibus_snapshots_pkey PRIMARY KEY (city)
)

