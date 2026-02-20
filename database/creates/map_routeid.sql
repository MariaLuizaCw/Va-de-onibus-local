-- Table: gps.map_routeid

-- DROP TABLE IF EXISTS gps.map_routeid;

CREATE TABLE IF NOT EXISTS gps.map_routeid
(
    route_id text COLLATE pg_catalog."default" NOT NULL,
    empresa text COLLATE pg_catalog."default" NOT NULL,
    numero_linha text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT map_routeid_pkey PRIMARY KEY (route_id, empresa, numero_linha)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gps.map_routeid
    OWNER to postgres;