-- Table: public.gps_posicoes_angra

-- DROP TABLE IF EXISTS public.gps_posicoes_angra;

CREATE TABLE IF NOT EXISTS public.gps_posicoes_angra
(
    id bigint NOT NULL DEFAULT nextval('gps_posicoes_angra_id_seq'::regclass),
    vehicle_integration_code character varying(100) COLLATE pg_catalog."default",
    vehicle_description character varying(255) COLLATE pg_catalog."default",
    line_integration_code character varying(100) COLLATE pg_catalog."default",
    line_number character varying(50) COLLATE pg_catalog."default",
    line_description character varying(255) COLLATE pg_catalog."default",
    route_integration_code character varying(100) COLLATE pg_catalog."default",
    route_direction integer,
    route_description character varying(255) COLLATE pg_catalog."default",
    estimated_departure_date timestamp without time zone,
    estimated_arrival_date timestamp without time zone,
    real_departure_date timestamp without time zone,
    real_arrival_date timestamp without time zone,
    shift integer,
    latitude double precision,
    longitude double precision,
    event_date timestamp without time zone NOT NULL,
    update_date timestamp without time zone NOT NULL,
    speed double precision,
    direction double precision,
    event_code integer,
    event_name character varying(100) COLLATE pg_catalog."default",
    is_route_start_point boolean,
    is_route_end_point boolean,
    is_garage boolean,
    license_plate character varying(20) COLLATE pg_catalog."default",
    client_bus_integration_code character varying(100) COLLATE pg_catalog."default",
    route_type text COLLATE pg_catalog."default",
    CONSTRAINT gps_posicoes_angra_pkey PRIMARY KEY (id, update_date),
    CONSTRAINT gps_posicoes_angra_unique_ponto UNIQUE (vehicle_integration_code, line_number, event_date, update_date)
) PARTITION BY RANGE (update_date);

ALTER TABLE IF EXISTS public.gps_posicoes_angra
    OWNER to postgres;

GRANT ALL ON TABLE public.gps_posicoes_angra TO anon;

GRANT ALL ON TABLE public.gps_posicoes_angra TO authenticated;

GRANT ALL ON TABLE public.gps_posicoes_angra TO postgres;

GRANT ALL ON TABLE public.gps_posicoes_angra TO service_role;
-- Index: idx_gps_posicoes_angra_event_date

-- DROP INDEX IF EXISTS public.idx_gps_posicoes_angra_event_date;

CREATE INDEX IF NOT EXISTS idx_gps_posicoes_angra_event_date
    ON public.gps_posicoes_angra USING btree
    (event_date ASC NULLS LAST)
;
-- Index: idx_gps_posicoes_angra_line_number

-- DROP INDEX IF EXISTS public.idx_gps_posicoes_angra_line_number;

CREATE INDEX IF NOT EXISTS idx_gps_posicoes_angra_line_number
    ON public.gps_posicoes_angra USING btree
    (line_number COLLATE pg_catalog."default" ASC NULLS LAST)
;
-- Index: idx_gps_posicoes_angra_vehicle

-- DROP INDEX IF EXISTS public.idx_gps_posicoes_angra_vehicle;

CREATE INDEX IF NOT EXISTS idx_gps_posicoes_angra_vehicle
    ON public.gps_posicoes_angra USING btree
    (vehicle_integration_code COLLATE pg_catalog."default" ASC NULLS LAST)
;