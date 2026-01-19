-- Table: public.itinerario

-- DROP TABLE IF EXISTS public.itinerario;

CREATE TABLE IF NOT EXISTS public.itinerario
(
    id integer NOT NULL,
    versaosentido_oid integer NOT NULL,
    route_id integer NOT NULL,
    codigo_master integer,
    route_name text COLLATE pg_catalog."default" NOT NULL,
    vista text COLLATE pg_catalog."default" NOT NULL,
    servico text COLLATE pg_catalog."default" NOT NULL,
    sentido text COLLATE pg_catalog."default" NOT NULL,
    tipo_veiculo text COLLATE pg_catalog."default" NOT NULL,
    operadora text COLLATE pg_catalog."default" NOT NULL,
    consorcio text COLLATE pg_catalog."default" NOT NULL,
    numero_linha text COLLATE pg_catalog."default" NOT NULL,
    municipio text COLLATE pg_catalog."default" NOT NULL,
    prioridade double precision NOT NULL,
    habilitado boolean NOT NULL,
    distancia_total double precision NOT NULL,
    the_geom geometry NOT NULL,
    the_geom_google geometry NOT NULL,
    codigo_detro text COLLATE pg_catalog."default" NOT NULL,
    corconsorcio text COLLATE pg_catalog."default",
    brs text COLLATE pg_catalog."default",
    funcao text COLLATE pg_catalog."default"
)
