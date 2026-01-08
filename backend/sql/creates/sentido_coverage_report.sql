-- Table: public.sentido_coverage_report

-- DROP TABLE IF EXISTS public.sentido_coverage_report;

CREATE TABLE IF NOT EXISTS public.sentido_coverage_report
(
    id integer NOT NULL DEFAULT nextval('sentido_coverage_report_id_seq'::regclass),
    report_date date NOT NULL,
    city text COLLATE pg_catalog."default" NOT NULL,
    linha text COLLATE pg_catalog."default",
    total_pontos bigint NOT NULL,
    pontos_sem_sentido bigint NOT NULL,
    pct_sem_sentido numeric(5,2) NOT NULL,
    CONSTRAINT sentido_coverage_report_pkey PRIMARY KEY (id),
    CONSTRAINT sentido_coverage_report_report_date_city_linha_key UNIQUE (report_date, city, linha)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sentido_coverage_report
    OWNER to postgres;

GRANT ALL ON TABLE public.sentido_coverage_report TO anon;

GRANT ALL ON TABLE public.sentido_coverage_report TO authenticated;

GRANT ALL ON TABLE public.sentido_coverage_report TO postgres;

GRANT ALL ON TABLE public.sentido_coverage_report TO service_role;