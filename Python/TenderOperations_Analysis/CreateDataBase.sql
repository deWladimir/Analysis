DROP DATABASE IF EXISTS prozzoro_load_data;

CREATE DATABASE prozzoro_load_data;

CREATE TABLE IF NOT EXISTS public.list_tenders
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tender_id character varying(128) COLLATE pg_catalog."default",
    date_modified timestamp with time zone,
    is_load_data boolean NOT NULL,
    queue_number integer,
    CONSTRAINT list_tenders_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_list_tenders_is_load_data
    ON public.list_tenders USING btree
    (is_load_data ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS list_tenders_tender_id_idx
    ON public.list_tenders USING btree
    (tender_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;