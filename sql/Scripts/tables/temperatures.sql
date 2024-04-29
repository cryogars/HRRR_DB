DROP TABLE temperatures;
CREATE TABLE public.temperatures (
    site_id int4 NOT NULL,
    datetime timestamptz NOT NULL,
    data_source public."data_source" NOT NULL,
    value float4 NOT NULL,
    PRIMARY KEY (site_id, data_source, datetime),
    CONSTRAINT temperatures_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(id)
);