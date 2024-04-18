DROP TABLE temperatures;
CREATE TABLE public.temperatures (
    site_id int4 NULL,
    datetime timestamptz NULL,
    value float4 NULL,
    CONSTRAINT temperatures_site_id_datetime_key UNIQUE (site_id, datetime),
    CONSTRAINT temperatures_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(id)
);