DROP TABLE radiation;
CREATE TABLE public.radiation (
    site_id int4 NULL,
    datetime timestamptz NULL,
    shortwave_in float4 NULL,
    shortwave_out float4 NULL,
    longwave_in float4 NULL,
    longwave_out float4 NULL,
    CONSTRAINT radiation_site_id_datetime_key UNIQUE (site_id, datetime),
    CONSTRAINT radiation_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(id)
);