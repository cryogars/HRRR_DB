DROP TABLE radiation;
CREATE TABLE public.radiation (
    site_id int4 NOT NULL,
    datetime timestamptz NOT NULL,
    data_source public."data_source" NOT NULL,
    shortwave_in float4 NOT NULL,
    shortwave_out float4 NULL,
    longwave_in float4 NULL,
    longwave_out float4 NULL,
    PRIMARY KEY (site_id, datetime, data_source),
    CONSTRAINT radiation_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.sites(id)
);