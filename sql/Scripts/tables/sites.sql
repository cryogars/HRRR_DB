DROP TABLE IF EXISTS sites;
CREATE TABLE public.sites (
    id serial4 NOT NULL,
    "name" varchar(100) NOT NULL,
    maintainer public."site_maintainer" NOT NULL,
    maintainer_id int4 NOT NULL,
    geom public.geometry(point, 4326) NULL,
    CONSTRAINT sites_name_maintainer_id_key UNIQUE (name, maintainer_id),
    CONSTRAINT sites_pkey PRIMARY KEY (id)
);
CREATE INDEX sites_geom_idx ON public.sites USING gist (geom);
CREATE INDEX sites_maintainer_id_idx ON public.sites USING btree (maintainer_id);