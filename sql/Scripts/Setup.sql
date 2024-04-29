ALTER DATABASE modeling SET timezone TO 'UTC';
SELECT pg_reload_conf();

CREATE EXTENSION postgis;

CREATE TYPE site_maintainer AS ENUM ('NRCS', 'BSU');
