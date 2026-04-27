-- Sample dataset for postgres-postgis-docsbox-mcp tests and demos.
--
-- Greater Sydney mini-fixture: suburbs (polygons), schools (points),
-- hospitals (points), with one foreign key from schools -> suburbs.
--
-- Loadable via:
--   psql "$PG_DOCSBOX_DSN" -f examples/sample_data.sql
--
-- All geometries are EPSG:4326 (WGS84). Coordinates are illustrative.

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS public.schools CASCADE;
DROP TABLE IF EXISTS public.hospitals CASCADE;
DROP TABLE IF EXISTS public.suburbs CASCADE;

CREATE TABLE public.suburbs (
    gid          serial PRIMARY KEY,
    name         text NOT NULL,
    state        text NOT NULL DEFAULT 'NSW',
    population   integer,
    geom         geometry(MultiPolygon, 4326) NOT NULL
);

COMMENT ON TABLE public.suburbs IS 'Sydney suburb polygons (illustrative)';
COMMENT ON COLUMN public.suburbs.geom IS 'WGS84 MultiPolygon';

CREATE INDEX suburbs_geom_gix ON public.suburbs USING GIST (geom);
CREATE INDEX suburbs_name_idx ON public.suburbs (name);

CREATE TABLE public.schools (
    sid         serial PRIMARY KEY,
    name        text NOT NULL,
    sector      text NOT NULL CHECK (sector IN ('government','catholic','independent')),
    enrolments  integer,
    suburb_id   integer REFERENCES public.suburbs(gid),
    geom        geometry(Point, 4326) NOT NULL
);

COMMENT ON TABLE public.schools IS 'Sydney school points (illustrative)';

CREATE INDEX schools_geom_gix ON public.schools USING GIST (geom);
CREATE INDEX schools_suburb_idx ON public.schools (suburb_id);

CREATE TABLE public.hospitals (
    hid         serial PRIMARY KEY,
    name        text NOT NULL,
    beds        integer,
    geom        geometry(Point, 4326) NOT NULL
);

CREATE INDEX hospitals_geom_gix ON public.hospitals USING GIST (geom);

-- Suburbs (rough rectangles around real Sydney suburbs)
INSERT INTO public.suburbs (name, population, geom) VALUES
  ('Bondi',         11000, ST_Multi(ST_MakeEnvelope(151.260, -33.900, 151.290, -33.880, 4326))),
  ('Surry Hills',   17000, ST_Multi(ST_MakeEnvelope(151.205, -33.890, 151.225, -33.875, 4326))),
  ('Parramatta',    28000, ST_Multi(ST_MakeEnvelope(151.000, -33.825, 151.030, -33.805, 4326))),
  ('Manly',         16000, ST_Multi(ST_MakeEnvelope(151.275, -33.805, 151.300, -33.785, 4326))),
  ('Newtown',       14000, ST_Multi(ST_MakeEnvelope(151.170, -33.905, 151.190, -33.885, 4326)));

-- Schools (points inside the suburb envelopes; suburb_id resolved by ST_Contains)
INSERT INTO public.schools (name, sector, enrolments, suburb_id, geom) VALUES
  ('Bondi Beach Public School',          'government',   650, 1, ST_SetSRID(ST_MakePoint(151.275, -33.890), 4326)),
  ('Bondi Public School',                'government',   480, 1, ST_SetSRID(ST_MakePoint(151.270, -33.892), 4326)),
  ('Surry Hills Public School',          'government',   320, 2, ST_SetSRID(ST_MakePoint(151.215, -33.882), 4326)),
  ('Reddam House',                       'independent', 1100, 2, ST_SetSRID(ST_MakePoint(151.218, -33.885), 4326)),
  ('Parramatta Marist High',             'catholic',     950, 3, ST_SetSRID(ST_MakePoint(151.015, -33.815), 4326)),
  ('Arthur Phillip High',                'government',  1200, 3, ST_SetSRID(ST_MakePoint(151.012, -33.818), 4326)),
  ('Manly West Public School',           'government',   720, 4, ST_SetSRID(ST_MakePoint(151.286, -33.795), 4326)),
  ('Newtown High of Performing Arts',    'government',   980, 5, ST_SetSRID(ST_MakePoint(151.180, -33.895), 4326));

-- Hospitals
INSERT INTO public.hospitals (name, beds, geom) VALUES
  ('St Vincents Hospital',     400, ST_SetSRID(ST_MakePoint(151.220, -33.880), 4326)),
  ('Westmead Hospital',        980, ST_SetSRID(ST_MakePoint(150.985, -33.802), 4326)),
  ('Royal North Shore',        700, ST_SetSRID(ST_MakePoint(151.190, -33.825), 4326)),
  ('Prince of Wales',          440, ST_SetSRID(ST_MakePoint(151.240, -33.918), 4326));

ANALYZE public.suburbs;
ANALYZE public.schools;
ANALYZE public.hospitals;

COMMIT;
