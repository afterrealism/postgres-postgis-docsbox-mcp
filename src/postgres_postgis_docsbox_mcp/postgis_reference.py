"""Intent-organised PostGIS function reference, modeled on geochat.

This is the single best return value for "I'm stuck on a spatial query".
Indexed by the *user intent*, not by function name. Each entry tells the
LLM the right function, the common wrong way, and the gotcha.

References:
- https://postgis.net/docs/reference.html
- https://postgis.net/workshops/postgis-intro/
"""

from __future__ import annotations

POSTGIS_REFERENCE = """\
# PostGIS query reference (intent-organised)

## Universal rules
- All geometries must share an SRID. Use `ST_Transform(geom, 4326)` if not.
- For metric distances/areas, **cast to ::geography**: `ST_Distance(a::geography, b::geography)` returns metres.
- For boolean spatial predicates over indexed columns, prefer the **`&&` bbox operator first**: `WHERE a.geom && b.geom AND ST_Intersects(a.geom, b.geom)`.
- Always join on `ST_DWithin(a.geom::geography, b.geom::geography, METRES)` for "within N metres" — it uses the GiST index.
- The geometry column metadata catalog is `geometry_columns` (a view).

## "How far is X from Y?" — distance in metres
- WRONG: `ST_Distance(a.geom, b.geom)` — returns degrees for SRID 4326.
- RIGHT: `ST_Distance(a.geom::geography, b.geom::geography)` — metres.
- For very large distances on a sphere: `ST_DistanceSphere(a.geom, b.geom)`.

## "Find features within N metres" — radius search
- WRONG: `WHERE ST_Distance(a.geom, b.geom) < 0.001` — wrong units, no index.
- RIGHT: `WHERE ST_DWithin(a.geom::geography, b.geom::geography, 500)` — uses GiST.
- Always pass metres on the right-hand side when both sides are geography.

## "Nearest k features" — knn search
- WRONG: `ORDER BY ST_Distance(...) LIMIT k` over geometry — no index.
- RIGHT: `ORDER BY a.geom <-> ST_SetSRID(ST_MakePoint(lon,lat), 4326) LIMIT k` — uses the `<->` knn operator on GiST.
- For metric ordering: `ORDER BY a.geom::geography <-> ST_SetSRID(ST_MakePoint(lon,lat),4326)::geography LIMIT k`.

## "Inside polygon X" — point-in-polygon
- WRONG: `ST_Contains(point.geom, poly.geom)` — argument order matters; `Contains(A,B)` means A contains B.
- RIGHT: `ST_Contains(poly.geom, point.geom)` or `ST_Within(point.geom, poly.geom)`.
- For ambiguous-boundary cases use `ST_Covers(poly, point)`.

## "Touches/overlaps/intersects" — relations
- `ST_Intersects(a, b)` — any shared point.
- `ST_Touches(a, b)` — share boundary, no interior overlap.
- `ST_Overlaps(a, b)` — share interior, neither contained.
- `ST_Crosses(a, b)` — line crossing line/polygon.
- All accept geometry; all benefit from `&&` prefilter.

## "Buffer / dilate" — geometric grow
- WRONG: `ST_Buffer(geom, 100)` on SRID 4326 — buffers in degrees.
- RIGHT: `ST_Buffer(geom::geography, 100)::geometry` — 100-metre buffer, returned as geometry.
- Or transform to a metric SRID first: `ST_Transform(geom, 3857)` then `ST_Buffer`.

## "Centroid / interior point"
- `ST_Centroid(geom)` — geometric centroid (may fall outside concave shapes).
- `ST_PointOnSurface(geom)` — guaranteed-inside representative point.

## "Bounding box / extent"
- `ST_Envelope(geom)` — per-row bbox as geometry.
- `ST_Extent(geom)` — aggregate bbox over a query, returns `box2d`.
- `ST_Extent(geom)::geometry` — convert to polygon.
- For a JSON-friendly extent: `ST_AsGeoJSON(ST_Envelope(ST_Extent(geom)))`.

## "Area / length / perimeter" — measurements
- WRONG: `ST_Area(geom)` on SRID 4326 — square degrees.
- RIGHT: `ST_Area(geom::geography)` — square metres.
- Length: `ST_Length(geom::geography)` — metres.
- Perimeter of polygon: `ST_Perimeter(geom::geography)` — metres.

## "Read GeoJSON in / out"
- In:  `ST_GeomFromGeoJSON(text)` — produces SRID 4326.
- Out: `ST_AsGeoJSON(geom)` or `ST_AsGeoJSON(geom, 6, 0)` (precision, options).
- Bulk to FeatureCollection: build with `jsonb_build_object('type','FeatureCollection','features', jsonb_agg(jsonb_build_object('type','Feature','geometry', ST_AsGeoJSON(geom)::jsonb, 'properties', to_jsonb(t) - 'geom')))`.

## "Read WKT in / out"
- In:  `ST_GeomFromText('POINT(151.2 -33.9)', 4326)`.
- Out: `ST_AsText(geom)`.
- Hex WKB: `ST_AsBinary(geom)` / `encode(ST_AsBinary(geom),'hex')`.

## "Snap to grid / simplify"
- `ST_SnapToGrid(geom, 0.0001)` — quantise to a grid.
- `ST_SimplifyPreserveTopology(geom, tolerance)` — simplification that keeps validity.
- Use `ST_Subdivide(geom, 256)` for very large polygons before joining (faster index lookup).

## "Reproject"
- `ST_Transform(geom, 3857)` — Web Mercator.
- `ST_Transform(geom, 4326)` — WGS84 lon/lat.
- Always set SRID first if missing: `ST_SetSRID(ST_MakePoint(lon,lat), 4326)`.

## "Convert raster<->vector"
- Polygonise raster: `ST_DumpAsPolygons(rast)`.
- Rasterise vector: `ST_AsRaster(geom, ref)`.

## Validate / repair geometry
- `ST_IsValid(geom)` / `ST_IsValidReason(geom)` — diagnose.
- `ST_MakeValid(geom)` — most invalids fixable.
- `ST_Multi(geom)` — coerce to multi-variant (useful before union).

## Spatial joins — the cookbook
```sql
-- All schools inside the suburb 'Surry Hills':
SELECT s.*
FROM schools s
JOIN suburbs sub ON sub.name = 'Surry Hills'
WHERE s.geom && sub.geom AND ST_Within(s.geom, sub.geom);

-- Nearest 5 hospitals to a point:
SELECT id, name, ST_Distance(geom::geography, p::geography) AS metres
FROM hospitals,
     LATERAL (SELECT ST_SetSRID(ST_MakePoint(151.21, -33.87), 4326) AS p) q
ORDER BY geom <-> q.p
LIMIT 5;

-- Suburbs and the count of schools within each:
SELECT sub.name, COUNT(s.*) AS n_schools
FROM suburbs sub
LEFT JOIN schools s ON s.geom && sub.geom AND ST_Within(s.geom, sub.geom)
GROUP BY sub.name
ORDER BY n_schools DESC;
```

## Catalog tables worth knowing
- `geometry_columns` — view of (f_table_schema, f_table_name, f_geometry_column, srid, type, coord_dimension).
- `spatial_ref_sys` — every SRID with `proj4text` and `srtext`.
- `pg_extension` — has `postgis`, `postgis_topology`, etc with version.

## Common errors and the fix
- `Operation on mixed SRID geometries` → `ST_Transform` one side or `ST_SetSRID` if missing.
- `parse error - invalid geometry` → check WKT/GeoJSON; try `ST_GeomFromText` with explicit SRID.
- `function st_intersects(geometry, geography) does not exist` → cast both sides to the same family.
- `column "geom" does not exist` → call `get_table_schema` to verify the geometry column name (often `the_geom`, `wkb_geometry`, `shape`).
"""
