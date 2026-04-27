"""Schema introspection tools.

Goal: let an LLM agent build *situational awareness* of a Postgres database
in 3-5 tool calls — what tables exist, which look interesting, what columns
they have, what spatial reference systems are in use, and how tables relate.

The tools are deliberately read-only and capped:

* All queries run inside ``Database.readonly`` (READ ONLY tx + statement_timeout
  + always-rollback).
* Result sizes are bounded (sample rows default 5, listings cap at 500).
* Geometry payloads are returned as GeoJSON for inspection, never as full WKB.

Tools registered here:

* ``list_tables``           — every user table with row estimate, kind, has_geom.
* ``get_table_schema``      — DDL-style schema with column types and per-column
                              sample values (geochat pattern).
* ``get_column_values``     — distinct sample values for one column.
* ``list_srids``            — SRIDs in use, with column counts.
* ``get_relationships``     — foreign-key edges.
* ``list_extensions``       — installed extensions (PostGIS, pgvector, ...).
* ``pick_interesting_tables``— score tables by rows + geometry density + extent
                              area; surfaces the "look here first" candidates.

Each tool docstring includes a worked example with expected JSON shape.
"""

from __future__ import annotations

import json
import logging
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..db import Database, is_metadata_excluded

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_EXCLUDES: tuple[str, ...] = (
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "tiger",
    "tiger_data",
    "topology",
)


def _schema_filter_clause(alias: str = "n.nspname") -> tuple[str, list[Any]]:
    """Build a ``WHERE`` fragment that excludes catalog/extension schemas."""
    placeholders = ",".join(["%s"] * len(DEFAULT_SCHEMA_EXCLUDES))
    return f"{alias} NOT IN ({placeholders})", list(DEFAULT_SCHEMA_EXCLUDES)


def _err(kind: str, message: str, hint: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": kind, "message": message}
    if hint is not None:
        out["hint"] = hint
    return out


def register(mcp: FastMCP, db: Database | None) -> None:
    if db is None:
        # No DSN configured: register stubs that explain why.
        _register_stubs(mcp)
        return

    _register_list_tables(mcp, db)
    _register_get_table_schema(mcp, db)
    _register_get_column_values(mcp, db)
    _register_list_srids(mcp, db)
    _register_get_relationships(mcp, db)
    _register_list_extensions(mcp, db)
    _register_pick_interesting_tables(mcp, db)


# ---------------------------------------------------------------------------
# Stubs (no DSN configured)
# ---------------------------------------------------------------------------


def _register_stubs(mcp: FastMCP) -> None:
    msg = (
        "PG_DOCSBOX_DSN is not set; database introspection tools are disabled. "
        "Set PG_DOCSBOX_DSN to a postgres URI (e.g. "
        "postgresql://user:pass@host:5432/db) and restart."
    )

    async def _stub() -> dict[str, Any]:
        return _err("not_configured", msg, hint="Set PG_DOCSBOX_DSN env var.")

    for name in (
        "list_tables",
        "get_table_schema",
        "get_column_values",
        "list_srids",
        "get_relationships",
        "list_extensions",
        "pick_interesting_tables",
    ):
        mcp.tool(name=name, description=msg)(_stub)


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


def _register_list_tables(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_tables",
        description=(
            "List user tables/views with row estimate, kind, and a 'has_geom' "
            "flag for spatial tables. This is the typical first call when "
            "exploring an unknown database.\n\n"
            "Example return shape: {\"ok\": true, \"tables\": [{\"schema\": \"public\", "
            "\"name\": \"suburbs\", \"kind\": \"table\", \"row_estimate\": 357, "
            "\"has_geom\": true, \"geom_column\": \"geom\", \"srid\": 4326}, ...]}"
        ),
    )
    async def list_tables(
        schema_pattern: Annotated[
            str,
            Field(
                description=(
                    "SQL LIKE pattern for schemas to include (default '%' = all "
                    "non-catalog). Catalog schemas (pg_catalog, information_schema, "
                    "tiger, topology) are always excluded."
                ),
            ),
        ] = "%",
        include_views: Annotated[
            bool,
            Field(description="Whether to include views and materialized views."),
        ] = True,
        limit: Annotated[
            int,
            Field(description="Maximum tables to return (1-2000)."),
        ] = 500,
    ) -> dict[str, Any]:
        limit = max(1, min(2000, int(limit)))
        kinds = ["r"]  # ordinary table
        if include_views:
            kinds.extend(["v", "m"])  # view, materialised view
        kinds_in = ",".join(["%s"] * len(kinds))

        excl, excl_args = _schema_filter_clause()
        sql = f"""
            SELECT
                n.nspname           AS schema,
                c.relname           AS name,
                CASE c.relkind
                    WHEN 'r' THEN 'table'
                    WHEN 'v' THEN 'view'
                    WHEN 'm' THEN 'materialized_view'
                END                 AS kind,
                c.reltuples::bigint AS row_estimate,
                gc.f_geometry_column AS geom_column,
                gc.srid              AS srid,
                gc.type              AS geom_type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN geometry_columns gc
                   ON gc.f_table_schema = n.nspname
                  AND gc.f_table_name   = c.relname
            WHERE c.relkind IN ({kinds_in})
              AND n.nspname LIKE %s
              AND {excl}
            ORDER BY n.nspname, c.relname
            LIMIT %s
        """
        args = [*kinds, schema_pattern, *excl_args, limit]

        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, args)
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            return _err(
                "query_failed",
                str(exc),
                hint=(
                    "If the error mentions geometry_columns, install PostGIS: "
                    "CREATE EXTENSION postgis;"
                ),
            )

        tables = []
        for r in rows:
            full = f"{r['schema']}.{r['name']}"
            if is_metadata_excluded(full, db.cfg):
                continue
            tables.append(
                {
                    "schema": r["schema"],
                    "name": r["name"],
                    "kind": r["kind"],
                    "row_estimate": int(r["row_estimate"] or 0),
                    "has_geom": r["geom_column"] is not None,
                    "geom_column": r["geom_column"],
                    "srid": r["srid"],
                    "geom_type": r["geom_type"],
                }
            )
        return {"ok": True, "tables": tables, "count": len(tables)}


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------


def _register_get_table_schema(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_table_schema",
        description=(
            "Return a DDL-like description of one table: columns, types, "
            "nullability, defaults, primary key, indexes, and N sample rows. "
            "Sample rows materially help an LLM pick the right column for a "
            "task (geochat-style 'schema as DDL with sample-value comments').\n\n"
            "Example: get_table_schema(table='public.suburbs', sample_rows=2) -> "
            "{\"ok\": true, \"ddl\": \"CREATE TABLE public.suburbs (...)\", "
            "\"columns\": [...], \"primary_key\": [\"gid\"], \"indexes\": [...], "
            "\"sample\": [{\"gid\": 1, \"name\": \"Bondi\", ...}], \"row_estimate\": 357}"
        ),
    )
    async def get_table_schema(
        table: Annotated[
            str,
            Field(
                description=(
                    "Fully-qualified table name (schema.table). If schema is omitted, "
                    "'public' is assumed."
                ),
            ),
        ],
        sample_rows: Annotated[
            int,
            Field(description="How many sample rows to fetch (0-20)."),
        ] = 3,
    ) -> dict[str, Any]:
        schema, _, name = table.partition(".")
        if not name:
            schema, name = "public", schema
        sample_rows = max(0, min(20, int(sample_rows)))

        col_sql = """
            SELECT
                a.attname                                  AS name,
                format_type(a.atttypid, a.atttypmod)        AS type,
                NOT a.attnotnull                            AS nullable,
                pg_get_expr(d.adbin, d.adrelid)             AS default,
                col_description(a.attrelid, a.attnum)       AS comment
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d
                ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        pk_sql = """
            SELECT a.attname AS name
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE n.nspname = %s AND c.relname = %s AND i.indisprimary
            ORDER BY a.attnum
        """
        idx_sql = """
            SELECT i.relname AS name,
                   pg_get_indexdef(ix.indexrelid) AS definition,
                   ix.indisunique AS unique
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index ix ON ix.indrelid = c.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            WHERE n.nspname = %s AND c.relname = %s
              AND NOT ix.indisprimary
            ORDER BY i.relname
        """
        meta_sql = """
            SELECT c.reltuples::bigint AS row_estimate,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                   gc.f_geometry_column AS geom_col,
                   gc.srid AS srid,
                   gc.type AS geom_type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN geometry_columns gc
                ON gc.f_table_schema = n.nspname
               AND gc.f_table_name = c.relname
            WHERE n.nspname = %s AND c.relname = %s
        """

        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(meta_sql, (schema, name))
                    meta = cur.fetchone()
                    if meta is None:
                        return _err(
                            "not_found",
                            f"No relation {schema}.{name}",
                            hint="Use list_tables to see what is available.",
                        )
                    cur.execute(col_sql, (schema, name))
                    columns = cur.fetchall()
                    cur.execute(pk_sql, (schema, name))
                    pk = [r["name"] for r in cur.fetchall()]
                    cur.execute(idx_sql, (schema, name))
                    indexes = cur.fetchall()

                    sample: list[dict[str, Any]] = []
                    if sample_rows > 0:
                        # Wrap geometries as GeoJSON for inspection-friendliness.
                        select_cols = []
                        for col in columns:
                            ctype = col["type"].lower()
                            if "geometry" in ctype or "geography" in ctype:
                                select_cols.append(
                                    f'ST_AsGeoJSON("{col["name"]}")::json AS "{col["name"]}"'
                                )
                            else:
                                select_cols.append(f'"{col["name"]}"')
                        sample_sql = (
                            f'SELECT {", ".join(select_cols)} '
                            f'FROM "{schema}"."{name}" LIMIT %s'
                        )
                        try:
                            cur.execute(sample_sql, (sample_rows,))
                            sample = list(cur.fetchall())
                        except Exception as exc:  # noqa: BLE001
                            sample = [{"_sample_error": str(exc)}]
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        ddl_lines = [f'CREATE TABLE "{schema}"."{name}" (']
        for col in columns:
            line = f'    "{col["name"]}" {col["type"]}'
            if not col["nullable"]:
                line += " NOT NULL"
            if col["default"]:
                line += f' DEFAULT {col["default"]}'
            if col["comment"]:
                line += f'  -- {col["comment"]}'
            ddl_lines.append(line + ",")
        if pk:
            ddl_lines.append(f'    PRIMARY KEY ({", ".join(pk)})')
        else:
            # strip trailing comma on last column
            if ddl_lines[-1].endswith(","):
                ddl_lines[-1] = ddl_lines[-1].rstrip(",")
        ddl_lines.append(");")

        return {
            "ok": True,
            "schema": schema,
            "table": name,
            "row_estimate": int(meta["row_estimate"] or 0),
            "total_size": meta["total_size"],
            "geom_column": meta["geom_col"],
            "srid": meta["srid"],
            "geom_type": meta["geom_type"],
            "columns": [dict(c) for c in columns],
            "primary_key": pk,
            "indexes": [dict(i) for i in indexes],
            "ddl": "\n".join(ddl_lines),
            "sample": sample,
        }


# ---------------------------------------------------------------------------
# get_column_values
# ---------------------------------------------------------------------------


def _register_get_column_values(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_column_values",
        description=(
            "Return up to N distinct values from one column, with their row "
            "counts. Useful for spotting categorical columns and their domain.\n\n"
            "Example: get_column_values(table='public.suburbs', column='state', "
            "limit=5) -> {\"ok\": true, \"values\": [{\"value\": \"NSW\", "
            "\"count\": 357}]}"
        ),
    )
    async def get_column_values(
        table: Annotated[str, Field(description="schema.table")],
        column: Annotated[str, Field(description="Column name.")],
        limit: Annotated[
            int,
            Field(description="Max distinct values to return (1-200)."),
        ] = 20,
    ) -> dict[str, Any]:
        schema, _, name = table.partition(".")
        if not name:
            schema, name = "public", schema
        limit = max(1, min(200, int(limit)))

        # Validate identifiers via catalog lookup (defence vs. injection).
        check_sql = """
            SELECT 1
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
        """
        sample_sql = (
            f'SELECT "{column}" AS value, COUNT(*)::bigint AS count '
            f'FROM "{schema}"."{name}" '
            f'GROUP BY "{column}" '
            f'ORDER BY count DESC NULLS LAST '
            f'LIMIT %s'
        )

        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(check_sql, (schema, name, column))
                    if cur.fetchone() is None:
                        return _err(
                            "not_found",
                            f"Column {column} not found in {schema}.{name}",
                            hint="Use get_table_schema to list real columns.",
                        )
                    cur.execute(sample_sql, (limit,))
                    values = list(cur.fetchall())
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        return {"ok": True, "table": f"{schema}.{name}", "column": column, "values": values}


# ---------------------------------------------------------------------------
# list_srids
# ---------------------------------------------------------------------------


def _register_list_srids(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_srids",
        description=(
            "List spatial reference systems in active use across all geometry "
            "columns, with the number of columns each SRID appears on plus a "
            "human-readable name (e.g. 4326 = WGS 84). Helps an LLM choose the "
            "right SRID for buffers/distance.\n\n"
            "Example: list_srids() -> {\"ok\": true, \"srids\": [{\"srid\": 4326, "
            "\"name\": \"WGS 84\", \"unit\": \"degree\", \"column_count\": 3}]}"
        ),
    )
    async def list_srids() -> dict[str, Any]:
        sql = """
            SELECT gc.srid,
                   COUNT(*)::int AS column_count,
                   sr.srtext
            FROM geometry_columns gc
            LEFT JOIN spatial_ref_sys sr ON sr.srid = gc.srid
            GROUP BY gc.srid, sr.srtext
            ORDER BY column_count DESC, gc.srid
        """
        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            return _err(
                "query_failed",
                str(exc),
                hint="If geometry_columns is missing, run CREATE EXTENSION postgis;",
            )

        srids = []
        for r in rows:
            srtext: str = r["srtext"] or ""
            # crude name extraction from WKT
            name = ""
            unit = ""
            if srtext.startswith(("PROJCS[", "GEOGCS[", "PROJCRS[", "GEOGCRS[")):
                start = srtext.find('"') + 1
                end = srtext.find('"', start)
                if end > start:
                    name = srtext[start:end]
            if "UNIT[\"degree\"" in srtext or "AXIS[\"Lat\"" in srtext or "GEOGCS" in srtext:
                unit = "degree"
            elif "UNIT[\"metre\"" in srtext or "UNIT[\"meter\"" in srtext:
                unit = "metre"
            srids.append(
                {
                    "srid": r["srid"],
                    "name": name,
                    "unit": unit,
                    "column_count": r["column_count"],
                }
            )
        return {"ok": True, "srids": srids}


# ---------------------------------------------------------------------------
# get_relationships
# ---------------------------------------------------------------------------


def _register_get_relationships(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="get_relationships",
        description=(
            "List foreign-key edges, optionally filtered to one table. Each "
            "edge has (from_table, from_columns, to_table, to_columns, "
            "constraint_name).\n\n"
            "Example: get_relationships(table='public.schools') -> {\"ok\": true, "
            "\"edges\": [{\"from_table\": \"public.schools\", \"from_columns\": "
            "[\"suburb_id\"], \"to_table\": \"public.suburbs\", \"to_columns\": "
            "[\"gid\"], \"constraint\": \"schools_suburb_id_fkey\"}]}"
        ),
    )
    async def get_relationships(
        table: Annotated[
            str,
            Field(
                description=(
                    "Optional schema.table to filter to FKs originating from this "
                    "table. Empty = all FKs in the database."
                ),
            ),
        ] = "",
    ) -> dict[str, Any]:
        excl, excl_args = _schema_filter_clause("ns.nspname")
        where = excl
        args: list[Any] = list(excl_args)
        if table:
            schema, _, name = table.partition(".")
            if not name:
                schema, name = "public", schema
            where += " AND ns.nspname = %s AND cl.relname = %s"
            args.extend([schema, name])

        sql = f"""
            SELECT
                con.conname AS constraint,
                ns.nspname  AS from_schema,
                cl.relname  AS from_table,
                array_agg(a1.attname ORDER BY k.ord) AS from_columns,
                fns.nspname AS to_schema,
                fcl.relname AS to_table,
                array_agg(a2.attname ORDER BY fk.ord) AS to_columns
            FROM pg_constraint con
            JOIN pg_class cl     ON cl.oid  = con.conrelid
            JOIN pg_namespace ns ON ns.oid  = cl.relnamespace
            JOIN pg_class fcl    ON fcl.oid = con.confrelid
            JOIN pg_namespace fns ON fns.oid = fcl.relnamespace
            JOIN unnest(con.conkey)  WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) ON fk.ord = k.ord
            JOIN pg_attribute a1 ON a1.attrelid = con.conrelid  AND a1.attnum = k.attnum
            JOIN pg_attribute a2 ON a2.attrelid = con.confrelid AND a2.attnum = fk.attnum
            WHERE con.contype = 'f' AND {where}
            GROUP BY con.conname, ns.nspname, cl.relname, fns.nspname, fcl.relname
            ORDER BY ns.nspname, cl.relname, con.conname
        """
        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, args)
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        edges = [
            {
                "constraint": r["constraint"],
                "from_table": f"{r['from_schema']}.{r['from_table']}",
                "from_columns": list(r["from_columns"]),
                "to_table": f"{r['to_schema']}.{r['to_table']}",
                "to_columns": list(r["to_columns"]),
            }
            for r in rows
        ]
        return {"ok": True, "edges": edges, "count": len(edges)}


# ---------------------------------------------------------------------------
# list_extensions
# ---------------------------------------------------------------------------


def _register_list_extensions(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="list_extensions",
        description=(
            "List installed extensions with version. Tells the LLM whether "
            "PostGIS, pgvector, pg_trgm, etc. are available before it composes "
            "a query.\n\n"
            "Example: list_extensions() -> {\"ok\": true, \"extensions\": "
            "[{\"name\": \"postgis\", \"version\": \"3.5.0\", \"schema\": "
            "\"public\"}, {\"name\": \"plpgsql\", \"version\": \"1.0\", "
            "\"schema\": \"pg_catalog\"}]}"
        ),
    )
    async def list_extensions() -> dict[str, Any]:
        sql = """
            SELECT e.extname AS name,
                   e.extversion AS version,
                   n.nspname AS schema
            FROM pg_extension e
            JOIN pg_namespace n ON n.oid = e.extnamespace
            ORDER BY e.extname
        """
        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))
        return {"ok": True, "extensions": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# pick_interesting_tables
# ---------------------------------------------------------------------------


def _register_pick_interesting_tables(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="pick_interesting_tables",
        description=(
            "Score user tables by 'interestingness' to surface where to look "
            "first in an unfamiliar database. Score combines:\n"
            "  - log10(row_estimate) — favour data-rich tables\n"
            "  - +2 if table has a geometry column with a spatial index\n"
            "  - +1 if table has a GiST or GIN index\n"
            "  - +1 if table is referenced by foreign keys (a 'hub' table)\n\n"
            "Tables in the operator's exclude list are dropped.\n\n"
            "Example: pick_interesting_tables(limit=5) -> {\"ok\": true, "
            "\"tables\": [{\"table\": \"public.suburbs\", \"row_estimate\": "
            "357, \"score\": 5.55, \"reasons\": [\"3 inbound FKs\", \"GiST "
            "spatial index\", \"GeoJSON extent: ...\"], \"extent\": [...]}]}"
        ),
    )
    async def pick_interesting_tables(
        limit: Annotated[
            int,
            Field(description="Top N tables to return (1-50)."),
        ] = 10,
        compute_extent: Annotated[
            bool,
            Field(
                description=(
                    "Whether to compute ST_Extent per geometry table. Cheap on "
                    "indexed tables, can be slow on millions of rows; turn off if "
                    "introspection is timing out."
                ),
            ),
        ] = True,
    ) -> dict[str, Any]:
        limit = max(1, min(50, int(limit)))

        excl, excl_args = _schema_filter_clause()
        catalog_sql = f"""
            SELECT n.nspname AS schema,
                   c.relname AS name,
                   c.reltuples::bigint AS row_estimate,
                   c.oid AS oid,
                   gc.f_geometry_column AS geom_col,
                   gc.srid AS srid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN geometry_columns gc
                   ON gc.f_table_schema = n.nspname
                  AND gc.f_table_name   = c.relname
            WHERE c.relkind = 'r' AND {excl}
        """
        idx_sql = """
            SELECT c.oid,
                   bool_or(am.amname IN ('gist', 'spgist', 'brin')) AS has_spatial_idx,
                   bool_or(am.amname = 'gin') AS has_gin_idx
            FROM pg_class c
            JOIN pg_index ix ON ix.indrelid = c.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            GROUP BY c.oid
        """
        fk_in_sql = """
            SELECT confrelid AS oid, COUNT(*)::int AS inbound_fk_count
            FROM pg_constraint
            WHERE contype = 'f'
            GROUP BY confrelid
        """

        import math

        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(catalog_sql, excl_args)
                    base = list(cur.fetchall())
                    cur.execute(idx_sql)
                    idx_by_oid = {r["oid"]: r for r in cur.fetchall()}
                    cur.execute(fk_in_sql)
                    fk_by_oid = {r["oid"]: r["inbound_fk_count"] for r in cur.fetchall()}

                    scored = []
                    for r in base:
                        full = f"{r['schema']}.{r['name']}"
                        if is_metadata_excluded(full, db.cfg):
                            continue
                        rows = int(r["row_estimate"] or 0)
                        score = math.log10(max(1, rows))
                        reasons: list[str] = []
                        if rows > 0:
                            reasons.append(f"~{rows} rows")
                        idx = idx_by_oid.get(r["oid"])
                        has_spatial_idx = bool(idx and idx["has_spatial_idx"])
                        has_gin = bool(idx and idx["has_gin_idx"])
                        if r["geom_col"] and has_spatial_idx:
                            score += 2.0
                            reasons.append("GiST spatial index")
                        elif r["geom_col"]:
                            score += 1.0
                            reasons.append("geometry column (no spatial index)")
                        if has_gin:
                            score += 1.0
                            reasons.append("GIN index")
                        inbound = fk_by_oid.get(r["oid"], 0)
                        if inbound:
                            score += 1.0
                            reasons.append(f"{inbound} inbound FKs")
                        scored.append(
                            {
                                "table": full,
                                "row_estimate": rows,
                                "score": round(score, 3),
                                "reasons": reasons,
                                "geom_column": r["geom_col"],
                                "srid": r["srid"],
                                "_oid": r["oid"],
                            }
                        )

                    scored.sort(key=lambda x: x["score"], reverse=True)
                    top = scored[:limit]

                    if compute_extent:
                        for entry in top:
                            if not entry["geom_column"]:
                                continue
                            schema, _, name = entry["table"].partition(".")
                            ext_sql = (
                                f'SELECT ST_AsGeoJSON(ST_Extent("{entry["geom_column"]}"))'
                                f'::json AS extent FROM "{schema}"."{name}"'
                            )
                            try:
                                cur.execute(ext_sql)
                                row = cur.fetchone()
                                entry["extent"] = row["extent"] if row else None
                            except Exception as exc:  # noqa: BLE001
                                entry["extent_error"] = str(exc)

        except Exception as exc:  # noqa: BLE001
            return _err("query_failed", str(exc))

        for entry in top:
            entry.pop("_oid", None)
        return {"ok": True, "tables": top, "count": len(top)}


__all__ = ["register"]

# Helpful for json-serialising decimal/uuid/etc. in tests
def _to_jsonable(value: Any) -> Any:  # pragma: no cover - utility
    return json.loads(json.dumps(value, default=str))
