"""SQL execution tools.

These are the only tools that take *user-authored SQL*; every other tool
composes its own SQL internally. Hence the heaviest defence here:

* ``validate_sql``  — static check (sqlglot + denylist + structural rules);
                      cheap, never touches the database.
* ``explain_sql``   — static check then ``EXPLAIN`` (no actual execution).
                      Catches things static can't see: unknown columns,
                      type mismatches, missing tables.
* ``execute_sql``   — static + EXPLAIN + run inside ``Database.readonly``
                      (READ ONLY tx, statement_timeout, always-rollback).

Only top-level ``SELECT`` / ``WITH`` / ``EXPLAIN`` is allowed. Multi-statement
inputs are rejected. ``LIMIT`` is auto-injected at 500 if not present.

Geometry columns: by default the rows come back in PostGIS's raw EWKB-hex
text form. Pass ``geometry_format='geojson'`` or ``'wkt'`` to have the
helper detect EWKB-hex cells and rewrite them server-side in a single
batched round-trip.
"""

from __future__ import annotations

import logging
import re
from typing import Annotated, Any

import psycopg
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..db import Database
from ..sql_validator import static_validate

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
MAX_CELL_BYTES = 64 * 1024

# Hex EWKB starts with the byte order marker (00/01) followed by a 32-bit
# type code; min 18 hex chars. Match the common case conservatively to
# avoid false positives on regular hex strings.
_EWKB_HEX_RE = re.compile(r"^0[01][0-9A-Fa-f]{16,}$")


def _err(kind: str, message: str, hint: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": kind, "message": message}
    if hint is not None:
        out["hint"] = hint
    return out


def _format_value(val: Any) -> Any:
    """Truncate giant byte/string cells so a single row can't blow the budget."""
    if isinstance(val, (bytes, bytearray, memoryview)):
        b = bytes(val)
        if len(b) > MAX_CELL_BYTES:
            return f"<{len(b)} bytes, truncated>"
        return b.hex()
    if isinstance(val, str) and len(val) > MAX_CELL_BYTES:
        return val[:MAX_CELL_BYTES] + "... [truncated]"
    return val


def _looks_like_ewkb_hex(val: Any) -> bool:
    return isinstance(val, str) and bool(_EWKB_HEX_RE.match(val))


def _format_rows(
    db: Database,
    raw_rows: list[Any],
    fmt: str,
) -> list[dict[str, Any]]:
    """Apply per-cell truncation and optional geometry rewrite.

    For ``fmt`` of ``geojson`` or ``wkt``, EWKB-hex cells across all rows
    are batched into a single follow-up readonly query that calls
    ``ST_AsGeoJSON`` / ``ST_AsText`` server-side. ``raw`` is a no-op.
    """
    rows = [{k: _format_value(v) for k, v in r.items()} for r in raw_rows]
    if fmt == "raw" or not rows:
        return rows

    candidates: list[tuple[int, str, str]] = []
    for ridx, row in enumerate(rows):
        for col, val in row.items():
            if _looks_like_ewkb_hex(val):
                candidates.append((ridx, col, val))
    if not candidates:
        return rows

    wrap = "ST_AsGeoJSON(geom)::json" if fmt == "geojson" else "ST_AsText(geom)"
    sql = (
        "SELECT idx, "
        f"{wrap} AS out "
        "FROM unnest(%s::text[]) WITH ORDINALITY AS t(hex, idx), "
        "LATERAL (SELECT ST_GeomFromEWKB(decode(t.hex, 'hex')) AS geom) g"
    )
    try:
        with db.readonly() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, ([c[2] for c in candidates],))
                wrapped = {r["idx"]: r["out"] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        logger.warning("geometry_format=%s rewrite failed: %s", fmt, exc)
        return rows

    for i, (ridx, col, _hex) in enumerate(candidates, start=1):
        if i in wrapped:
            rows[ridx][col] = wrapped[i]
    return rows


def register(mcp: FastMCP, db: Database | None) -> None:
    if db is None:
        _register_validate_only(mcp)
        return
    _register_validate(mcp)
    _register_explain(mcp, db)
    _register_execute(mcp, db)


# ---------------------------------------------------------------------------
# validate_sql (always available, no DB needed)
# ---------------------------------------------------------------------------


def _register_validate_only(mcp: FastMCP) -> None:
    _register_validate(mcp)
    msg = (
        "PG_DOCSBOX_DSN not set; only validate_sql is available. "
        "Set PG_DOCSBOX_DSN to enable explain_sql and execute_sql."
    )

    async def _stub() -> dict[str, Any]:
        return _err("not_configured", msg)

    for name in ("explain_sql", "execute_sql"):
        mcp.tool(name=name, description=msg)(_stub)


def _register_validate(mcp: FastMCP) -> None:
    @mcp.tool(
        name="validate_sql",
        description=(
            "Statically validate a SQL string without executing it. Returns "
            "the (possibly LIMIT-augmented) SQL plus an ok/error verdict. "
            "Use this before execute_sql for cheap fast-fail.\n\n"
            "Example: validate_sql(sql='SELECT 1') -> {\"ok\": true, "
            "\"sql\": \"SELECT 1 LIMIT 500\", \"auto_limit_applied\": true}\n"
            "Example: validate_sql(sql='DROP TABLE x') -> {\"ok\": false, "
            "\"error\": \"forbidden\", \"hint\": \"Only SELECT / WITH / "
            "EXPLAIN are allowed.\"}"
        ),
    )
    async def validate_sql(
        sql: Annotated[str, Field(description="SQL string to validate.")],
        default_limit: Annotated[
            int,
            Field(description="LIMIT to inject when none is present (1-1000)."),
        ] = 500,
    ) -> dict[str, Any]:
        default_limit = max(1, min(MAX_ROWS, int(default_limit)))
        result = static_validate(sql, default_limit=default_limit)
        out = {
            "ok": result.ok,
            "sql": result.sql,
            "auto_limit_applied": result.auto_limit_applied,
        }
        if not result.ok:
            out["error"] = result.error or "invalid"
        if result.hint:
            out["hint"] = result.hint
        return out


# ---------------------------------------------------------------------------
# explain_sql
# ---------------------------------------------------------------------------


def _register_explain(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="explain_sql",
        description=(
            "Run EXPLAIN (FORMAT JSON) against a SELECT/WITH query without "
            "executing it. Returns the planner's plan tree, which lets you "
            "spot seq scans, missing indexes, or wrong join order.\n\n"
            "Example: explain_sql(sql='SELECT * FROM suburbs WHERE name=$$Bondi$$') "
            "-> {\"ok\": true, \"plan\": [...], \"sql\": \"...\"}"
        ),
    )
    async def explain_sql(
        sql: Annotated[str, Field(description="SQL to explain (read-only only).")],
        analyze: Annotated[
            bool,
            Field(
                description=(
                    "If true, EXPLAIN ANALYZE — actually executes the query in a "
                    "rolled-back transaction. Useful but slower; off by default."
                ),
            ),
        ] = False,
    ) -> dict[str, Any]:
        result = static_validate(sql, default_limit=MAX_ROWS)
        if not result.ok:
            return {
                "ok": False,
                "error": result.error or "invalid",
                "hint": result.hint,
                "sql": result.sql,
            }
        opts = "FORMAT JSON, BUFFERS, VERBOSE"
        if analyze:
            opts += ", ANALYZE"
        wrapped = f"EXPLAIN ({opts}) {result.sql}"
        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(wrapped)
                    row = cur.fetchone()
                    plan = next(iter(row.values())) if row else None
        except psycopg.errors.QueryCanceled as exc:
            return _err(
                "timeout",
                str(exc),
                hint=(
                    "Statement timeout exceeded. Tighten the query or raise "
                    "PG_DOCSBOX_STATEMENT_TIMEOUT_MS."
                ),
            )
        except Exception as exc:  # noqa: BLE001
            return _err("explain_failed", str(exc))
        return {"ok": True, "sql": result.sql, "analyzed": analyze, "plan": plan}


# ---------------------------------------------------------------------------
# execute_sql
# ---------------------------------------------------------------------------


def _register_execute(mcp: FastMCP, db: Database) -> None:
    @mcp.tool(
        name="execute_sql",
        description=(
            "Execute a SELECT/WITH/EXPLAIN query inside a read-only "
            "transaction. The connection has SET TRANSACTION READ ONLY plus a "
            "short statement_timeout, and the transaction is always "
            "rolled back regardless of outcome. Geometry columns are "
            "returned in their raw EWKB-hex form by default; pass "
            "geometry_format='geojson' or 'wkt' to wrap them, or call "
            "ST_AsGeoJSON/ST_AsText in your SELECT list.\n\n"
            "Example: execute_sql(sql='SELECT name, ST_AsText(geom) "
            "FROM suburbs LIMIT 2') -> {\"ok\": true, \"columns\": "
            "[\"name\", \"st_astext\"], \"rows\": [...], \"row_count\": 2}"
        ),
    )
    async def execute_sql(
        sql: Annotated[
            str, Field(description="SQL to execute (SELECT / WITH / EXPLAIN only).")
        ],
        max_rows: Annotated[
            int,
            Field(description="Cap on rows materialised in the response (1-1000)."),
        ] = 200,
        geometry_format: Annotated[
            str,
            Field(
                description=(
                    "How to render geometry/geography columns: 'raw' returns the "
                    "DB's text form (often EWKB hex), 'geojson' wraps via "
                    "ST_AsGeoJSON, 'wkt' wraps via ST_AsText. Use 'raw' if your "
                    "query already calls ST_AsGeoJSON/ST_AsText itself."
                ),
            ),
        ] = "raw",
    ) -> dict[str, Any]:
        max_rows = max(1, min(MAX_ROWS, int(max_rows)))
        if geometry_format not in {"raw", "geojson", "wkt"}:
            return _err(
                "invalid_arg",
                f"geometry_format={geometry_format!r} not in {{raw, geojson, wkt}}",
            )

        result = static_validate(sql, default_limit=max_rows)
        if not result.ok:
            return {
                "ok": False,
                "error": result.error or "invalid",
                "hint": result.hint,
                "sql": result.sql,
            }

        try:
            with db.readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(result.sql)
                    if cur.description is None:
                        return _err(
                            "no_rowset",
                            "Query did not return a rowset.",
                            hint="Only SELECT / WITH / EXPLAIN return rows here.",
                        )
                    columns = [d.name for d in cur.description]
                    type_codes = {d.name: d.type_code for d in cur.description}
                    fetched = cur.fetchmany(max_rows + 1)
                    truncated = len(fetched) > max_rows
                    raw_rows = fetched[:max_rows]
        except psycopg.errors.QueryCanceled as exc:
            return _err(
                "timeout",
                str(exc),
                hint=(
                    "Statement timeout. Add WHERE/LIMIT, ensure a spatial "
                    "index exists (CREATE INDEX ... USING GIST(geom)), or "
                    "raise PG_DOCSBOX_STATEMENT_TIMEOUT_MS."
                ),
            )
        except psycopg.errors.InsufficientPrivilege as exc:
            return _err("permission_denied", str(exc))
        except psycopg.errors.UndefinedTable as exc:
            return _err(
                "undefined_table",
                str(exc),
                hint="Use list_tables to see what is available.",
            )
        except psycopg.errors.UndefinedColumn as exc:
            return _err(
                "undefined_column",
                str(exc),
                hint="Use get_table_schema to see real column names.",
            )
        except psycopg.errors.UndefinedFunction as exc:
            return _err(
                "undefined_function",
                str(exc),
                hint=(
                    "If the missing function is ST_*, install PostGIS: "
                    "CREATE EXTENSION postgis;"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            return _err("execute_failed", str(exc))

        # Geometry post-format. PostGIS hands us geometry/geography as
        # EWKB-hex strings by default; detect those cells and rewrite them
        # server-side in a single follow-up readonly round-trip. When format
        # is "raw" (or no rows / no geom-shaped cells) this is a no-op.
        del type_codes  # detection is on the value, not the OID
        out_rows = _format_rows(db, raw_rows, geometry_format)

        return {
            "ok": True,
            "sql": result.sql,
            "auto_limit_applied": result.auto_limit_applied,
            "columns": columns,
            "rows": out_rows,
            "row_count": len(out_rows),
            "truncated": truncated,
        }


__all__ = ["register"]
