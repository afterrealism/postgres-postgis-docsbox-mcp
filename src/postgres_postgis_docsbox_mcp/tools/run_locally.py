"""Local-execution planner.

The MCP server itself never executes psql or pg_dump — that would turn it
into a remote shell. Instead this tool returns a structured *plan* (a list
of shell steps) that the calling agent can dispatch through its own bash
tool, on the user's machine. The agent's host is the trust boundary.

Templates supported (case-insensitive substring match on ``task``):
- ``connect``     -> open a psql session
- ``schema``      -> dump the schema with pg_dump --schema-only
- ``backup``      -> full pg_dump
- ``restore``     -> pg_restore from a dump file
- ``psql_query``  -> run an ad-hoc query string via psql -c
- ``script``      -> run a SQL file via psql -f
- ``vacuum``      -> VACUUM ANALYZE (the engine, not us)
- ``shapefile``   -> import a shapefile via shp2pgsql | psql
- ``geojson``     -> import GeoJSON via ogr2ogr
"""

from __future__ import annotations

import base64
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="run_locally",
        description=(
            "Return a deterministic execution plan (list of shell steps) "
            "the calling agent can run on the user's host with its own "
            "bash tool. Templates: connect, schema, backup, restore, "
            "psql_query, script, vacuum, shapefile, geojson. The MCP "
            "server does NOT execute these commands itself."
        ),
    )
    async def run_locally(
        task: Annotated[
            str,
            Field(description="Free-form description; matched against template keywords."),
        ],
        dsn_env: Annotated[
            str,
            Field(description="Env var the agent should pass as PGURL/connection-string."),
        ] = "DATABASE_URL",
        sql: Annotated[
            str | None,
            Field(description="SQL string, used by psql_query and script templates."),
        ] = None,
        path: Annotated[
            str | None,
            Field(description="Path argument used by script/restore/shapefile/geojson templates."),
        ] = None,
        timeout_s: Annotated[
            int,
            Field(description="Suggested timeout the agent should pass to its bash tool."),
        ] = 60,
    ) -> dict[str, Any]:
        t = task.lower()
        steps: list[dict[str, Any]] = []
        notes: list[str] = []

        if any(k in t for k in ("connect", "shell", "interactive")):
            steps.append({
                "name": "connect",
                "shell": f'psql "${{{dsn_env}}}"',
                "purpose": "Open an interactive psql session.",
                "interactive": True,
            })
        elif "schema" in t:
            steps.append({
                "name": "dump_schema",
                "shell": f'pg_dump --schema-only --no-owner --no-acl "${{{dsn_env}}}"',
                "purpose": "Dump DDL only (no data, no owners).",
            })
        elif "backup" in t:
            tag = "${TAG:-$(date +%Y%m%d-%H%M%S)}"
            steps.append({
                "name": "backup",
                "shell": (
                    f'pg_dump --format=custom --file="backup-{tag}.dump" '
                    f'"${{{dsn_env}}}"'
                ),
                "purpose": "Custom-format pg_dump, restorable with pg_restore.",
            })
        elif "restore" in t:
            if not path:
                return {"ok": False, "error": "restore template requires `path` to a .dump file"}
            steps.append({
                "name": "restore",
                "shell": f'pg_restore --no-owner --no-acl --dbname="${{{dsn_env}}}" {_q(path)}',
                "purpose": "Restore a pg_dump custom-format file.",
            })
        elif any(k in t for k in ("psql_query", "query", "select")):
            if not sql:
                return {"ok": False, "error": "psql_query template requires `sql`"}
            encoded = base64.b64encode(sql.encode("utf-8")).decode("ascii")
            steps.append({
                "name": "write_sql",
                "shell": (
                    'TMPSQL="$(mktemp -t pgdocsbox-XXXXXX.sql)" && '
                    f'echo {encoded} | base64 -d > "$TMPSQL" && echo "$TMPSQL"'
                ),
                "captures": "TMPSQL",
            })
            steps.append({
                "name": "run_sql",
                "shell": (
                    f'psql --variable=ON_ERROR_STOP=1 --no-psqlrc '
                    f'--pset=pager=off --file="$TMPSQL" "${{{dsn_env}}}"'
                ),
                "timeout_s": int(timeout_s),
            })
            steps.append({"name": "cleanup", "shell": 'rm -f "$TMPSQL"', "best_effort": True})
        elif "script" in t:
            if not path:
                return {"ok": False, "error": "script template requires `path` to a .sql file"}
            steps.append({
                "name": "run_script",
                "shell": (
                    f'psql --variable=ON_ERROR_STOP=1 --no-psqlrc '
                    f'--pset=pager=off --file={_q(path)} "${{{dsn_env}}}"'
                ),
                "timeout_s": int(timeout_s),
            })
        elif "vacuum" in t:
            steps.append({
                "name": "vacuum_analyze",
                "shell": f'psql --no-psqlrc -c "VACUUM ANALYZE" "${{{dsn_env}}}"',
                "purpose": "Reclaim space and refresh statistics.",
            })
        elif "shapefile" in t or t.endswith(".shp"):
            if not path:
                return {"ok": False, "error": "shapefile template requires `path` to a .shp"}
            table = "imported_shp"
            steps.append({
                "name": "import_shapefile",
                "shell": (
                    f'shp2pgsql -s 4326 -I -D {_q(path)} {table} | '
                    f'psql --variable=ON_ERROR_STOP=1 "${{{dsn_env}}}"'
                ),
                "purpose": "Import a shapefile into a new table called imported_shp (SRID 4326).",
                "timeout_s": int(timeout_s),
            })
            notes.append("Edit the SRID (-s) and table name as needed before running.")
        elif "geojson" in t or t.endswith(".geojson") or t.endswith(".json"):
            if not path:
                return {"ok": False, "error": "geojson template requires `path` to a .geojson"}
            steps.append({
                "name": "import_geojson",
                "shell": (
                    'ogr2ogr -f PostgreSQL '
                    f'PG:"${{{dsn_env}}}" {_q(path)} -nln imported_geojson '
                    '-lco GEOMETRY_NAME=geom -lco FID=id -t_srs EPSG:4326'
                ),
                "purpose": "Import GeoJSON into PostGIS via ogr2ogr (table imported_geojson).",
                "timeout_s": int(timeout_s),
            })
        else:
            return {
                "ok": False,
                "error": "no template matched",
                "available_templates": [
                    "connect", "schema", "backup", "restore", "psql_query",
                    "script", "vacuum", "shapefile", "geojson",
                ],
            }

        return {
            "ok": True,
            "plan": {
                "task": task,
                "dsn_env": dsn_env,
                "timeout_s": int(timeout_s),
                "steps": steps,
                "notes": notes + [
                    "Dispatch each step through your own bash tool, in order.",
                    f"Make sure ${dsn_env} is exported and contains a valid postgres URL.",
                    "Review SQL before running — psql will not roll back DDL.",
                ],
            },
        }


def _q(s: str) -> str:
    if not s:
        return "''"
    if all(c.isalnum() or c in "-_./=+,@:" for c in s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"
