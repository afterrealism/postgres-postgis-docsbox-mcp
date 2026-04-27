"""Integration tests against a live Postgres+PostGIS DB.

Loaded fixture: examples/sample_data.sql (Sydney suburbs/schools/hospitals).

Skipped unless --run-db is passed and PG_DOCSBOX_DSN is set.
"""

from __future__ import annotations

import pytest
from mcp.server.fastmcp import FastMCP

from postgres_postgis_docsbox_mcp.tools import execute as execute_mod
from postgres_postgis_docsbox_mcp.tools import introspect as introspect_mod

pytestmark = pytest.mark.db


@pytest.fixture
def tools(database):
    mcp = FastMCP(name="t", host="127.0.0.1", port=0)
    introspect_mod.register(mcp, database)
    execute_mod.register(mcp, database)
    tm = mcp._tool_manager
    return {
        "list_tables": tm.get_tool("list_tables").fn,
        "get_table_schema": tm.get_tool("get_table_schema").fn,
        "get_column_values": tm.get_tool("get_column_values").fn,
        "list_srids": tm.get_tool("list_srids").fn,
        "get_relationships": tm.get_tool("get_relationships").fn,
        "list_extensions": tm.get_tool("list_extensions").fn,
        "pick_interesting_tables": tm.get_tool("pick_interesting_tables").fn,
        "validate_sql": tm.get_tool("validate_sql").fn,
        "explain_sql": tm.get_tool("explain_sql").fn,
        "execute_sql": tm.get_tool("execute_sql").fn,
    }


@pytest.mark.asyncio
async def test_list_extensions_has_postgis(tools) -> None:
    out = await tools["list_extensions"]()
    assert out["ok"] is True
    names = {e["name"] for e in out["extensions"]}
    assert "postgis" in names


@pytest.mark.asyncio
async def test_list_tables_finds_sample(tools) -> None:
    out = await tools["list_tables"]()
    assert out["ok"]
    names = {f"{t['schema']}.{t['name']}" for t in out["tables"]}
    assert {"public.suburbs", "public.schools", "public.hospitals"} <= names


@pytest.mark.asyncio
async def test_get_table_schema_suburbs(tools) -> None:
    out = await tools["get_table_schema"](table="public.suburbs", sample_rows=2)
    assert out["ok"]
    assert out["geom_column"] == "geom"
    assert out["srid"] == 4326
    assert "gid" in out["primary_key"]
    assert len(out["sample"]) == 2
    # geometry should have been wrapped to GeoJSON in samples
    assert isinstance(out["sample"][0]["geom"], dict)
    assert out["sample"][0]["geom"]["type"] in {"MultiPolygon", "Polygon"}


@pytest.mark.asyncio
async def test_get_column_values(tools) -> None:
    out = await tools["get_column_values"](
        table="public.schools", column="sector", limit=10
    )
    assert out["ok"]
    sectors = {v["value"] for v in out["values"]}
    assert sectors <= {"government", "catholic", "independent"}


@pytest.mark.asyncio
async def test_list_srids(tools) -> None:
    out = await tools["list_srids"]()
    assert out["ok"]
    srids = {s["srid"] for s in out["srids"]}
    assert 4326 in srids


@pytest.mark.asyncio
async def test_get_relationships(tools) -> None:
    out = await tools["get_relationships"](table="public.schools")
    assert out["ok"]
    targets = {e["to_table"] for e in out["edges"]}
    assert "public.suburbs" in targets


@pytest.mark.asyncio
async def test_pick_interesting_tables(tools) -> None:
    out = await tools["pick_interesting_tables"](limit=5, compute_extent=True)
    assert out["ok"]
    names = {t["table"] for t in out["tables"]}
    assert "public.suburbs" in names
    sub = next(t for t in out["tables"] if t["table"] == "public.suburbs")
    assert sub["geom_column"] == "geom"
    # extent should be a GeoJSON object
    assert isinstance(sub["extent"], dict)
    assert sub["extent"]["type"] in {"Polygon", "MultiPolygon"}


@pytest.mark.asyncio
async def test_execute_sql_spatial_join(tools) -> None:
    sql = (
        "SELECT s.name AS school, sub.name AS suburb "
        "FROM schools s "
        "JOIN suburbs sub ON ST_Contains(sub.geom, s.geom) "
        "ORDER BY school"
    )
    out = await tools["execute_sql"](sql=sql, max_rows=20)
    assert out["ok"], out
    assert out["row_count"] > 0
    # Each row should have school + suburb keys
    assert {"school", "suburb"} <= set(out["rows"][0].keys())


@pytest.mark.asyncio
async def test_execute_sql_rejects_ddl(tools) -> None:
    out = await tools["execute_sql"](sql="DROP TABLE suburbs")
    assert out["ok"] is False
    assert out["error"]


@pytest.mark.asyncio
async def test_explain_sql(tools) -> None:
    out = await tools["explain_sql"](sql="SELECT * FROM suburbs")
    assert out["ok"], out
    assert isinstance(out["plan"], list)


@pytest.mark.asyncio
async def test_validate_sql_works_without_db(tools) -> None:
    out = await tools["validate_sql"](sql="SELECT * FROM suburbs")
    assert out["ok"]
    assert out["auto_limit_applied"]
