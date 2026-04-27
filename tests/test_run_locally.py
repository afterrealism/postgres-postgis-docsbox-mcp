"""Plan-only tool returns structured shell command plans.

These tests do not invoke shell commands; they verify that the planner
produces the right shape for each task.
"""

from __future__ import annotations

import pytest
from mcp.server.fastmcp import FastMCP

from postgres_postgis_docsbox_mcp.tools import run_locally as run_locally_mod


@pytest.fixture
def planner():
    """Return the run_locally tool callable."""
    mcp = FastMCP(name="t", host="127.0.0.1", port=0)
    run_locally_mod.register(mcp)
    # FastMCP exposes registered tools via _tool_manager
    tool = mcp._tool_manager.get_tool("run_locally")
    return tool.fn


@pytest.mark.asyncio
async def test_connect_plan(planner) -> None:
    out = await planner(task="connect")
    assert out["ok"] is True
    plan = out["plan"]
    assert plan["task"] == "connect"
    assert any("psql" in s["shell"] for s in plan["steps"])


@pytest.mark.asyncio
async def test_psql_query_plan(planner) -> None:
    out = await planner(task="psql_query", sql="SELECT 1")
    assert out["ok"] is True
    plan = out["plan"]
    assert plan["task"] == "psql_query"
    # SQL should be base64-injected via mktemp; should not appear raw
    joined = " ".join(s["shell"] for s in plan["steps"])
    assert "psql" in joined
    assert "SELECT 1" not in joined  # base64-encoded, not raw


@pytest.mark.asyncio
async def test_unknown_task(planner) -> None:
    out = await planner(task="not_a_task")
    assert out["ok"] is False
    assert "error" in out


@pytest.mark.asyncio
async def test_shapefile_plan(planner) -> None:
    out = await planner(task="shapefile", path="/tmp/data.shp")
    assert out["ok"] is True
    steps = out["plan"]["steps"]
    joined = " ".join(s["shell"] for s in steps)
    assert "shp2pgsql" in joined


@pytest.mark.asyncio
async def test_geojson_plan(planner) -> None:
    out = await planner(task="geojson", path="/tmp/data.geojson")
    assert out["ok"] is True
    joined = " ".join(s["shell"] for s in out["plan"]["steps"])
    assert "ogr2ogr" in joined


@pytest.mark.asyncio
async def test_psql_query_requires_sql(planner) -> None:
    out = await planner(task="psql_query")
    assert out["ok"] is False
    assert "sql" in out["error"].lower()
