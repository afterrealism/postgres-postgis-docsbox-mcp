"""FastMCP streamable-HTTP server for postgres-postgis-docsbox-mcp.

This server gives an LLM agent safe, bounded tools for exploring and
querying a PostgreSQL+PostGIS database, plus a curated PostGIS reference
and a generic docs/manifest lookup.

Safety model
------------

* **No mutation paths** are exposed. ``execute_sql`` runs inside a
  ``SET TRANSACTION READ ONLY`` block with ``statement_timeout`` and is
  rolled back unconditionally.
* **SQL is statically validated** (sqlglot + denylist) before it is sent
  to the database; multi-statement payloads, DDL, DML, and dangerous
  functions (``pg_read_file``, ``lo_import``, ``dblink``, ...) are refused.
* **Result sizes are capped** (rows, cell bytes, payload bytes).
* **No subprocess fan-out**: the only subprocess is the optional
  ``run_locally`` tool, which is *plan-only* — it never executes.

Configuration (environment variables)
-------------------------------------

* ``PG_DOCSBOX_DSN``                — postgres URI (required for DB tools).
* ``PG_DOCSBOX_STATEMENT_TIMEOUT_MS`` (default 10000)
* ``PG_DOCSBOX_LOCK_TIMEOUT_MS``      (default 2000)
* ``PG_DOCSBOX_IDLE_TX_TIMEOUT_MS``   (default 5000)
* ``PG_DOCSBOX_METADATA_EXCLUDES``    — comma-separated ``schema.table``
                                        names to drop from listings.
* ``PG_DOCSBOX_BIND``                 (default 127.0.0.1:7820)
* ``PG_DOCSBOX_CORPUS_DIR``           — override packaged corpus.
* ``PG_DOCSBOX_DISABLE_DNS_PROTECTION`` — set to 1 in tests only.
* ``PG_DOCSBOX_ALLOWED_HOSTS`` / ``PG_DOCSBOX_ALLOWED_ORIGINS``
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse

from .corpus import Corpus, load_corpus
from .db import Database, config_from_env
from .tools import (
    docs as docs_tool,
)
from .tools import (
    execute as execute_tool,
)
from .tools import (
    introspect as introspect_tool,
)
from .tools import (
    postgis_help as postgis_help_tool,
)
from .tools import (
    run_locally as run_locally_tool,
)
from .web import landing_page, llms_full_txt, llms_txt, robots_txt, sitemap_xml

logger = logging.getLogger("postgres-postgis-docsbox-mcp")


def _default_security(host: str, port: int) -> TransportSecuritySettings:
    if os.environ.get("PG_DOCSBOX_DISABLE_DNS_PROTECTION") == "1":
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)

    extra_hosts = [
        h.strip() for h in os.environ.get("PG_DOCSBOX_ALLOWED_HOSTS", "").split(",") if h.strip()
    ]
    extra_origins = [
        o.strip() for o in os.environ.get("PG_DOCSBOX_ALLOWED_ORIGINS", "").split(",") if o.strip()
    ]
    base_hosts = [
        f"127.0.0.1:{port}",
        f"localhost:{port}",
        "127.0.0.1:*",
        "localhost:*",
        "postgres-mcp.afterrealism.com",
        "postgres-mcp.afterrealism.com:*",
    ]
    base_origins = [
        f"http://127.0.0.1:{port}",
        f"http://localhost:{port}",
        "https://postgres-mcp.afterrealism.com",
    ]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(dict.fromkeys(base_hosts + extra_hosts)),
        allowed_origins=list(dict.fromkeys(base_origins + extra_origins)),
    )


def _build_mcp(
    corpus: Corpus,
    http: httpx.AsyncClient,
    db: Database | None,
    *,
    host: str,
    port: int,
) -> FastMCP:
    mcp = FastMCP(
        name="postgres-postgis-docsbox",
        instructions=(
            "PostgreSQL+PostGIS exploration tools. All database access is "
            "read-only (SET TRANSACTION READ ONLY + statement_timeout + "
            "always-rollback). Tools: list_tables, get_table_schema, "
            "get_column_values, list_srids, get_relationships, "
            "list_extensions, pick_interesting_tables, validate_sql, "
            "explain_sql, execute_sql, postgis_help, list_sections, "
            "get_documentation, run_locally (plan-only)."
        ),
        host=host,
        port=port,
        json_response=True,
        stateless_http=True,
        transport_security=_default_security(host, port),
    )

    docs_tool.register(mcp, corpus, http)
    postgis_help_tool.register(mcp)
    introspect_tool.register(mcp, db)
    execute_tool.register(mcp, db)
    run_locally_tool.register(mcp)

    @mcp.custom_route("/", methods=["GET"])
    async def _index(_: Request) -> HTMLResponse:
        return HTMLResponse(landing_page())

    @mcp.custom_route("/health", methods=["GET"])
    async def _health(_: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "service": "postgres-postgis-docsbox-mcp",
                "db_configured": db is not None,
            }
        )

    @mcp.custom_route("/robots.txt", methods=["GET"])
    async def _robots(_: Request) -> PlainTextResponse:
        return PlainTextResponse(robots_txt(), media_type="text/plain; charset=utf-8")

    @mcp.custom_route("/sitemap.xml", methods=["GET"])
    async def _sitemap(_: Request) -> PlainTextResponse:
        return PlainTextResponse(sitemap_xml(), media_type="application/xml; charset=utf-8")

    @mcp.custom_route("/llms.txt", methods=["GET"])
    async def _llms(_: Request) -> PlainTextResponse:
        return PlainTextResponse(llms_txt(), media_type="text/markdown; charset=utf-8")

    @mcp.custom_route("/llms-full.txt", methods=["GET"])
    async def _llms_full(_: Request) -> PlainTextResponse:
        return PlainTextResponse(llms_full_txt(), media_type="text/markdown; charset=utf-8")

    return mcp


def _build_app(mcp: FastMCP) -> Any:
    return mcp.streamable_http_app()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PG_DOCSBOX_LOG", "info").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bind = os.environ.get("PG_DOCSBOX_BIND", "127.0.0.1:7820")
    host, _, port_s = bind.partition(":")
    port = int(port_s or "7820")

    corpus = load_corpus(os.environ.get("PG_DOCSBOX_CORPUS_DIR"))
    http = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        headers={"user-agent": "postgres-postgis-docsbox-mcp/0.1"},
    )

    cfg = config_from_env()
    db = Database(cfg) if cfg is not None else None
    if db is None:
        logger.warning(
            "PG_DOCSBOX_DSN not set; database tools will return not_configured. "
            "Doc tools (postgis_help, list_sections, get_documentation, validate_sql) "
            "remain available."
        )

    mcp = _build_mcp(corpus, http, db, host=host, port=port)

    logger.info("postgres-postgis-docsbox-mcp listening on %s:%d (mcp at /mcp)", host, port)
    try:
        mcp.run(transport="streamable-http")
    finally:
        import asyncio

        try:
            asyncio.run(http.aclose())
        except (RuntimeError, OSError) as exc:
            logger.debug("ignoring httpx shutdown error: %s", exc)
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
