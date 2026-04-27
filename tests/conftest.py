"""Shared pytest fixtures.

Tests fall into two categories:

* **Unit tests** for the SQL validator, plan templates, and corpus loader.
  Run without a database.
* **Integration tests** marked with the ``db`` marker that require
  ``PG_DOCSBOX_DSN`` to point at a live Postgres+PostGIS instance with
  ``examples/sample_data.sql`` loaded.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-db",
        action="store_true",
        default=False,
        help="run integration tests that need PG_DOCSBOX_DSN.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-db"):
        return
    skip_db = pytest.mark.skip(
        reason="needs --run-db and PG_DOCSBOX_DSN pointing at sample DB"
    )
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip_db)


@pytest.fixture
def database() -> Iterator:
    """Yield a Database wired to PG_DOCSBOX_DSN, or skip if unset."""
    from postgres_postgis_docsbox_mcp.db import Database, config_from_env

    cfg = config_from_env()
    if cfg is None:
        pytest.skip("PG_DOCSBOX_DSN not set")
    db = Database(cfg)
    try:
        yield db
    finally:
        db.close()
