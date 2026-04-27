"""Read-only PostgreSQL/PostGIS connection helper.

Design:

* The MCP server holds a single ``psycopg_pool.ConnectionPool`` keyed by
  ``PG_DOCSBOX_DSN``. Pool is sized small (default min=1, max=4) — agents
  send a handful of introspection calls per turn, not thousands.

* Every tool acquires a connection, opens a *read-only* transaction, sets a
  short ``statement_timeout``, runs its query, then ``ROLLBACK``s. Even if
  a query mutated, the rollback would undo it. The READ ONLY flag is the
  belt; the rollback is the braces.

* ``DICT_ROW`` row factory so tools always get column names back.

* Connections inherit no environment beyond what the operator configured.
  Secrets live in ``PG_DOCSBOX_DSN`` and are never echoed.

The pool is lazily initialised so the server can boot without a database
(useful for the corpus-only doc tools).
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

DEFAULT_STATEMENT_TIMEOUT_MS = 10_000
DEFAULT_LOCK_TIMEOUT_MS = 2_000
DEFAULT_IDLE_TX_TIMEOUT_MS = 5_000


@dataclass
class PgConfig:
    dsn: str
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS
    lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS
    idle_tx_timeout_ms: int = DEFAULT_IDLE_TX_TIMEOUT_MS
    application_name: str = "postgres-postgis-docsbox-mcp"
    metadata_excludes: tuple[str, ...] = field(default_factory=tuple)


def config_from_env() -> PgConfig | None:
    """Build a config from env vars. Returns None when no DSN is set."""
    dsn = os.environ.get("PG_DOCSBOX_DSN", "").strip()
    if not dsn:
        return None
    excludes = tuple(
        s.strip()
        for s in os.environ.get("PG_DOCSBOX_METADATA_EXCLUDES", "").split(",")
        if s.strip()
    )
    return PgConfig(
        dsn=dsn,
        statement_timeout_ms=int(
            os.environ.get("PG_DOCSBOX_STATEMENT_TIMEOUT_MS", DEFAULT_STATEMENT_TIMEOUT_MS)
        ),
        lock_timeout_ms=int(os.environ.get("PG_DOCSBOX_LOCK_TIMEOUT_MS", DEFAULT_LOCK_TIMEOUT_MS)),
        idle_tx_timeout_ms=int(
            os.environ.get("PG_DOCSBOX_IDLE_TX_TIMEOUT_MS", DEFAULT_IDLE_TX_TIMEOUT_MS)
        ),
        metadata_excludes=excludes,
    )


class Database:
    """Lazy connection pool wrapper, thread-safe."""

    def __init__(self, cfg: PgConfig) -> None:
        self._cfg = cfg
        self._pool: ConnectionPool | None = None
        self._lock = threading.Lock()

    @property
    def cfg(self) -> PgConfig:
        return self._cfg

    def _ensure_pool(self) -> ConnectionPool:
        with self._lock:
            if self._pool is None:
                self._pool = ConnectionPool(
                    conninfo=self._cfg.dsn,
                    min_size=1,
                    max_size=4,
                    open=True,
                    kwargs={
                        "row_factory": dict_row,
                        "application_name": self._cfg.application_name,
                    },
                )
            return self._pool

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.close()
                self._pool = None

    @contextmanager
    def readonly(self) -> Iterator[psycopg.Connection[Any]]:
        """Yield a connection inside a READ ONLY transaction.

        The transaction is always rolled back on exit, even on success. This
        is the safety belt: a buggy tool query that managed to mutate would
        still be undone.
        """
        pool = self._ensure_pool()
        with pool.connection() as conn:
            # autocommit must be off for SET TRANSACTION to bind to the tx.
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    # SET does not accept bind parameters; coerce to int and inline.
                    stmt_to = int(self._cfg.statement_timeout_ms)
                    lock_to = int(self._cfg.lock_timeout_ms)
                    idle_to = int(self._cfg.idle_tx_timeout_ms)
                    cur.execute(f"SET LOCAL statement_timeout = {stmt_to}")
                    cur.execute(f"SET LOCAL lock_timeout = {lock_to}")
                    cur.execute(
                        f"SET LOCAL idle_in_transaction_session_timeout = {idle_to}"
                    )
                    cur.execute("SET TRANSACTION READ ONLY")
                yield conn
            finally:
                conn.rollback()


def is_metadata_excluded(table: str, cfg: PgConfig) -> bool:
    """Whether a table is in the operator-configured exclude list."""
    return table in cfg.metadata_excludes
