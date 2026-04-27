"""Unit tests for the static SQL validator.

The validator is the most important security boundary. These tests pin
the contract: only top-level SELECT/WITH/EXPLAIN, no multi-statement, no
denylisted functions or DML/DDL, auto-LIMIT injection on bare SELECTs.
"""

from __future__ import annotations

import pytest

from postgres_postgis_docsbox_mcp.sql_validator import static_validate


# ---- Allowed shapes -------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select 1, 2, 3",
        "SELECT * FROM foo WHERE x = 1",
        "WITH a AS (SELECT 1) SELECT * FROM a",
        "EXPLAIN SELECT * FROM foo",
        "EXPLAIN (FORMAT JSON) SELECT * FROM foo",
        "  \n  SELECT 1  \n  ",
    ],
)
def test_allows_basic_selects(sql: str) -> None:
    r = static_validate(sql)
    assert r.ok, r.error


def test_auto_injects_limit() -> None:
    r = static_validate("SELECT * FROM foo")
    assert r.ok
    assert r.auto_limit_applied is True
    assert r.sql.rstrip().upper().endswith("LIMIT 500")


def test_keeps_existing_limit() -> None:
    r = static_validate("SELECT * FROM foo LIMIT 10")
    assert r.ok
    assert r.auto_limit_applied is False
    assert "LIMIT 10" in r.sql.upper()


def test_explain_does_not_get_limit() -> None:
    r = static_validate("EXPLAIN SELECT * FROM foo")
    assert r.ok
    # EXPLAIN already returns one row per plan node; LIMIT is meaningless.
    assert "LIMIT 500" not in r.sql.upper().replace(" ", "")


# ---- Forbidden shapes -----------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE foo",
        "DELETE FROM foo",
        "INSERT INTO foo VALUES (1)",
        "UPDATE foo SET x = 1",
        "ALTER TABLE foo ADD COLUMN y int",
        "TRUNCATE foo",
        "CREATE TABLE foo (x int)",
        "GRANT ALL ON foo TO bar",
        "REVOKE ALL ON foo FROM bar",
        "VACUUM foo",
        "COPY foo FROM '/tmp/x'",
        "MERGE INTO foo USING bar ON foo.x = bar.x WHEN MATCHED THEN DELETE",
        "CALL my_proc()",
        "LISTEN x",
        "NOTIFY x",
    ],
)
def test_rejects_dml_ddl(sql: str) -> None:
    r = static_validate(sql)
    assert not r.ok
    assert r.error


def test_rejects_multistatement() -> None:
    r = static_validate("SELECT 1; SELECT 2")
    assert not r.ok


def test_rejects_dangerous_functions() -> None:
    for fn in ("pg_read_file", "lo_import", "lo_export", "dblink", "pg_terminate_backend"):
        r = static_validate(f"SELECT {fn}('x')")
        assert not r.ok, fn


def test_rejects_copy_to_program() -> None:
    r = static_validate("COPY (SELECT 1) TO PROGRAM 'cat'")
    assert not r.ok


def test_rejects_garbage() -> None:
    r = static_validate("not even sql")
    assert not r.ok


def test_empty_sql_rejected() -> None:
    r = static_validate("")
    assert not r.ok
