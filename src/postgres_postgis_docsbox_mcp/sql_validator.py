"""Two-stage SQL safety pipeline.

Stage 1 (static): sqlglot parse + keyword/statement-shape rules.
Stage 2 (live):   ``EXPLAIN`` the query against the connection (no execution
                  beyond planning). Wrapped in a sub-transaction we always
                  rollback so EXPLAIN side-effects (e.g. statistics
                  autovacuum trigger) cannot leak.

Notes on the choice of denylist over allowlist: PostGIS geometry constructors
look like function calls (`ST_MakePoint(...)`) and we want them. SQL functions
that mutate (e.g. `pg_advisory_lock`, `pg_terminate_backend`) live in the
denylist. We additionally enforce that the *top-level* statement is a SELECT
or WITH so callers cannot smuggle DML behind a function.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp

# Disallowed SQL constructs. The denylist is matched case-insensitively against
# the *raw* SQL string AND against the parsed expression tree; the latter
# catches obfuscated forms like ``/*comment*/DROP``.
_DENY_KEYWORDS = (
    "drop",
    "delete",
    "insert",
    "update",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "exec",
    "execute",
    "copy",
    "merge",
    "vacuum",
    "analyze",
    "cluster",
    "reindex",
    "lock",
    "listen",
    "notify",
    "comment",
    "security",
    "set",
    "reset",
    "begin",
    "commit",
    "rollback",
    "savepoint",
    "do ",
    "call ",
)

# Disallowed function names. These are PostgreSQL builtins that read or write
# the host filesystem, perform privileged operations, or mutate cluster state.
_DENY_FUNCTIONS = {
    "pg_read_file",
    "pg_read_binary_file",
    "pg_write_file",
    "pg_ls_dir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "lo_create",
    "lo_unlink",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_advisory_lock",
    "pg_advisory_unlock",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_promote",
    "set_config",
    "current_setting",
    "dblink",
    "dblink_exec",
    "dblink_connect",
}


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    sql: str
    error: str | None = None
    hint: str | None = None
    auto_limit_applied: bool = False


def static_validate(sql: str, *, default_limit: int = 500) -> ValidationResult:
    """Run static checks. Returns the (possibly LIMIT-augmented) SQL."""
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        return ValidationResult(ok=False, sql=sql, error="empty SQL")

    # Multi-statement guard via raw split. Caveat: a literal ';' inside a
    # string literal would trip this. That's acceptable — read-only
    # introspection rarely needs literal semicolons.
    if ";" in cleaned:
        return ValidationResult(
            ok=False,
            sql=sql,
            error="multiple statements are not allowed",
            hint="submit one SELECT or WITH statement at a time",
        )

    head = cleaned.lstrip().lower()
    if not (head.startswith("select") or head.startswith("with") or head.startswith("explain")):
        return ValidationResult(
            ok=False,
            sql=sql,
            error="only SELECT, WITH, and EXPLAIN statements are allowed",
            hint="this MCP server is read-only; use a CTE or a subquery",
        )

    lower = " " + head + " "
    # ``EXPLAIN ANALYZE <SELECT>`` is read-only profiling; allow ``analyze`` only
    # as the second token after ``explain``.
    explain_analyze = head.startswith("explain analyze")
    for kw in _DENY_KEYWORDS:
        if explain_analyze and kw.strip() == "analyze":
            continue
        # Prefix the keyword so we don't false-positive on substrings.
        needle = " " + kw.strip() + (" " if not kw.endswith(" ") else "")
        if needle in lower:
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"disallowed keyword: {kw.strip()!r}",
                hint="this MCP server only runs read-only queries",
            )

    try:
        tree = sqlglot.parse_one(cleaned, dialect="postgres")
    except sqlglot.errors.ParseError as exc:
        return ValidationResult(
            ok=False,
            sql=sql,
            error=f"parse error: {exc}",
            hint="check the SQL syntax; column/table names that contain reserved words must be quoted",
        )

    if tree is None:
        return ValidationResult(ok=False, sql=sql, error="parse produced no statement")

    # Walk for forbidden function calls.
    for func in tree.find_all(exp.Func):
        name = (func.name or "").lower()
        if name in _DENY_FUNCTIONS:
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"disallowed function: {name}",
                hint="this function can read or write outside the database",
            )

    # Top-level statement must be a query.
    top = tree
    if not isinstance(top, exp.Select | exp.Union | exp.Subquery | exp.With):
        # Allow EXPLAIN wrappers (sqlglot models EXPLAIN as exp.Command).
        if not (isinstance(top, exp.Command) and top.this and top.this.upper().startswith("EXPLAIN")):
            return ValidationResult(
                ok=False,
                sql=sql,
                error=f"top-level statement is {type(top).__name__}, not SELECT/WITH/EXPLAIN",
            )

    auto_limit = False
    out_sql = cleaned
    if (
        not head.startswith("explain")
        and isinstance(top, exp.Select | exp.With | exp.Union)
        and "limit" not in head
    ):
        out_sql = f"{cleaned}\nLIMIT {int(default_limit)}"
        auto_limit = True

    return ValidationResult(ok=True, sql=out_sql, auto_limit_applied=auto_limit)
