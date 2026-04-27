# postgres-postgis-docsbox-mcp

A Model Context Protocol (MCP) server giving an LLM agent **safe, bounded
read-only access** to a PostgreSQL + PostGIS database, plus a curated
PostGIS reference and a docs/manifest lookup.

Sibling project to [`python-docsbox-mcp`](../python-docsbox-mcp) and
[`rust-docsbox-mcp`](../rust-docsbox-mcp); shares the same FastMCP
streamable-HTTP shape.

## Why

Generic `psql` MCPs run user-authored SQL with full ambient permissions.
That's a footgun: an agent can `DROP TABLE` mid-conversation, or `COPY
... TO PROGRAM 'rm -rf'`. This server takes the opposite stance.

Defence in depth:

1. **Static SQL validation** — sqlglot parse + denylist of statement
   kinds, dangerous functions, and multi-statement payloads. Refuses
   anything that isn't top-level `SELECT` / `WITH` / `EXPLAIN`. Auto-injects
   `LIMIT` if missing.
2. **Read-only transaction** — every query runs inside `SET TRANSACTION
   READ ONLY` with `statement_timeout`, `lock_timeout`, and
   `idle_in_transaction_session_timeout` bounded; rollback is unconditional.
3. **Bounded results** — row caps, per-cell byte caps, geometry wrapped
   to GeoJSON for inspection.
4. **No process fan-out** — the only subprocess-shaped tool is
   `run_locally`, which is plan-only.

## Tools

| Tool | Purpose |
|------|---------|
| `list_tables` | User tables/views with row estimate, geometry flag, SRID |
| `get_table_schema` | DDL + sample rows + indexes |
| `get_column_values` | Distinct sample values for a column |
| `list_srids` | SRIDs in active use, with column counts |
| `get_relationships` | Foreign-key edges |
| `list_extensions` | Installed extensions (PostGIS, pgvector, ...) |
| `pick_interesting_tables` | Score by rows + geom density + FK hubness |
| `validate_sql` | Static safety check (no DB needed) |
| `explain_sql` | EXPLAIN [ANALYZE] in a rolled-back tx |
| `execute_sql` | SELECT/WITH/EXPLAIN with row caps |
| `postgis_help` | Curated PostGIS recipe reference |
| `list_sections` / `get_documentation` | Doc manifest browser |
| `run_locally` | Plan-only execution recipes (psql, pg_dump, ogr2ogr) |

Each tool's docstring carries a worked example with the expected JSON shape;
the agent doesn't have to guess.

## Quick start

### 1. Spin up Postgres + PostGIS (Docker)

```sh
docker run -d --name postgis-docsbox \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  postgis/postgis:17-3.5
```

### 2. Load the sample dataset

```sh
PGPASSWORD=secret psql -h 127.0.0.1 -U postgres -d postgres \
  -f examples/sample_data.sql
```

### 3. Run the MCP server

```sh
pip install -e .
export PG_DOCSBOX_DSN='postgresql://postgres:secret@127.0.0.1:5432/postgres'
postgres-postgis-docsbox-mcp
# listening on 127.0.0.1:7820 (mcp at /mcp)
```

### 4. Wire into your client

`opencode`:

```json
{
  "mcp": {
    "postgres-postgis-docsbox": {
      "type": "remote",
      "url": "http://127.0.0.1:7820/mcp",
      "enabled": true
    }
  }
}
```

Claude Code (`~/.claude.json`):

```json
{
  "mcpServers": {
    "postgres-postgis-docsbox": {
      "type": "http",
      "url": "http://127.0.0.1:7820/mcp"
    }
  }
}
```

## Worked examples (against the sample dataset)

```
list_extensions()
-> {"ok": true, "extensions": [
     {"name":"plpgsql","version":"1.0","schema":"pg_catalog"},
     {"name":"postgis","version":"3.5.0","schema":"public"}
   ]}

list_tables()
-> {"ok": true, "tables": [
     {"schema":"public","name":"hospitals","kind":"table","row_estimate":4,
      "has_geom":true,"geom_column":"geom","srid":4326,"geom_type":"POINT"},
     {"schema":"public","name":"schools","kind":"table","row_estimate":8,
      "has_geom":true,"geom_column":"geom","srid":4326,"geom_type":"POINT"},
     {"schema":"public","name":"suburbs","kind":"table","row_estimate":5,
      "has_geom":true,"geom_column":"geom","srid":4326,"geom_type":"MULTIPOLYGON"}
   ], "count": 3}

pick_interesting_tables(limit=3)
-> top tables ordered by log10(rows) + spatial-index/FK-hub bonuses,
   each with a GeoJSON extent.

get_table_schema(table='public.suburbs', sample_rows=2)
-> DDL + columns + sample rows with geometry as GeoJSON.

execute_sql(sql=
  'SELECT s.name AS school, sub.name AS suburb '
  'FROM schools s JOIN suburbs sub ON ST_Contains(sub.geom, s.geom) '
  'ORDER BY school')
-> rows pairing each school with the suburb whose polygon contains it.

postgis_help(section='nearest')
-> the "nearest k" recipe (use KNN <-> on a GiST-indexed geometry).
```

## Configuration

| Var | Default | Purpose |
|-----|---------|---------|
| `PG_DOCSBOX_DSN` | (required) | postgres URI |
| `PG_DOCSBOX_STATEMENT_TIMEOUT_MS` | 10000 | per-statement bound |
| `PG_DOCSBOX_LOCK_TIMEOUT_MS` | 2000 | lock-wait bound |
| `PG_DOCSBOX_IDLE_TX_TIMEOUT_MS` | 5000 | idle-in-tx bound |
| `PG_DOCSBOX_METADATA_EXCLUDES` | "" | comma-separated `schema.table` to hide |
| `PG_DOCSBOX_BIND` | 127.0.0.1:7820 | host:port |
| `PG_DOCSBOX_CORPUS_DIR` | packaged | override doc manifest dir |
| `PG_DOCSBOX_DISABLE_DNS_PROTECTION` | unset | tests only |

### Recommended Postgres-side hardening

Even though the server forces `READ ONLY`, give it a role that physically
cannot mutate:

```sql
CREATE ROLE docsbox LOGIN PASSWORD 'secret';
GRANT pg_read_all_data TO docsbox;
```

## Docker

```sh
docker build -t postgres-postgis-docsbox-mcp .
docker run --rm -p 7820:7820 \
  -e PG_DOCSBOX_DSN='postgresql://docsbox:secret@host.docker.internal:5432/mydb' \
  postgres-postgis-docsbox-mcp
```

## Development

```sh
pip install -e '.[dev]'
pytest -q
ruff check src tests
```

## License

MIT
