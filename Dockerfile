FROM python:3.12-slim AS build

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY corpus ./corpus

RUN pip install --upgrade pip build \
 && pip wheel --no-deps --wheel-dir /wheels .

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PG_DOCSBOX_BIND=0.0.0.0:7820

# psycopg[binary] ships its own libpq, so no system libpq-dev required.
COPY --from=build /wheels /wheels
RUN pip install --no-deps /wheels/*.whl \
 && pip install \
        "mcp>=1.6,<2" \
        "starlette>=0.36,<0.50" \
        "uvicorn[standard]>=0.27,<0.40" \
        "httpx>=0.27,<0.30" \
        "psycopg[binary,pool]>=3.2,<4" \
        "sqlglot>=25.0,<27" \
        "pydantic>=2.10,<3" \
 && rm -rf /wheels

EXPOSE 7820

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request, sys; \
                 r=urllib.request.urlopen('http://127.0.0.1:7820/health',timeout=3); \
                 sys.exit(0 if r.status==200 else 1)" || exit 1

ENTRYPOINT ["postgres-postgis-docsbox-mcp"]
