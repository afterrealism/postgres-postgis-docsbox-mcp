"""Entrypoint so `python -m postgres_postgis_docsbox_mcp` works."""

from .server import main

if __name__ == "__main__":
    main()
