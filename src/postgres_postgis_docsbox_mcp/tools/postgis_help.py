"""postgis_help tool — return the intent-organised PostGIS reference."""

from __future__ import annotations

from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from ..postgis_reference import POSTGIS_REFERENCE


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="postgis_help",
        description=(
            "Return an intent-organised PostGIS function reference. Use this "
            "when a SELECT errored with a spatial-function complaint, when "
            "you need to choose between geometry/geography casts, or when "
            "you're picking the right function for distance, nearest, "
            "within, or buffer queries. The reference is concise (~150 "
            "lines) and bundles common gotchas with worked examples."
        ),
    )
    async def postgis_help(
        section: Annotated[
            str | None,
            Field(
                description=(
                    "Optional case-insensitive substring filter applied to "
                    "headings (e.g. 'distance', 'nearest', 'buffer'). When "
                    "omitted, returns the full reference."
                ),
            ),
        ] = None,
    ) -> dict[str, Any]:
        if not section:
            return {"reference": POSTGIS_REFERENCE, "filtered": False}
        needle = section.lower()
        keep: list[str] = []
        current: list[str] = []
        emit = False
        for line in POSTGIS_REFERENCE.splitlines():
            if line.startswith("## "):
                if emit and current:
                    keep.extend(current)
                current = [line]
                emit = needle in line.lower()
            else:
                current.append(line)
        if emit and current:
            keep.extend(current)
        body = "\n".join(keep) if keep else f"(no section title matched {section!r})"
        return {"reference": body, "filtered": True, "filter": section}
