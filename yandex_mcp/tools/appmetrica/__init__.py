"""Yandex AppMetrica tools registration."""

from mcp.server.fastmcp import FastMCP


def register_appmetrica_tools(mcp: FastMCP) -> None:
    """Register all Yandex AppMetrica tools."""
    from . import (
        applications,
        reports,
        logs,
        events,
        crashes,
        profiles,
        funnel,
        push,
    )

    applications.register(mcp)
    reports.register(mcp)
    logs.register(mcp)
    events.register(mcp)
    crashes.register(mcp)
    profiles.register(mcp)
    funnel.register(mcp)
    push.register(mcp)
