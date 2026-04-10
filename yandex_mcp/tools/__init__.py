"""Tools registration for Yandex MCP Server."""

from mcp.server.fastmcp import FastMCP


def register_all_tools(mcp: FastMCP) -> None:
    """Register all Direct, Metrika, Wordstat, and AppMetrica tools."""
    from .direct import register_direct_tools
    from .metrika import register_metrika_tools
    from .appmetrica import register_appmetrica_tools
    from . import wordstat

    register_direct_tools(mcp)
    register_metrika_tools(mcp)
    wordstat.register(mcp)
    register_appmetrica_tools(mcp)
