"""Tools registration for Yandex MCP Server."""

from mcp.server.fastmcp import FastMCP


def register_all_tools(mcp: FastMCP) -> None:
    """Register all Direct, Metrika, Wordstat, and Webmaster tools."""
    from .direct import register_direct_tools
    from .metrika import register_metrika_tools
    from . import wordstat
    from . import webmaster

    register_direct_tools(mcp)
    register_metrika_tools(mcp)
    wordstat.register(mcp)
    webmaster.register(mcp)
