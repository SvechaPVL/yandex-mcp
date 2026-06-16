"""Yandex Webmaster API v4 tools."""

import json
from mcp.server.fastmcp import FastMCP

from ..client import api_client
from ..models.common import ResponseFormat
from ..models.webmaster import (
    WebmasterUserIdInput,
    WebmasterHostsInput,
    WebmasterHostSummaryInput,
    WebmasterPopularQueriesInput,
)
from ..formatters.webmaster import (
    format_webmaster_hosts_markdown,
    format_webmaster_summary_markdown,
    format_webmaster_popular_queries_markdown,
)
from ..utils import handle_api_error

_READ_ONLY = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}


def register(mcp: FastMCP) -> None:
    """Register Webmaster tools."""

    @mcp.tool(
        name="webmaster_get_user_id",
        annotations={"title": "Get Webmaster UserID", **_READ_ONLY},
    )
    async def webmaster_get_user_id(params: WebmasterUserIdInput) -> str:
        """Get the Yandex Webmaster UserID of the token owner.

        This UserID is required by the other Webmaster tools (they resolve it
        automatically when 'user_id' is omitted).
        """
        try:
            result = await api_client.webmaster_request("/user/")
            return json.dumps(result, indent=2, ensure_ascii=False)
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_get_hosts",
        annotations={"title": "Get Webmaster Hosts", **_READ_ONLY},
    )
    async def webmaster_get_hosts(params: WebmasterHostsInput) -> str:
        """List sites (hosts) added to Yandex Webmaster.

        Returns each host's ID (needed for other tools) and verification status.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            result = await api_client.webmaster_request(f"/user/{user_id}/hosts/")

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_hosts_markdown(result)
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_get_host_summary",
        annotations={"title": "Get Webmaster Host Summary", **_READ_ONLY},
    )
    async def webmaster_get_host_summary(params: WebmasterHostSummaryInput) -> str:
        """Get a host summary: SQI (ИКС), searchable and excluded page counts.

        Useful as an organic-search baseline for an SEO overhaul.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{params.host_id}/summary/"
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_summary_markdown(result)
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_get_popular_queries",
        annotations={"title": "Get Webmaster Popular Queries", **_READ_ONLY},
    )
    async def webmaster_get_popular_queries(params: WebmasterPopularQueriesInput) -> str:
        """Get popular organic search queries for a host.

        Returns shows, clicks and average positions per query. Use 'limit' and
        'offset' to paginate (max 500 per page).
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{params.host_id}/search-queries/popular/",
                params={
                    "order_by": params.order_by,
                    "query_indicator": params.query_indicator,
                    "limit": params.limit,
                    "offset": params.offset,
                },
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_popular_queries_markdown(result)
        except Exception as e:
            return handle_api_error(e)
