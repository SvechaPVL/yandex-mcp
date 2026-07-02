"""Yandex Webmaster API v4 tools."""

import json
from urllib.parse import quote

from mcp.server.fastmcp import FastMCP

from ..client import api_client
from ..models.common import ResponseFormat
from ..models.webmaster import (
    WebmasterUserIdInput,
    WebmasterHostsInput,
    WebmasterHostSummaryInput,
    WebmasterPopularQueriesInput,
    WebmasterAddHostInput,
    WebmasterDeleteHostInput,
    WebmasterGetVerificationInput,
    WebmasterVerifyHostInput,
    WebmasterListSitemapsInput,
    WebmasterAddSitemapInput,
    WebmasterDeleteSitemapInput,
    WebmasterRecrawlUrlInput,
    WebmasterRecrawlQuotaInput,
)
from ..formatters.webmaster import (
    format_webmaster_hosts_markdown,
    format_webmaster_summary_markdown,
    format_webmaster_popular_queries_markdown,
    format_webmaster_verification_markdown,
    format_webmaster_sitemaps_markdown,
    format_webmaster_recrawl_quota_markdown,
)
from ..utils import handle_api_error

_READ_ONLY = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}

_WRITE_SAFE = {
    "readOnlyHint": False,
    "destructiveHint": False,
    "idempotentHint": False,
    "openWorldHint": False,
}

_WRITE_DESTRUCTIVE = {
    "readOnlyHint": False,
    "destructiveHint": True,
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
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/summary/"
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
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/search-queries/popular/",
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

    # -------------------------------------------------------------------
    # Host management (add / delete / verification)
    # -------------------------------------------------------------------

    @mcp.tool(
        name="webmaster_add_host",
        annotations={"title": "Add Webmaster Host", **_WRITE_SAFE},
    )
    async def webmaster_add_host(params: WebmasterAddHostInput) -> str:
        """Add a new site (host) to Yandex Webmaster.

        Returns the new 'host_id' (format 'scheme:domain:port'), which is
        required by every other Webmaster tool. The host still needs to be
        verified afterwards via webmaster_verify_host before most data
        (search queries, summary, sitemaps) becomes available.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/",
                method="POST",
                json_body={"host_url": params.host_url},
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return (
                f"Host added: `{result.get('host_id', 'N/A')}`.\n\n"
                "Run webmaster_get_verification / webmaster_verify_host next "
                "to confirm ownership."
            )
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_delete_host",
        annotations={"title": "Delete Webmaster Host", **_WRITE_DESTRUCTIVE},
    )
    async def webmaster_delete_host(params: WebmasterDeleteHostInput) -> str:
        """Permanently remove a host from Yandex Webmaster.

        WARNING - DESTRUCTIVE: this deletes all history/statistics Webmaster
        collected for the host from this account. It does NOT affect the live
        site or Yandex's search index. Re-adding the same host later starts
        from a clean state and requires re-verification. Double-check
        'host_id' against webmaster_get_hosts before calling this.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/", method="DELETE"
            )
            return f"Host `{params.host_id}` deleted from Webmaster."
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_get_verification",
        annotations={"title": "Get Webmaster Host Verification Status", **_READ_ONLY},
    )
    async def webmaster_get_verification(params: WebmasterGetVerificationInput) -> str:
        """Get the current rights-verification status of a host.

        Returns 'verification_state' (NONE, VERIFIED, IN_PROGRESS,
        VERIFICATION_FAILED, INTERNAL_ERROR), the confirmation code
        ('verification_uin' - needed for the DNS TXT record or HTML_FILE/
        META_TAG instructions) and the verification methods applicable to
        this host.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/verification/"
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_verification_markdown(result)
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_verify_host",
        annotations={"title": "Start Webmaster Host Verification", **_WRITE_SAFE},
    )
    async def webmaster_verify_host(params: WebmasterVerifyHostInput) -> str:
        """Start (or re-check) the rights-verification procedure for a host.

        Call webmaster_get_verification first to see 'verification_uin' and
        'applicable_verifiers' for the host, place the required DNS TXT
        record / HTML file / meta tag, THEN call this tool to trigger the
        actual check. Yandex performs the check asynchronously - poll
        webmaster_get_verification afterwards for the result.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/verification/",
                method="POST",
                params={"verification_type": params.verification_type},
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_verification_markdown(result)
        except Exception as e:
            return handle_api_error(e)

    # -------------------------------------------------------------------
    # User-added sitemaps
    # -------------------------------------------------------------------

    @mcp.tool(
        name="webmaster_list_sitemaps",
        annotations={"title": "List Webmaster User-Added Sitemaps", **_READ_ONLY},
    )
    async def webmaster_list_sitemaps(params: WebmasterListSitemapsInput) -> str:
        """List Sitemap files added by the user for a host.

        Use 'limit' (max 100) and 'offset' (a sitemap_id cursor, '0' for the
        first page) to paginate.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/user-added-sitemaps/",
                params={"limit": params.limit, "from": params.offset},
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_sitemaps_markdown(result)
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_add_sitemap",
        annotations={"title": "Add Webmaster Sitemap", **_WRITE_SAFE},
    )
    async def webmaster_add_sitemap(params: WebmasterAddSitemapInput) -> str:
        """Register a Sitemap file (e.g. sitemap.xml) for a host.

        The URL must already be publicly reachable on the host. Returns the
        new 'sitemap_id' (see webmaster_delete_sitemap).
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/user-added-sitemaps/",
                method="POST",
                json_body={"url": params.sitemap_url},
            )
            return f"Sitemap added: `{result.get('sitemap_id', 'N/A')}` ({params.sitemap_url})"
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_delete_sitemap",
        annotations={"title": "Delete Webmaster Sitemap", **_WRITE_DESTRUCTIVE},
    )
    async def webmaster_delete_sitemap(params: WebmasterDeleteSitemapInput) -> str:
        """Remove a user-added Sitemap file from a host.

        WARNING - DESTRUCTIVE: Yandex stops using this sitemap as a URL
        source for crawling. It does NOT delete the file from the site and
        does NOT de-index pages already discovered through it. Get
        'sitemap_id' from webmaster_list_sitemaps.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            sitemap = quote(params.sitemap_id, safe="")
            await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/user-added-sitemaps/{sitemap}/",
                method="DELETE",
            )
            return f"Sitemap `{params.sitemap_id}` deleted from host `{params.host_id}`."
        except Exception as e:
            return handle_api_error(e)

    # -------------------------------------------------------------------
    # Recrawl
    # -------------------------------------------------------------------

    @mcp.tool(
        name="webmaster_recrawl_url",
        annotations={"title": "Submit Webmaster URL For Recrawl", **_WRITE_SAFE},
    )
    async def webmaster_recrawl_url(params: WebmasterRecrawlUrlInput) -> str:
        """Submit a single page URL for priority re-crawl by Yandex.

        The host must already be verified. Consumes part of the host's daily
        recrawl quota (see webmaster_recrawl_quota) - check the remaining
        quota before submitting many URLs.
        """
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/recrawl/queue/",
                method="POST",
                json_body={"url": params.url},
            )
            return (
                f"Recrawl task queued: `{result.get('task_id', 'N/A')}`. "
                f"Quota remaining today: {result.get('quota_remainder', 'N/A')}."
            )
        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="webmaster_recrawl_quota",
        annotations={"title": "Get Webmaster Recrawl Quota", **_READ_ONLY},
    )
    async def webmaster_recrawl_quota(params: WebmasterRecrawlQuotaInput) -> str:
        """Get the daily recrawl quota and remaining balance for a host."""
        try:
            user_id = params.user_id or await api_client.webmaster_user_id()
            host = quote(params.host_id, safe=":")
            result = await api_client.webmaster_request(
                f"/user/{user_id}/hosts/{host}/recrawl/quota/"
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)
            return format_webmaster_recrawl_quota_markdown(result)
        except Exception as e:
            return handle_api_error(e)
