"""Yandex Wordstat tools (Yandex Cloud Search API v2).

The original Wordstat API (api.wordstat.yandex.net/v1/*) was decommissioned.
These tools now call the Yandex Cloud Search API v2
(https://searchapi.api.cloud.yandex.net/v2/wordstat/*) while keeping the
original MCP tool signatures and output shapes intact:

- tool inputs still accept the v1-style values (devices "all/desktop/phone/
  tablet", period "monthly/weekly/daily", region_type "all/cities/regions",
  dates "YYYY-MM-DD") and are mapped to the v2 enums / RFC3339 timestamps;
- v2 responses are mapped back to the v1 response shape the formatters and
  downstream JSON consumers already rely on (topRequests/dynamics/regions
  keys, integer counts — v2 serializes int64 as JSON strings).
"""

import json
from mcp.server.fastmcp import FastMCP

from ..client import api_client
from ..models.common import ResponseFormat
from ..models.wordstat import (
    WordstatTopRequestsInput,
    WordstatDynamicsInput,
    WordstatRegionsInput,
    WordstatRegionsTreeInput,
    WordstatUserInfoInput,
)
from ..formatters.wordstat import (
    format_wordstat_top_requests_markdown,
    format_wordstat_dynamics_markdown,
    format_wordstat_regions_markdown,
)
from ..utils import handle_api_error


# --- v1 tool input -> v2 request mapping -----------------------------------

DEVICE_MAP = {
    "all": "DEVICE_ALL",
    "desktop": "DEVICE_DESKTOP",
    "phone": "DEVICE_PHONE",
    "tablet": "DEVICE_TABLET",
}

PERIOD_MAP = {
    "monthly": "PERIOD_MONTHLY",
    "weekly": "PERIOD_WEEKLY",
    "daily": "PERIOD_DAILY",
}

REGION_TYPE_MAP = {
    "all": "REGION_ALL",
    "cities": "REGION_CITIES",
    "regions": "REGION_REGIONS",
}


def map_devices(devices):
    """Map v1 device names to v2 enum values (v2 names pass through)."""
    mapped = []
    for device in devices:
        key = device.strip().lower()
        if key in DEVICE_MAP:
            mapped.append(DEVICE_MAP[key])
        elif device.strip() in DEVICE_MAP.values():
            mapped.append(device.strip())
        else:
            raise ValueError(
                f"Unknown device type: {device!r}. "
                "Allowed: all, desktop, phone, tablet."
            )
    return mapped


def map_period(period):
    """Map v1 period name to the v2 enum value (v2 names pass through)."""
    key = period.strip().lower()
    if key in PERIOD_MAP:
        return PERIOD_MAP[key]
    if period.strip() in PERIOD_MAP.values():
        return period.strip()
    raise ValueError(
        f"Unknown period: {period!r}. Allowed: monthly, weekly, daily."
    )


def map_region_type(region_type):
    """Map v1 region_type to the v2 enum value (v2 names pass through)."""
    key = region_type.strip().lower()
    if key in REGION_TYPE_MAP:
        return REGION_TYPE_MAP[key]
    if region_type.strip() in REGION_TYPE_MAP.values():
        return region_type.strip()
    raise ValueError(
        f"Unknown region_type: {region_type!r}. Allowed: all, cities, regions."
    )


def date_to_rfc3339(date_str):
    """v2 expects google.protobuf.Timestamp (RFC3339), not bare YYYY-MM-DD."""
    return f"{date_str}T00:00:00Z"


# --- v2 response -> v1 response shape mapping -------------------------------
# NOTE on .get(..., 0): proto3 JSON omits zero-valued fields, so a missing
# count/share legitimately means 0 — this is protocol semantics, not a guess.
# int64 values arrive as JSON strings ("178254") and must be coerced to int.

def _phrase_items(items):
    return [
        {"phrase": item.get("phrase", ""), "count": int(item.get("count", 0))}
        for item in items
    ]


def map_top_requests_response(phrase, result):
    """v2 {results, associations, totalCount} -> v1 topRequests shape."""
    return {
        "requestPhrase": phrase,
        "totalCount": int(result.get("totalCount", 0)),
        "topRequests": _phrase_items(result.get("results", [])),
        "associations": _phrase_items(result.get("associations", [])),
    }


def map_dynamics_response(result):
    """v2 {results: [{date, count, share}]} -> v1 {dynamics: [...]}."""
    return {
        "dynamics": [
            {
                # v2 date is an RFC3339 timestamp; v1 exposed a plain date.
                "date": str(item.get("date", ""))[:10],
                "count": int(item.get("count", 0)),
                "share": float(item.get("share", 0.0)),
            }
            for item in result.get("results", [])
        ]
    }


def map_regions_response(result):
    """v2 {results: [{region, ...}]} -> v1 {regions: [{regionId, ...}]}."""
    return {
        "regions": [
            {
                "regionId": item.get("region", ""),
                "count": int(item.get("count", 0)),
                "share": float(item.get("share", 0.0)),
                "affinityIndex": float(item.get("affinityIndex", 0.0)),
            }
            for item in result.get("results", [])
        ]
    }


def register(mcp: FastMCP) -> None:
    """Register Wordstat tools."""

    @mcp.tool(
        name="wordstat_top_requests",
        annotations={
            "title": "Get Wordstat Top Requests",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def wordstat_top_requests(params: WordstatTopRequestsInput) -> str:
        """Get popular search queries from Yandex Wordstat.

        Returns top requests and associated queries for given phrases.
        Provide either 'phrase' (single) or 'phrases' (multiple, max 128).
        """
        try:
            phrases = params.phrases or ([params.phrase] if params.phrase else [])
            if not phrases:
                return "Error: provide either 'phrase' or 'phrases' parameter."

            base_data = {"numPhrases": params.num_phrases}
            if params.regions is not None:
                # v2 expects region IDs as strings
                base_data["regions"] = [str(region) for region in params.regions]
            if params.devices is not None:
                base_data["devices"] = map_devices(params.devices)

            # v2 accepts a single phrase per request (v1 supported batches),
            # so batch input is preserved by issuing one request per phrase.
            results = []
            for phrase in phrases:
                try:
                    raw = await api_client.wordstat_request(
                        "/topRequests", {"phrase": phrase, **base_data}
                    )
                    results.append(map_top_requests_response(phrase, raw))
                except Exception as phrase_error:  # surfaced per phrase, as in v1
                    results.append({
                        "requestPhrase": phrase,
                        "error": handle_api_error(phrase_error),
                    })

            result = results[0] if len(results) == 1 else results

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_wordstat_top_requests_markdown(result)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="wordstat_dynamics",
        annotations={
            "title": "Get Wordstat Query Dynamics",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def wordstat_dynamics(params: WordstatDynamicsInput) -> str:
        """Get query frequency dynamics over time from Yandex Wordstat.

        Shows how search query popularity changes over a specified time period.
        Only the + operator is allowed in the phrase.
        Note (API v2): for period=monthly toDate must be the last day of a
        month; for period=weekly it must be a Sunday.
        """
        try:
            data = {
                "phrase": params.phrase,
                "period": map_period(params.period),
                "fromDate": date_to_rfc3339(params.from_date),
            }
            if params.to_date is not None:
                data["toDate"] = date_to_rfc3339(params.to_date)
            if params.regions is not None:
                data["regions"] = [str(region) for region in params.regions]
            if params.devices is not None:
                data["devices"] = map_devices(params.devices)

            raw = await api_client.wordstat_request("/dynamics", data)
            result = map_dynamics_response(raw)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_wordstat_dynamics_markdown(result)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="wordstat_regions",
        annotations={
            "title": "Get Wordstat Regional Distribution",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def wordstat_regions(params: WordstatRegionsInput) -> str:
        """Get regional distribution of search queries from Yandex Wordstat.

        Shows how query popularity varies across different regions.
        """
        try:
            data = {
                "phrase": params.phrase,
                "region": map_region_type(params.region_type),
            }
            if params.devices is not None:
                data["devices"] = map_devices(params.devices)

            raw = await api_client.wordstat_request("/regions", data)
            result = map_regions_response(raw)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_wordstat_regions_markdown(result)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="wordstat_regions_tree",
        annotations={
            "title": "Get Wordstat Regions Tree",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def wordstat_regions_tree(params: WordstatRegionsTreeInput) -> str:
        """Get the full regions tree from Yandex Wordstat.

        Returns hierarchical structure of all available regions with their IDs.
        Useful for finding region IDs to use in other Wordstat tools.
        """
        try:
            result = await api_client.wordstat_request("/getRegionsTree")

            return json.dumps(result, indent=2, ensure_ascii=False)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="wordstat_user_info",
        annotations={
            "title": "Get Wordstat User Info",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def wordstat_user_info(params: WordstatUserInfoInput) -> str:
        """Get Wordstat user quota information.

        The v1 /userInfo endpoint was decommissioned together with
        api.wordstat.yandex.net; Yandex Cloud Search API v2 has no
        user-quota endpoint. Quotas are managed in the Yandex Cloud console.
        """
        return (
            "Error: the Wordstat v1 endpoint /v1/userInfo was decommissioned "
            "together with api.wordstat.yandex.net. Yandex Cloud Search API v2 "
            "has no user-quota endpoint. Check Search API quotas and usage in "
            "the Yandex Cloud console (Quota Manager / Search API) instead."
        )
