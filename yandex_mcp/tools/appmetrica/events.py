"""Yandex AppMetrica event analytics tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import AppMetricaEventsInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import format_appmetrica_events_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register event analytics tools."""

    @mcp.tool(
        name="appmetrica_get_events",
        annotations={
            "title": "Get AppMetrica Event Statistics",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_events(params: AppMetricaEventsInput) -> str:
        """Get list of app events with user and event counts.

        Returns all events sent to AppMetrica with the number of unique users
        and total event count for each event name. Sorted by user count descending.
        """
        try:
            query_params: dict[str, object] = {
                "id": params.app_id,
                "metrics": "ym:ce:users",
                "dimensions": "ym:ce:eventLabel",
                "sort": "-ym:ce:users",
                "limit": params.limit,
            }

            if params.date1:
                query_params["date1"] = params.date1
            if params.date2:
                query_params["date2"] = params.date2

            result = await api_client.appmetrica_request(
                "/stat/v1/data",
                params=query_params,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_events_markdown(result)

        except Exception as e:
            return handle_api_error(e)
