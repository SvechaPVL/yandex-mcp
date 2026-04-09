"""Yandex AppMetrica crash analytics tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import AppMetricaCrashesInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import format_appmetrica_crashes_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register crash analytics tools."""

    @mcp.tool(
        name="appmetrica_get_crashes",
        annotations={
            "title": "Get AppMetrica Crash Statistics",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_crashes(params: AppMetricaCrashesInput) -> str:
        """Get crash statistics from AppMetrica.

        Returns crash counts grouped by specified dimensions.
        Default grouping: crash group name.

        Common dimensions for group_by:
        - ym:cr:crashGroupName — crash group/type
        - ym:cr:operatingSystemInfo — OS version
        - ym:cr:appVersion — app version
        - ym:cr:mobileDeviceBranding — device manufacturer
        - ym:cr:mobileDeviceModel — device model
        """
        try:
            dimensions = params.group_by or ["ym:cr:crashGroupName"]

            query_params: dict[str, object] = {
                "id": params.app_id,
                "metrics": "ym:cr:crashes",
                "dimensions": ",".join(dimensions),
                "sort": "-ym:cr:crashes",
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

            return format_appmetrica_crashes_markdown(result)

        except Exception as e:
            return handle_api_error(e)
