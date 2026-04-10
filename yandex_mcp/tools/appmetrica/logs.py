"""Yandex AppMetrica Logs API tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import AppMetricaLogsExportInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import format_appmetrica_logs_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register Logs API tools."""

    @mcp.tool(
        name="appmetrica_export_logs",
        annotations={
            "title": "Export AppMetrica Logs Data",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_export_logs(params: AppMetricaLogsExportInput) -> str:
        """Export raw data from AppMetrica Logs API.

        Available export types:
        - clicks - Tracking clicks and impressions
        - installations - App installations
        - postbacks - Postback notifications
        - events - App events
        - sessions_starts - Session starts
        - crashes - App crashes
        - errors - App errors
        - push_tokens - Push tokens
        - deeplinks - Deeplink opens
        - profiles_v2 - User profiles
        - revenue_events - In-app purchases
        - ecommerce_events - E-commerce events
        - ad_revenue_events - Ad revenue events
        """
        try:
            query_params: dict[str, object] = {
                "application_id": params.app_id,
                "date_since": params.date_since,
                "date_until": params.date_until,
            }

            if params.fields:
                query_params["fields"] = ",".join(params.fields)

            result = await api_client.appmetrica_request(
                f"/logs/v1/export/{params.export_type.value}.json",
                params=query_params,
                timeout=120.0,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_logs_markdown(result, params.export_type.value)

        except Exception as e:
            return handle_api_error(e)
