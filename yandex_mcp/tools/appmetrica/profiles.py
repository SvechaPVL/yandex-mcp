"""Yandex AppMetrica user profiles tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import AppMetricaProfilesInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import format_appmetrica_logs_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register user profile tools."""

    @mcp.tool(
        name="appmetrica_get_profiles",
        annotations={
            "title": "Export AppMetrica User Profiles",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_profiles(params: AppMetricaProfilesInput) -> str:
        """Export user profiles from AppMetrica.

        Returns user profile data including device info, location, and
        custom profile attributes set via the SDK.
        """
        try:
            query_params: dict[str, object] = {
                "application_id": params.app_id,
                "date_since": params.date_since,
                "date_until": params.date_until,
            }

            if params.fields:
                query_params["fields"] = ",".join(params.fields)
            else:
                query_params["fields"] = ",".join([
                    "appmetrica_device_id",
                    "profile_id",
                    "os_name",
                    "device_manufacturer",
                    "device_model",
                    "city",
                    "country_iso_code",
                    "app_version_name",
                ])

            result = await api_client.appmetrica_request(
                "/logs/v1/export/profiles_v2.json",
                params=query_params,
                timeout=120.0,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_logs_markdown(result, "profiles")

        except Exception as e:
            return handle_api_error(e)
