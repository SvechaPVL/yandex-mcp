"""Yandex AppMetrica application management tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import GetApplicationsInput, GetApplicationInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import (
    format_appmetrica_applications_markdown,
    format_appmetrica_application_markdown,
)
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register application management tools."""

    @mcp.tool(
        name="appmetrica_get_applications",
        annotations={
            "title": "Get AppMetrica Applications",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_applications(params: GetApplicationsInput) -> str:
        """Get list of all AppMetrica applications available to the user."""
        try:
            result = await api_client.appmetrica_request(
                "/management/v1/applications",
            )

            apps = result.get("applications", [])

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_applications_markdown(apps)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="appmetrica_get_application",
        annotations={
            "title": "Get AppMetrica Application Details",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_application(params: GetApplicationInput) -> str:
        """Get details of a specific AppMetrica application by ID."""
        try:
            result = await api_client.appmetrica_request(
                f"/management/v1/application/{params.app_id}",
            )

            app = result.get("application", result)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_application_markdown(app)

        except Exception as e:
            return handle_api_error(e)
