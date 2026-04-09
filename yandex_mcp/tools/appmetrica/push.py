"""Yandex AppMetrica Push API tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import CreatePushGroupInput, GetPushStatusInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import (
    format_appmetrica_push_group_markdown,
    format_appmetrica_push_status_markdown,
)
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register Push API tools."""

    @mcp.tool(
        name="appmetrica_create_push_group",
        annotations={
            "title": "Create AppMetrica Push Group",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    )
    async def appmetrica_create_push_group(params: CreatePushGroupInput) -> str:
        """Create a push notification group in AppMetrica.

        Groups are used to organize push message sendings.
        Each sending must be associated with a group.
        """
        try:
            result = await api_client.appmetrica_request(
                "/push/v1/management/groups",
                method="POST",
                data={
                    "group": {
                        "app_id": params.app_id,
                        "name": params.name,
                    }
                },
                use_push_api=True,
            )

            group = result.get("group", result)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_push_group_markdown(group)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="appmetrica_get_push_status",
        annotations={
            "title": "Get AppMetrica Push Transfer Status",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_push_status(params: GetPushStatusInput) -> str:
        """Check the status of a push notification sending.

        Returns current status, creation date, and any errors
        for a previously initiated push transfer.
        """
        try:
            result = await api_client.appmetrica_request(
                f"/push/v1/status/{params.transfer_id}",
                use_push_api=True,
            )

            transfer = result.get("transfer", result)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_push_status_markdown(transfer)

        except Exception as e:
            return handle_api_error(e)
