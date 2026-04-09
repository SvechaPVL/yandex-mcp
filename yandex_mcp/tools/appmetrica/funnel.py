"""Yandex AppMetrica funnel analysis tool."""

import json
from collections import defaultdict

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import AppMetricaFunnelInput
from ...models.common import ResponseFormat
from ...formatters.appmetrica import format_appmetrica_funnel_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register funnel analysis tools."""

    @mcp.tool(
        name="appmetrica_get_funnel",
        annotations={
            "title": "Build AppMetrica Conversion Funnel",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_funnel(params: AppMetricaFunnelInput) -> str:
        """Build a conversion funnel from app events.

        Exports raw event data via Logs API and calculates conversion
        between sequential steps. Each step is an event name.
        A user counts for step N only if they also completed steps 1..N-1
        (in any order within the period).

        Example steps: ['app_open', 'view_catalog', 'add_to_cart', 'purchase']

        Note: This downloads raw event data and computes the funnel locally.
        For large date ranges or apps with many events, this may take time.
        """
        try:
            # Fetch events from Logs API with minimal fields
            query_params: dict[str, object] = {
                "application_id": params.app_id,
                "date_since": params.date_since,
                "date_until": params.date_until,
                "fields": "appmetrica_device_id,event_name,event_datetime",
            }

            result = await api_client.appmetrica_request(
                "/logs/v1/export/events.json",
                params=query_params,
                timeout=120.0,
            )

            rows = result.get("data", [])

            # Build per-user event sets
            user_events: dict[str, set[str]] = defaultdict(set)
            for row in rows:
                device_id = row.get("appmetrica_device_id", "")
                event_name = row.get("event_name", "")
                if device_id and event_name:
                    user_events[device_id].add(event_name)

            total_users = len(user_events)

            # Calculate funnel: user passes step N if they have all events 1..N
            step_users: list[int] = []
            required_events: set[str] = set()
            for step_name in params.steps:
                required_events.add(step_name)
                count = sum(
                    1 for events in user_events.values()
                    if required_events.issubset(events)
                )
                step_users.append(count)

            if params.response_format == ResponseFormat.JSON:
                funnel_data = {
                    "total_users": total_users,
                    "steps": [
                        {
                            "step": i + 1,
                            "event": name,
                            "users": users,
                            "conversion_from_prev": (
                                round(users / step_users[i - 1] * 100, 1)
                                if i > 0 and step_users[i - 1] > 0
                                else None
                            ),
                        }
                        for i, (name, users) in enumerate(
                            zip(params.steps, step_users)
                        )
                    ],
                }
                return json.dumps(funnel_data, indent=2, ensure_ascii=False)

            return format_appmetrica_funnel_markdown(
                params.steps, step_users, total_users
            )

        except Exception as e:
            return handle_api_error(e)
