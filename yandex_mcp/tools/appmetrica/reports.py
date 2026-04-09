"""Yandex AppMetrica reporting tools."""

import json

from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.appmetrica import (
    AppMetricaReportInput,
    AppMetricaByTimeInput,
    AppMetricaDrilldownInput,
)
from ...models.common import ResponseFormat
from ...formatters.appmetrica import (
    format_appmetrica_report_markdown,
    format_appmetrica_drilldown_markdown,
)
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register reporting tools."""

    @mcp.tool(
        name="appmetrica_get_report",
        annotations={
            "title": "Get AppMetrica Statistics Report",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_report(params: AppMetricaReportInput) -> str:
        """Get statistics report from AppMetrica.

        Retrieves app analytics with customizable metrics and dimensions.
        All metrics/dimensions in one request must share the same prefix.

        Available metrics by prefix:
        - ym:ge: (general) — users, sessions
        - ym:ce: (events) — users
        - ym:i: (installs) — users, devices, installDevices
        - ym:cr: (crashes) — users, crashes, crashDevices
        - ym:s: (sessions) — users
        - ym:u: (uninstalls) — users, devices
        - ym:p: (push) — users, devices

        Dimensions by prefix:
        - ym:ge: — date, regionCountry, regionCity, operatingSystemInfo,
          mobileDeviceBranding, mobileDeviceModel, appVersion, gender,
          ageInterval, screenResolution
        - ym:ce: — eventLabel, eventType
        - ym:i: — date, regionCountry, operatingSystemInfo,
          mobileDeviceBranding, appVersion, publisher
        - ym:cr: — date, operatingSystemInfo, appVersion,
          crashGroupName, mobileDeviceBranding
        - ym:s: — date, regionCountry, operatingSystemInfo
        """
        try:
            query_params: dict[str, object] = {
                "id": params.app_id,
                "metrics": ",".join(params.metrics),
                "limit": params.limit,
                "offset": params.offset,
            }

            if params.dimensions:
                query_params["dimensions"] = ",".join(params.dimensions)
            if params.date1:
                query_params["date1"] = params.date1
            if params.date2:
                query_params["date2"] = params.date2
            if params.filters:
                query_params["filters"] = params.filters
            if params.sort:
                query_params["sort"] = params.sort

            result = await api_client.appmetrica_request(
                "/stat/v1/data",
                params=query_params,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_report_markdown(result)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="appmetrica_get_report_by_time",
        annotations={
            "title": "Get AppMetrica Time-Based Report",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_report_by_time(params: AppMetricaByTimeInput) -> str:
        """Get time-based statistics from AppMetrica.

        Retrieves statistics grouped by time periods (day, week, month, etc.).
        Useful for tracking trends and building charts.
        """
        try:
            query_params: dict[str, object] = {
                "id": params.app_id,
                "metrics": ",".join(params.metrics),
                "group": params.group.value,
            }

            if params.dimensions:
                query_params["dimensions"] = ",".join(params.dimensions)
            if params.date1:
                query_params["date1"] = params.date1
            if params.date2:
                query_params["date2"] = params.date2

            result = await api_client.appmetrica_request(
                "/stat/v1/data/bytime",
                params=query_params,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            lines = ["# AppMetrica Time-Based Report\n"]

            query = result.get("query", {})
            lines.append(
                f"**Period**: {query.get('date1', 'N/A')} - {query.get('date2', 'N/A')}"
            )
            lines.append(f"**Grouping**: {params.group.value}\n")

            time_intervals = result.get("time_intervals", [])
            data = result.get("data", [])

            if data:
                for row in data:
                    dims = row.get("dimensions", [])
                    metrics = row.get("metrics", [[]])

                    dim_str = " / ".join(
                        str(d.get("name", d.get("id", "N/A")))
                        if isinstance(d, dict) else str(d)
                        for d in dims
                    ) if dims else "Total"

                    lines.append(f"## {dim_str}")

                    if time_intervals and metrics:
                        for i, interval in enumerate(time_intervals):
                            interval_str = (
                                " - ".join(str(t) for t in interval)
                                if isinstance(interval, list) else str(interval)
                            )
                            values = [m[i] if i < len(m) else 0 for m in metrics]
                            values_str = ", ".join(f"{v:,.2f}" for v in values)
                            lines.append(f"- {interval_str}: {values_str}")

                    lines.append("")

            return "\n".join(lines)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="appmetrica_get_drilldown_report",
        annotations={
            "title": "Get AppMetrica Drilldown Report",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def appmetrica_get_drilldown_report(
        params: AppMetricaDrilldownInput,
    ) -> str:
        """Get drilldown (hierarchical) report from AppMetrica.

        Creates tree-view reports where you can drill down into branches.
        For example: Country > City > Device.
        """
        try:
            query_params: dict[str, object] = {
                "id": params.app_id,
                "metrics": ",".join(params.metrics),
                "dimensions": ",".join(params.dimensions),
                "limit": params.limit,
            }

            if params.date1:
                query_params["date1"] = params.date1
            if params.date2:
                query_params["date2"] = params.date2
            if params.filters:
                query_params["filters"] = params.filters
            if params.parent_id:
                query_params["parent_id"] = json.dumps(params.parent_id)

            result = await api_client.appmetrica_request(
                "/stat/v1/data/drilldown",
                params=query_params,
            )

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(result, indent=2, ensure_ascii=False)

            return format_appmetrica_drilldown_markdown(result)

        except Exception as e:
            return handle_api_error(e)
