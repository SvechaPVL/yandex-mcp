"""Markdown formatters for Yandex AppMetrica API responses."""

from typing import Any, Dict, List


def format_appmetrica_applications_markdown(apps: List[Dict[str, Any]]) -> str:
    """Format AppMetrica applications list as markdown."""
    if not apps:
        return "No applications found."

    lines = ["# AppMetrica Applications\n"]
    for app in apps:
        app_id = app.get("id", "N/A")
        lines.append(f"## {app.get('name', 'Unnamed')} (ID: {app_id})")
        lines.append(f"- **Time Zone**: {app.get('time_zone_name', 'N/A')}")
        lines.append(f"- **Create Date**: {app.get('create_date', 'N/A')}")

        if app.get("permission"):
            lines.append(f"- **Permission**: {app['permission']}")

        lines.append("")

    return "\n".join(lines)


def format_appmetrica_application_markdown(app: Dict[str, Any]) -> str:
    """Format a single AppMetrica application as markdown."""
    lines = [f"# {app.get('name', 'Unnamed')} (ID: {app.get('id', 'N/A')})\n"]
    lines.append(f"- **Time Zone**: {app.get('time_zone_name', 'N/A')}")
    lines.append(f"- **Create Date**: {app.get('create_date', 'N/A')}")
    lines.append(f"- **Permission**: {app.get('permission', 'N/A')}")

    if app.get("label"):
        lines.append(f"- **Label**: {app['label']}")

    return "\n".join(lines)


def format_appmetrica_report_markdown(data: Dict[str, Any]) -> str:
    """Format AppMetrica report data as markdown."""
    lines = ["# AppMetrica Report\n"]

    query = data.get("query", {})
    lines.append("## Query Parameters")
    lines.append(f"- **Period**: {query.get('date1', 'N/A')} - {query.get('date2', 'N/A')}")

    if query.get("dimensions"):
        dims = query["dimensions"]
        lines.append(f"- **Dimensions**: {', '.join(dims) if isinstance(dims, list) else dims}")
    if query.get("metrics"):
        mets = query["metrics"]
        lines.append(f"- **Metrics**: {', '.join(mets) if isinstance(mets, list) else mets}")

    lines.append("")

    totals = data.get("totals", [])
    if totals:
        lines.append("## Totals")
        metrics = query.get("metrics", [])
        if isinstance(metrics, str):
            metrics = metrics.split(",")
        for i, total in enumerate(totals):
            metric_name = metrics[i] if i < len(metrics) else f"Metric {i + 1}"
            lines.append(f"- **{metric_name}**: {total:,.2f}")
        lines.append("")

    rows = data.get("data", [])
    if rows:
        lines.append(f"## Data ({len(rows)} rows)")
        for row in rows[:50]:
            dims = row.get("dimensions", [])
            metrics_vals = row.get("metrics", [])

            dim_str = " / ".join(
                str(d.get("name", d.get("id", "N/A"))) if isinstance(d, dict) else str(d)
                for d in dims
            ) if dims else "Total"

            metrics_str = ", ".join(f"{v:,.2f}" for v in metrics_vals)
            lines.append(f"- **{dim_str}**: {metrics_str}")

        if len(rows) > 50:
            lines.append(f"\n*...and {len(rows) - 50} more rows*")

    return "\n".join(lines)


def format_appmetrica_drilldown_markdown(data: Dict[str, Any]) -> str:
    """Format AppMetrica drilldown report as markdown."""
    lines = ["# AppMetrica Drilldown Report\n"]

    query = data.get("query", {})
    lines.append(f"- **Period**: {query.get('date1', 'N/A')} - {query.get('date2', 'N/A')}")
    lines.append("")

    rows = data.get("data", [])
    if rows:
        for row in rows[:50]:
            dims = row.get("dimensions", [])
            metrics_vals = row.get("metrics", [])

            dim_str = " / ".join(
                str(d.get("name", d.get("id", "N/A"))) if isinstance(d, dict) else str(d)
                for d in dims
            ) if dims else "Total"

            metrics_str = ", ".join(f"{v:,.2f}" for v in metrics_vals)
            expand = " (expandable)" if row.get("expand", False) else ""
            lines.append(f"- **{dim_str}**: {metrics_str}{expand}")

        if len(rows) > 50:
            lines.append(f"\n*...and {len(rows) - 50} more rows*")
    else:
        lines.append("No data found.")

    return "\n".join(lines)


def format_appmetrica_logs_markdown(data: Dict[str, Any], export_type: str) -> str:
    """Format AppMetrica Logs API response as markdown."""
    lines = [f"# AppMetrica Logs Export: {export_type}\n"]

    rows = data.get("data", [])
    if not rows:
        lines.append("No data found for the specified period.")
        return "\n".join(lines)

    lines.append(f"**Total rows**: {len(rows)}\n")

    if rows:
        headers = list(rows[0].keys())
        preview = rows[:20]

        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join("---" for _ in headers) + " |")
        for row in preview:
            vals = [str(row.get(h, ""))[:40] for h in headers]
            lines.append("| " + " | ".join(vals) + " |")

        if len(rows) > 20:
            lines.append(f"\n*...and {len(rows) - 20} more rows*")

    return "\n".join(lines)


def format_appmetrica_events_markdown(data: Dict[str, Any]) -> str:
    """Format AppMetrica events report as markdown."""
    lines = ["# AppMetrica Events\n"]

    query = data.get("query", {})
    lines.append(f"**Period**: {query.get('date1', 'N/A')} - {query.get('date2', 'N/A')}\n")

    rows = data.get("data", [])
    if not rows:
        lines.append("No events found.")
        return "\n".join(lines)

    lines.append("| Event | Users | Event Count |")
    lines.append("| --- | ---: | ---: |")
    for row in rows[:100]:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [0, 0])
        event_name = dims[0].get("name", "N/A") if dims and isinstance(dims[0], dict) else "N/A"
        users = metrics[0] if len(metrics) > 0 else 0
        count = metrics[1] if len(metrics) > 1 else 0
        lines.append(f"| {event_name} | {users:,.0f} | {count:,.0f} |")

    if len(rows) > 100:
        lines.append(f"\n*...and {len(rows) - 100} more events*")

    return "\n".join(lines)


def format_appmetrica_crashes_markdown(data: Dict[str, Any]) -> str:
    """Format AppMetrica crashes report as markdown."""
    lines = ["# AppMetrica Crashes\n"]

    query = data.get("query", {})
    lines.append(f"**Period**: {query.get('date1', 'N/A')} - {query.get('date2', 'N/A')}\n")

    totals = data.get("totals", [])
    if totals:
        lines.append(f"**Total crashes**: {totals[0]:,.0f}\n")

    rows = data.get("data", [])
    if not rows:
        lines.append("No crashes found.")
        return "\n".join(lines)

    for row in rows[:50]:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [0])

        dim_str = " / ".join(
            str(d.get("name", d.get("id", "N/A"))) if isinstance(d, dict) else str(d)
            for d in dims
        ) if dims else "Total"

        crash_count = metrics[0] if metrics else 0
        lines.append(f"- **{dim_str}**: {crash_count:,.0f} crashes")

    if len(rows) > 50:
        lines.append(f"\n*...and {len(rows) - 50} more rows*")

    return "\n".join(lines)


def format_appmetrica_funnel_markdown(
    steps: List[str],
    step_users: List[int],
    total_users: int,
) -> str:
    """Format funnel analysis as markdown."""
    lines = ["# AppMetrica Funnel\n"]

    if not step_users:
        lines.append("No data found for the specified events and period.")
        return "\n".join(lines)

    lines.append(f"**Total unique users in period**: {total_users:,}\n")
    lines.append("| Step | Event | Users | % of Total | Conversion |")
    lines.append("| ---: | --- | ---: | ---: | ---: |")

    for i, (step_name, users) in enumerate(zip(steps, step_users)):
        pct_total = (users / total_users * 100) if total_users > 0 else 0
        if i == 0:
            conversion = "—"
        else:
            prev = step_users[i - 1]
            conversion = f"{users / prev * 100:.1f}%" if prev > 0 else "0%"

        lines.append(
            f"| {i + 1} | {step_name} | {users:,} | {pct_total:.1f}% | {conversion} |"
        )

    if len(step_users) >= 2 and step_users[0] > 0:
        overall = step_users[-1] / step_users[0] * 100
        lines.append(f"\n**Overall conversion**: {overall:.1f}%")

    return "\n".join(lines)


def format_appmetrica_push_group_markdown(group: Dict[str, Any]) -> str:
    """Format push group as markdown."""
    lines = ["# Push Group Created\n"]
    lines.append(f"- **Group ID**: {group.get('id', 'N/A')}")
    lines.append(f"- **App ID**: {group.get('app_id', 'N/A')}")
    lines.append(f"- **Name**: {group.get('name', 'N/A')}")
    return "\n".join(lines)


def format_appmetrica_push_status_markdown(transfer: Dict[str, Any]) -> str:
    """Format push transfer status as markdown."""
    lines = ["# Push Transfer Status\n"]
    lines.append(f"- **Transfer ID**: {transfer.get('id', 'N/A')}")
    lines.append(f"- **Status**: {transfer.get('status', 'N/A')}")
    lines.append(f"- **Tag**: {transfer.get('tag', 'N/A')}")
    lines.append(f"- **Group ID**: {transfer.get('group_id', 'N/A')}")
    lines.append(f"- **Created**: {transfer.get('creation_date', 'N/A')}")

    errors = transfer.get("errors", [])
    if errors:
        lines.append("\n## Errors")
        for err in errors:
            lines.append(f"- {err}")

    return "\n".join(lines)
