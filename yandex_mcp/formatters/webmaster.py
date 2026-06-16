"""Markdown formatters for Yandex Webmaster API responses."""

from typing import Any, Dict


def format_webmaster_hosts_markdown(data: Dict[str, Any]) -> str:
    hosts = data.get("hosts", [])
    if not hosts:
        return "No verified hosts found."
    lines = ["# Webmaster Hosts\n", "| Host ID | URL | Verified | Main mirror |", "| --- | --- | :---: | --- |"]
    for h in hosts:
        lines.append(
            f"| `{h.get('host_id', 'N/A')}` "
            f"| {h.get('unicode_host_url') or h.get('ascii_host_url', 'N/A')} "
            f"| {'✓' if h.get('verified') else '✗'} "
            f"| {h.get('main_mirror', {}).get('host_id', '') if h.get('main_mirror') else ''} |"
        )
    return "\n".join(lines)


def format_webmaster_summary_markdown(data: Dict[str, Any]) -> str:
    lines = ["# Webmaster Host Summary\n"]
    lines.append(f"- **SQI (ИКС)**: {data.get('sqi', 'N/A')}")
    lines.append(f"- **Searchable pages**: {data.get('searchable_pages_count', 'N/A')}")
    lines.append(f"- **Excluded pages**: {data.get('excluded_pages_count', 'N/A')}")
    lines.append(f"- **Site problems**: {data.get('site_problems', {})}")
    return "\n".join(lines)


def format_webmaster_popular_queries_markdown(data: Dict[str, Any]) -> str:
    queries = data.get("queries", [])
    if not queries:
        return "No query data found."
    lines = [
        f"# Webmaster Popular Queries (total: {data.get('count', len(queries)):,})\n",
        "| Query | Shows | Clicks | Avg show pos | Avg click pos |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for q in queries:
        ind = q.get("indicators", {})

        def num(key: str, decimals: int = 0) -> str:
            val = ind.get(key)
            if val is None:
                return ""
            return f"{val:,.{decimals}f}" if decimals else f"{int(val):,}"

        lines.append(
            f"| {q.get('query_text', 'N/A')} "
            f"| {num('TOTAL_SHOWS')} "
            f"| {num('TOTAL_CLICKS')} "
            f"| {num('AVG_SHOW_POSITION', 1)} "
            f"| {num('AVG_CLICK_POSITION', 1)} |"
        )
    return "\n".join(lines)
