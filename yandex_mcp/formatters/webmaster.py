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


def format_webmaster_verification_markdown(data: Dict[str, Any]) -> str:
    lines = ["# Webmaster Host Verification\n"]
    lines.append(f"- **State**: {data.get('verification_state', 'N/A')}")
    lines.append(f"- **Type**: {data.get('verification_type', 'N/A')}")
    lines.append(f"- **UIN (confirmation code)**: `{data.get('verification_uin', 'N/A')}`")
    if data.get("latest_verification_time"):
        lines.append(f"- **Last checked**: {data['latest_verification_time']}")
    fail_info = data.get("fail_info")
    if fail_info:
        lines.append(f"- **Failure reason**: {fail_info.get('reason', 'N/A')} - {fail_info.get('message', '')}")
    applicable = data.get("applicable_verifiers", [])
    if applicable:
        lines.append(f"- **Applicable methods**: {', '.join(applicable)}")
    return "\n".join(lines)


def format_webmaster_sitemaps_markdown(data: Dict[str, Any]) -> str:
    sitemaps = data.get("sitemaps", [])
    if not sitemaps:
        return "No user-added sitemaps found."
    lines = [
        f"# Webmaster User-Added Sitemaps (total: {data.get('count', len(sitemaps)):,})\n",
        "| Sitemap ID | URL | Added |",
        "| --- | --- | --- |",
    ]
    for s in sitemaps:
        lines.append(
            f"| `{s.get('sitemap_id', 'N/A')}` "
            f"| {s.get('sitemap_url', 'N/A')} "
            f"| {s.get('added_date', 'N/A')} |"
        )
    return "\n".join(lines)


def format_webmaster_recrawl_quota_markdown(data: Dict[str, Any]) -> str:
    lines = ["# Webmaster Recrawl Quota\n"]
    lines.append(f"- **Daily quota**: {data.get('daily_quota', 'N/A')}")
    lines.append(f"- **Remaining today**: {data.get('quota_remainder', 'N/A')}")
    return "\n".join(lines)
