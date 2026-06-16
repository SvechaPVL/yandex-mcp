"""Tests for Yandex MCP package structure and tool registration."""

import pytest


EXPECTED_DIRECT_TOOLS = [
    # Campaigns (8)
    "direct_get_campaigns",
    "direct_create_campaign",
    "direct_update_campaign",
    "direct_suspend_campaigns",
    "direct_resume_campaigns",
    "direct_archive_campaigns",
    "direct_unarchive_campaigns",
    "direct_delete_campaigns",
    # Ad Groups (3)
    "direct_get_adgroups",
    "direct_create_adgroup",
    "direct_update_adgroup",
    # Ads (12)
    "direct_get_ads",
    "direct_create_text_ad",
    "direct_create_image_ad",
    "direct_create_dynamic_ad",
    "direct_create_shopping_ad",
    "direct_update_ad",
    "direct_moderate_ads",
    "direct_suspend_ads",
    "direct_resume_ads",
    "direct_archive_ads",
    "direct_unarchive_ads",
    "direct_delete_ads",
    # Keywords (6)
    "direct_get_keywords",
    "direct_add_keywords",
    "direct_set_keyword_bids",
    "direct_suspend_keywords",
    "direct_resume_keywords",
    "direct_delete_keywords",
    # Statistics (1)
    "direct_get_statistics",
    # Bid Modifiers (5)
    "direct_get_bid_modifiers",
    "direct_add_bid_modifier",
    "direct_set_bid_modifier",
    "direct_delete_bid_modifiers",
    "direct_toggle_bid_modifiers",
    # Retargeting (4)
    "direct_get_retargeting_lists",
    "direct_add_retargeting_list",
    "direct_update_retargeting_list",
    "direct_delete_retargeting_lists",
    # Audience Targets (5)
    "direct_get_audience_targets",
    "direct_add_audience_target",
    "direct_suspend_audience_targets",
    "direct_resume_audience_targets",
    "direct_delete_audience_targets",
    # Smart Ad Targets (5)
    "direct_get_smart_ad_targets",
    "direct_add_smart_ad_target",
    "direct_suspend_smart_ad_targets",
    "direct_resume_smart_ad_targets",
    "direct_delete_smart_ad_targets",
    # Sitelinks (3)
    "direct_get_sitelinks",
    "direct_add_sitelinks",
    "direct_delete_sitelinks",
    # VCards (3)
    "direct_get_vcards",
    "direct_add_vcard",
    "direct_delete_vcards",
    # Negative Keywords (4)
    "direct_get_negative_keyword_shared_sets",
    "direct_add_negative_keyword_shared_set",
    "direct_update_negative_keyword_shared_set",
    "direct_delete_negative_keyword_shared_sets",
    # Ad Extensions (3)
    "direct_get_adextensions",
    "direct_add_callouts",
    "direct_link_callouts_to_ad",
    # Videos & Creatives (4)
    "direct_upload_video",
    "direct_get_advideos",
    "direct_create_video_creative",
    "direct_get_creatives",
    # Feeds (4)
    "direct_get_feeds",
    "direct_add_feed",
    "direct_update_feed",
    "direct_delete_feeds",
    # Images (3)
    "direct_upload_image",
    "direct_get_images",
    "direct_delete_images",
    # Dictionaries & Regions (3)
    "direct_get_dictionaries",
    "direct_get_regions",
    "direct_get_interests",
    # Client & Changes (4)
    "direct_get_client_info",
    "direct_check_campaign_changes",
    "direct_check_all_changes",
    "direct_get_recent_changes_timestamp",
]

EXPECTED_METRIKA_TOOLS = [
    # Counters (5)
    "metrika_get_counters",
    "metrika_get_counter",
    "metrika_create_counter",
    "metrika_update_counter",
    "metrika_delete_counter",
    # Goals (4)
    "metrika_get_goals",
    "metrika_create_goal",
    "metrika_update_goal",
    "metrika_delete_goal",
    # Reports (4)
    "metrika_get_report",
    "metrika_get_report_by_time",
    "metrika_get_comparison_report",
    "metrika_get_drilldown_report",
    # Segments (4)
    "metrika_get_segments",
    "metrika_create_segment",
    "metrika_update_segment",
    "metrika_delete_segment",
    # Filters (4)
    "metrika_get_filters",
    "metrika_create_filter",
    "metrika_update_filter",
    "metrika_delete_filter",
    # Grants (4)
    "metrika_get_grants",
    "metrika_add_grant",
    "metrika_update_grant",
    "metrika_delete_grant",
    # Offline Data (5)
    "metrika_upload_offline_conversions",
    "metrika_get_offline_conversions_status",
    "metrika_upload_calls",
    "metrika_upload_expenses",
    "metrika_upload_user_parameters",
    # Labels (6)
    "metrika_get_labels",
    "metrika_create_label",
    "metrika_update_label",
    "metrika_delete_label",
    "metrika_link_counter_to_label",
    "metrika_unlink_counter_from_label",
    # Annotations (4)
    "metrika_get_annotations",
    "metrika_create_annotation",
    "metrika_update_annotation",
    "metrika_delete_annotation",
    # Delegates (3)
    "metrika_get_delegates",
    "metrika_add_delegate",
    "metrika_delete_delegate",
]

EXPECTED_WORDSTAT_TOOLS = [
    "wordstat_top_requests",
    "wordstat_dynamics",
    "wordstat_regions",
    "wordstat_regions_tree",
    "wordstat_user_info",
]

EXPECTED_WEBMASTER_TOOLS = [
    "webmaster_get_user_id",
    "webmaster_get_hosts",
    "webmaster_get_host_summary",
    "webmaster_get_popular_queries",
]


class TestToolRegistration:
    """Test that all tools are properly registered."""

    def test_total_tool_count(self, mcp_instance):
        tools = list(mcp_instance._tool_manager._tools.keys())
        all_expected = (
            EXPECTED_DIRECT_TOOLS
            + EXPECTED_METRIKA_TOOLS
            + EXPECTED_WORDSTAT_TOOLS
            + EXPECTED_WEBMASTER_TOOLS
        )
        expected = len(all_expected)
        assert len(tools) == expected, (
            f"Expected {expected} tools, got {len(tools)}. "
            f"Missing: {set(all_expected) - set(tools)}. "
            f"Extra: {set(tools) - set(all_expected)}"
        )

    def test_direct_tool_count(self, mcp_instance):
        tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("direct_")]
        assert len(tools) == len(EXPECTED_DIRECT_TOOLS)

    def test_metrika_tool_count(self, mcp_instance):
        tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("metrika_")]
        assert len(tools) == len(EXPECTED_METRIKA_TOOLS)

    def test_wordstat_tool_count(self, mcp_instance):
        tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("wordstat_")]
        assert len(tools) == len(EXPECTED_WORDSTAT_TOOLS)

    def test_webmaster_tool_count(self, mcp_instance):
        tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("webmaster_")]
        assert len(tools) == len(EXPECTED_WEBMASTER_TOOLS)

    @pytest.mark.parametrize("tool_name", EXPECTED_DIRECT_TOOLS)
    def test_direct_tool_exists(self, mcp_instance, tool_name):
        assert tool_name in mcp_instance._tool_manager._tools, f"Missing Direct tool: {tool_name}"

    @pytest.mark.parametrize("tool_name", EXPECTED_METRIKA_TOOLS)
    def test_metrika_tool_exists(self, mcp_instance, tool_name):
        assert tool_name in mcp_instance._tool_manager._tools, f"Missing Metrika tool: {tool_name}"

    @pytest.mark.parametrize("tool_name", EXPECTED_WORDSTAT_TOOLS)
    def test_wordstat_tool_exists(self, mcp_instance, tool_name):
        assert tool_name in mcp_instance._tool_manager._tools, f"Missing Wordstat tool: {tool_name}"

    @pytest.mark.parametrize("tool_name", EXPECTED_WEBMASTER_TOOLS)
    def test_webmaster_tool_exists(self, mcp_instance, tool_name):
        assert tool_name in mcp_instance._tool_manager._tools, f"Missing Webmaster tool: {tool_name}"


class TestToolNaming:
    """Test naming conventions."""

    def test_all_direct_tools_prefixed(self, mcp_instance):
        direct_tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("direct_")]
        for tool in direct_tools:
            assert tool.startswith("direct_"), f"Direct tool not properly prefixed: {tool}"

    def test_all_metrika_tools_prefixed(self, mcp_instance):
        metrika_tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("metrika_")]
        for tool in metrika_tools:
            assert tool.startswith("metrika_"), f"Metrika tool not properly prefixed: {tool}"

    def test_all_wordstat_tools_prefixed(self, mcp_instance):
        wordstat_tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("wordstat_")]
        for tool in wordstat_tools:
            assert tool.startswith("wordstat_"), f"Wordstat tool not properly prefixed: {tool}"

    def test_all_webmaster_tools_prefixed(self, mcp_instance):
        webmaster_tools = [t for t in mcp_instance._tool_manager._tools if t.startswith("webmaster_")]
        for tool in webmaster_tools:
            assert tool.startswith("webmaster_"), f"Webmaster tool not properly prefixed: {tool}"

    def test_no_unknown_prefixes(self, mcp_instance):
        known_prefixes = ("direct_", "metrika_", "wordstat_", "webmaster_")
        for tool in mcp_instance._tool_manager._tools:
            assert any(tool.startswith(p) for p in known_prefixes), f"Unknown prefix in tool: {tool}"


class TestImports:
    """Test that all modules import cleanly."""

    def test_import_package(self):
        import yandex_mcp
        assert hasattr(yandex_mcp, "mcp")

    def test_import_client(self):
        from yandex_mcp.client import api_client
        assert api_client is not None

    def test_import_config(self):
        from yandex_mcp.config import YANDEX_DIRECT_API_URL, YANDEX_WORDSTAT_API_URL
        assert "direct" in YANDEX_DIRECT_API_URL
        assert "wordstat" in YANDEX_WORDSTAT_API_URL

    def test_import_models(self):
        from yandex_mcp.models.common import ResponseFormat
        assert ResponseFormat.JSON.value == "json"
        assert ResponseFormat.MARKDOWN.value == "markdown"

    def test_import_wordstat_models(self):
        from yandex_mcp.models.wordstat import (
            WordstatTopRequestsInput,
            WordstatDynamicsInput,
            WordstatRegionsInput,
        )
        assert WordstatTopRequestsInput is not None

    def test_import_formatters(self):
        from yandex_mcp.formatters.wordstat import format_wordstat_top_requests_markdown
        assert callable(format_wordstat_top_requests_markdown)

    def test_import_webmaster_models(self):
        from yandex_mcp.models.webmaster import (
            WebmasterHostsInput,
            WebmasterHostSummaryInput,
            WebmasterPopularQueriesInput,
        )
        assert WebmasterPopularQueriesInput is not None

    def test_import_webmaster_formatters(self):
        from yandex_mcp.formatters.webmaster import format_webmaster_popular_queries_markdown
        assert callable(format_webmaster_popular_queries_markdown)
