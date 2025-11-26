#!/usr/bin/env python3
"""
Yandex MCP Server - MCP server for Yandex Direct and Yandex Metrika APIs.

This server provides tools for managing advertising campaigns in Yandex Direct
and analyzing website statistics in Yandex Metrika through the Model Context Protocol.
"""

import json
import os
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field, field_validator

# =============================================================================
# Configuration
# =============================================================================

# API Endpoints
YANDEX_DIRECT_API_URL = "https://api.direct.yandex.com/json/v5"
YANDEX_DIRECT_API_URL_V501 = "https://api.direct.yandex.com/json/v501"
YANDEX_DIRECT_SANDBOX_URL = "https://api-sandbox.direct.yandex.com/json/v5"
YANDEX_METRIKA_API_URL = "https://api-metrika.yandex.net"

# Default timeout for API requests
DEFAULT_TIMEOUT = 30.0

# Initialize MCP Server
mcp = FastMCP("yandex_mcp")


# =============================================================================
# Enums
# =============================================================================

class ResponseFormat(str, Enum):
    """Output format for tool responses."""
    MARKDOWN = "markdown"
    JSON = "json"


class CampaignState(str, Enum):
    """Campaign state filter."""
    ON = "ON"
    OFF = "OFF"
    SUSPENDED = "SUSPENDED"
    ENDED = "ENDED"
    CONVERTED = "CONVERTED"
    ARCHIVED = "ARCHIVED"


class CampaignStatus(str, Enum):
    """Campaign status filter."""
    ACCEPTED = "ACCEPTED"
    DRAFT = "DRAFT"
    MODERATION = "MODERATION"
    REJECTED = "REJECTED"


class CampaignType(str, Enum):
    """Campaign type filter."""
    TEXT_CAMPAIGN = "TEXT_CAMPAIGN"
    DYNAMIC_TEXT_CAMPAIGN = "DYNAMIC_TEXT_CAMPAIGN"
    MOBILE_APP_CAMPAIGN = "MOBILE_APP_CAMPAIGN"
    CPM_BANNER_CAMPAIGN = "CPM_BANNER_CAMPAIGN"
    SMART_CAMPAIGN = "SMART_CAMPAIGN"
    UNIFIED_CAMPAIGN = "UNIFIED_CAMPAIGN"


class AdState(str, Enum):
    """Ad state filter."""
    ON = "ON"
    OFF = "OFF"
    OFF_BY_MONITORING = "OFF_BY_MONITORING"
    SUSPENDED = "SUSPENDED"
    ARCHIVED = "ARCHIVED"


class AdStatus(str, Enum):
    """Ad status filter."""
    ACCEPTED = "ACCEPTED"
    DRAFT = "DRAFT"
    MODERATION = "MODERATION"
    PREACCEPTED = "PREACCEPTED"
    REJECTED = "REJECTED"


class DailyBudgetMode(str, Enum):
    """Daily budget spending mode."""
    STANDARD = "STANDARD"
    DISTRIBUTED = "DISTRIBUTED"


class MetrikaGroupType(str, Enum):
    """Time grouping for Metrika reports."""
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
    HOUR = "hour"
    MINUTE = "minute"


# =============================================================================
# API Client
# =============================================================================

class YandexAPIClient:
    """Unified client for Yandex Direct and Metrika APIs."""
    
    def __init__(self):
        self.direct_token = os.environ.get("YANDEX_DIRECT_TOKEN", "")
        self.metrika_token = os.environ.get("YANDEX_METRIKA_TOKEN", "")
        # Allow single token for both services
        self.unified_token = os.environ.get("YANDEX_TOKEN", "")
        self.client_login = os.environ.get("YANDEX_CLIENT_LOGIN", "")
        self.use_sandbox = os.environ.get("YANDEX_USE_SANDBOX", "false").lower() == "true"
    
    def _get_direct_token(self) -> str:
        """Get token for Direct API."""
        return self.direct_token or self.unified_token
    
    def _get_metrika_token(self) -> str:
        """Get token for Metrika API."""
        return self.metrika_token or self.unified_token
    
    def _get_direct_url(self, use_v501: bool = False) -> str:
        """Get Direct API URL based on configuration."""
        if self.use_sandbox:
            return YANDEX_DIRECT_SANDBOX_URL
        return YANDEX_DIRECT_API_URL_V501 if use_v501 else YANDEX_DIRECT_API_URL
    
    async def direct_request(
        self,
        service: str,
        method: str,
        params: Dict[str, Any],
        use_v501: bool = False
    ) -> Dict[str, Any]:
        """Make a request to Yandex Direct API."""
        token = self._get_direct_token()
        if not token:
            raise ValueError(
                "Yandex Direct API token not configured. "
                "Set YANDEX_DIRECT_TOKEN or YANDEX_TOKEN environment variable."
            )
        
        url = f"{self._get_direct_url(use_v501)}/{service}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json"
        }
        
        if self.client_login:
            headers["Client-Login"] = self.client_login
        
        payload = {
            "method": method,
            "params": params
        }
        
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
    
    async def metrika_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make a request to Yandex Metrika API."""
        token = self._get_metrika_token()
        if not token:
            raise ValueError(
                "Yandex Metrika API token not configured. "
                "Set YANDEX_METRIKA_TOKEN or YANDEX_TOKEN environment variable."
            )
        
        url = f"{YANDEX_METRIKA_API_URL}{endpoint}"
        headers = {
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            if method == "GET":
                response = await client.get(url, params=params, headers=headers)
            elif method == "POST":
                response = await client.post(url, json=data, params=params, headers=headers)
            elif method == "PUT":
                response = await client.put(url, json=data, params=params, headers=headers)
            elif method == "DELETE":
                response = await client.delete(url, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            if response.status_code == 204:
                return {"success": True}
            
            return response.json()


# Global API client instance
api_client = YandexAPIClient()


# =============================================================================
# Helper Functions
# =============================================================================

def _handle_api_error(e: Exception) -> str:
    """Format API errors into actionable messages."""
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        try:
            error_body = e.response.json()
            error_msg = error_body.get("error", {}).get("error_string", "")
            error_detail = error_body.get("error", {}).get("error_detail", "")
            if error_msg:
                return f"API Error ({status}): {error_msg}. {error_detail}".strip()
        except Exception:
            pass
        
        error_messages = {
            400: "Bad request. Check your parameters.",
            401: "Authentication failed. Check your API token.",
            403: "Access denied. Check permissions for this operation.",
            404: "Resource not found. Check the ID.",
            429: "Rate limit exceeded. Wait before making more requests.",
            500: "Server error. Try again later.",
            503: "Service unavailable. Try again later."
        }
        return f"API Error: {error_messages.get(status, f'Request failed with status {status}')}"
    
    if isinstance(e, httpx.TimeoutException):
        return "Request timed out. The operation may still complete on the server."
    
    if isinstance(e, ValueError):
        return f"Configuration Error: {str(e)}"
    
    return f"Unexpected error: {type(e).__name__}: {str(e)}"


def _format_campaigns_markdown(campaigns: List[Dict]) -> str:
    """Format campaigns list as markdown."""
    if not campaigns:
        return "No campaigns found."
    
    lines = ["# Campaigns\n"]
    for camp in campaigns:
        lines.append(f"## {camp.get('Name', 'Unnamed')} (ID: {camp.get('Id')})")
        lines.append(f"- **Type**: {camp.get('Type', 'N/A')}")
        lines.append(f"- **State**: {camp.get('State', 'N/A')}")
        lines.append(f"- **Status**: {camp.get('Status', 'N/A')}")
        
        if camp.get("DailyBudget"):
            budget = camp["DailyBudget"]
            amount = budget.get("Amount", 0) / 1_000_000
            lines.append(f"- **Daily Budget**: {amount:.2f} ({budget.get('Mode', 'N/A')})")
        
        if camp.get("Statistics"):
            stats = camp["Statistics"]
            lines.append(f"- **Clicks**: {stats.get('Clicks', 0)}")
            lines.append(f"- **Impressions**: {stats.get('Impressions', 0)}")
        
        lines.append("")
    
    return "\n".join(lines)


def _format_ads_markdown(ads: List[Dict]) -> str:
    """Format ads list as markdown."""
    if not ads:
        return "No ads found."
    
    lines = ["# Ads\n"]
    for ad in ads:
        ad_id = ad.get("Id")
        lines.append(f"## Ad ID: {ad_id}")
        lines.append(f"- **AdGroup ID**: {ad.get('AdGroupId')}")
        lines.append(f"- **Campaign ID**: {ad.get('CampaignId')}")
        lines.append(f"- **State**: {ad.get('State', 'N/A')}")
        lines.append(f"- **Status**: {ad.get('Status', 'N/A')}")
        
        if ad.get("TextAd"):
            text_ad = ad["TextAd"]
            lines.append(f"- **Title**: {text_ad.get('Title', 'N/A')}")
            lines.append(f"- **Title2**: {text_ad.get('Title2', 'N/A')}")
            lines.append(f"- **Text**: {text_ad.get('Text', 'N/A')}")
            lines.append(f"- **Href**: {text_ad.get('Href', 'N/A')}")
        
        lines.append("")
    
    return "\n".join(lines)


def _format_adgroups_markdown(groups: List[Dict]) -> str:
    """Format ad groups list as markdown."""
    if not groups:
        return "No ad groups found."
    
    lines = ["# Ad Groups\n"]
    for group in groups:
        lines.append(f"## {group.get('Name', 'Unnamed')} (ID: {group.get('Id')})")
        lines.append(f"- **Campaign ID**: {group.get('CampaignId')}")
        lines.append(f"- **Type**: {group.get('Type', 'N/A')}")
        lines.append(f"- **Status**: {group.get('Status', 'N/A')}")
        
        region_ids = group.get("RegionIds", [])
        if region_ids:
            lines.append(f"- **Regions**: {', '.join(map(str, region_ids))}")
        
        lines.append("")
    
    return "\n".join(lines)


def _format_keywords_markdown(keywords: List[Dict]) -> str:
    """Format keywords list as markdown."""
    if not keywords:
        return "No keywords found."
    
    lines = ["# Keywords\n"]
    for kw in keywords:
        lines.append(f"## {kw.get('Keyword', 'N/A')} (ID: {kw.get('Id')})")
        lines.append(f"- **AdGroup ID**: {kw.get('AdGroupId')}")
        lines.append(f"- **State**: {kw.get('State', 'N/A')}")
        lines.append(f"- **Status**: {kw.get('Status', 'N/A')}")
        
        bid = kw.get("Bid", 0)
        if bid:
            lines.append(f"- **Bid**: {bid / 1_000_000:.2f}")
        
        lines.append("")
    
    return "\n".join(lines)


def _format_metrika_counters_markdown(counters: List[Dict]) -> str:
    """Format Metrika counters as markdown."""
    if not counters:
        return "No counters found."
    
    lines = ["# Metrika Counters\n"]
    for counter in counters:
        lines.append(f"## {counter.get('name', 'Unnamed')} (ID: {counter.get('id')})")
        
        site = counter.get("site2", {}).get("site", "N/A")
        lines.append(f"- **Site**: {site}")
        lines.append(f"- **Status**: {counter.get('status', 'N/A')}")
        lines.append(f"- **Code Status**: {counter.get('code_status', 'N/A')}")
        lines.append(f"- **Owner**: {counter.get('owner_login', 'N/A')}")
        
        if counter.get("favorite"):
            lines.append("- **Favorite**: ⭐")
        
        lines.append("")
    
    return "\n".join(lines)


def _format_metrika_report_markdown(data: Dict) -> str:
    """Format Metrika report data as markdown."""
    lines = ["# Metrika Report\n"]
    
    query = data.get("query", {})
    lines.append("## Query Parameters")
    lines.append(f"- **Period**: {query.get('date1', 'N/A')} — {query.get('date2', 'N/A')}")
    
    if query.get("dimensions"):
        lines.append(f"- **Dimensions**: {', '.join(query['dimensions'])}")
    if query.get("metrics"):
        lines.append(f"- **Metrics**: {', '.join(query['metrics'])}")
    
    lines.append("")
    
    # Totals
    totals = data.get("totals", [])
    if totals:
        lines.append("## Totals")
        metrics = query.get("metrics", [])
        for i, total in enumerate(totals):
            metric_name = metrics[i] if i < len(metrics) else f"Metric {i+1}"
            lines.append(f"- **{metric_name}**: {total:,.2f}")
        lines.append("")
    
    # Data rows
    rows = data.get("data", [])
    if rows:
        lines.append(f"## Data ({len(rows)} rows)")
        for row in rows[:50]:  # Limit to 50 rows in markdown
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


# =============================================================================
# Pydantic Input Models
# =============================================================================

# --- Yandex Direct Models ---

class GetCampaignsInput(BaseModel):
    """Input for getting campaigns list."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by specific campaign IDs"
    )
    states: Optional[List[CampaignState]] = Field(
        default=None,
        description="Filter by campaign states: ON, OFF, SUSPENDED, ENDED, CONVERTED, ARCHIVED"
    )
    statuses: Optional[List[CampaignStatus]] = Field(
        default=None,
        description="Filter by campaign statuses: ACCEPTED, DRAFT, MODERATION, REJECTED"
    )
    types: Optional[List[CampaignType]] = Field(
        default=None,
        description="Filter by campaign types"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum number of campaigns to return"
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Offset for pagination"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class ManageCampaignInput(BaseModel):
    """Input for managing campaign state (suspend/resume/archive/unarchive/delete)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Campaign IDs to manage (max 10 per request)"
    )


class UpdateCampaignInput(BaseModel):
    """Input for updating campaign settings."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_id: int = Field(
        ...,
        description="Campaign ID to update"
    )
    name: Optional[str] = Field(
        default=None,
        max_length=255,
        description="New campaign name"
    )
    daily_budget_amount: Optional[float] = Field(
        default=None,
        gt=0,
        description="Daily budget in currency units (will be converted to micros)"
    )
    daily_budget_mode: Optional[DailyBudgetMode] = Field(
        default=None,
        description="Daily budget mode: STANDARD or DISTRIBUTED"
    )
    start_date: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Campaign start date (YYYY-MM-DD)"
    )
    end_date: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Campaign end date (YYYY-MM-DD)"
    )
    negative_keywords: Optional[List[str]] = Field(
        default=None,
        description="Campaign-level negative keywords"
    )


class GetAdGroupsInput(BaseModel):
    """Input for getting ad groups."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by campaign IDs"
    )
    adgroup_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by specific ad group IDs"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum number of ad groups to return"
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Offset for pagination"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class CreateAdGroupInput(BaseModel):
    """Input for creating an ad group."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_id: int = Field(
        ...,
        description="Campaign ID to create ad group in"
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Ad group name"
    )
    region_ids: List[int] = Field(
        ...,
        min_length=1,
        description="List of region IDs for targeting (e.g., 225 for Russia, 213 for Moscow)"
    )
    negative_keywords: Optional[List[str]] = Field(
        default=None,
        description="Group-level negative keywords"
    )


class GetAdsInput(BaseModel):
    """Input for getting ads."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by campaign IDs"
    )
    adgroup_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by ad group IDs"
    )
    ad_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by specific ad IDs"
    )
    states: Optional[List[AdState]] = Field(
        default=None,
        description="Filter by ad states"
    )
    statuses: Optional[List[AdStatus]] = Field(
        default=None,
        description="Filter by ad statuses"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum number of ads to return"
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Offset for pagination"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class CreateTextAdInput(BaseModel):
    """Input for creating a text ad."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    adgroup_id: int = Field(
        ...,
        description="Ad group ID to create ad in"
    )
    title: str = Field(
        ...,
        min_length=1,
        max_length=56,
        description="Ad title (max 56 characters)"
    )
    title2: Optional[str] = Field(
        default=None,
        max_length=30,
        description="Second title (max 30 characters)"
    )
    text: str = Field(
        ...,
        min_length=1,
        max_length=81,
        description="Ad text (max 81 characters)"
    )
    href: str = Field(
        ...,
        description="Landing page URL"
    )
    mobile: bool = Field(
        default=False,
        description="Whether this is a mobile ad"
    )


class ManageAdInput(BaseModel):
    """Input for managing ad state (suspend/resume/archive/unarchive/delete/moderate)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    ad_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Ad IDs to manage"
    )


class GetKeywordsInput(BaseModel):
    """Input for getting keywords."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by campaign IDs"
    )
    adgroup_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by ad group IDs"
    )
    keyword_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by specific keyword IDs"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum number of keywords to return"
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Offset for pagination"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class AddKeywordsInput(BaseModel):
    """Input for adding keywords."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    adgroup_id: int = Field(
        ...,
        description="Ad group ID to add keywords to"
    )
    keywords: List[str] = Field(
        ...,
        min_length=1,
        max_length=200,
        description="List of keywords to add"
    )
    bid: Optional[float] = Field(
        default=None,
        gt=0,
        description="Bid for all keywords in currency units"
    )


class SetKeywordBidsInput(BaseModel):
    """Input for setting keyword bids."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    keyword_bids: List[Dict[str, Any]] = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="List of keyword bid settings: [{'keyword_id': 123, 'search_bid': 1.5, 'network_bid': 0.5}]"
    )


class DirectReportInput(BaseModel):
    """Input for Direct statistics report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    report_type: str = Field(
        default="CAMPAIGN_PERFORMANCE_REPORT",
        description="Report type: ACCOUNT_PERFORMANCE_REPORT, CAMPAIGN_PERFORMANCE_REPORT, AD_PERFORMANCE_REPORT, etc."
    )
    date_from: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Report start date (YYYY-MM-DD)"
    )
    date_to: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Report end date (YYYY-MM-DD)"
    )
    field_names: List[str] = Field(
        default_factory=lambda: ["CampaignName", "Impressions", "Clicks", "Cost"],
        description="Fields to include in report"
    )
    campaign_ids: Optional[List[int]] = Field(
        default=None,
        description="Filter by campaign IDs"
    )
    include_vat: bool = Field(
        default=True,
        description="Include VAT in cost values"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class UpdateAdGroupInput(BaseModel):
    """Input for updating an ad group."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    adgroup_id: int = Field(
        ...,
        description="Ad group ID to update"
    )
    name: Optional[str] = Field(
        default=None,
        max_length=255,
        description="New ad group name"
    )
    region_ids: Optional[List[int]] = Field(
        default=None,
        min_length=1,
        description="New list of region IDs for targeting"
    )
    negative_keywords: Optional[List[str]] = Field(
        default=None,
        description="Group-level negative keywords"
    )
    tracking_params: Optional[str] = Field(
        default=None,
        description="Tracking parameters for all ads in group"
    )


class UpdateTextAdInput(BaseModel):
    """Input for updating a text ad."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    ad_id: int = Field(
        ...,
        description="Ad ID to update"
    )
    title: Optional[str] = Field(
        default=None,
        max_length=56,
        description="New ad title (max 56 characters)"
    )
    title2: Optional[str] = Field(
        default=None,
        max_length=30,
        description="New second title (max 30 characters)"
    )
    text: Optional[str] = Field(
        default=None,
        max_length=81,
        description="New ad text (max 81 characters)"
    )
    href: Optional[str] = Field(
        default=None,
        description="New landing page URL"
    )


class ManageKeywordInput(BaseModel):
    """Input for managing keywords (suspend/resume/delete)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    keyword_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="Keyword IDs to manage"
    )


# --- Yandex Metrika Models ---

class GetCountersInput(BaseModel):
    """Input for getting Metrika counters."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    favorite: Optional[bool] = Field(
        default=None,
        description="Filter by favorite status"
    )
    search_string: Optional[str] = Field(
        default=None,
        description="Search string to filter counters by name or site"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class GetCounterInput(BaseModel):
    """Input for getting single counter details."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    counter_id: int = Field(
        ...,
        description="Metrika counter ID"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class CreateCounterInput(BaseModel):
    """Input for creating a Metrika counter."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Counter name"
    )
    site: str = Field(
        ...,
        description="Website URL"
    )


class GetGoalsInput(BaseModel):
    """Input for getting counter goals."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    counter_id: int = Field(
        ...,
        description="Metrika counter ID"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class CreateGoalInput(BaseModel):
    """Input for creating a goal."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    counter_id: int = Field(
        ...,
        description="Metrika counter ID"
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Goal name"
    )
    goal_type: str = Field(
        ...,
        description="Goal type: url, action, phone, email, messenger, etc."
    )
    conditions: List[Dict[str, str]] = Field(
        ...,
        description="Goal conditions, e.g., [{'type': 'exact', 'url': '/thank-you'}]"
    )


class MetrikaReportInput(BaseModel):
    """Input for Metrika statistics report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    counter_id: int = Field(
        ...,
        description="Metrika counter ID"
    )
    metrics: List[str] = Field(
        default_factory=lambda: ["ym:s:visits", "ym:s:users", "ym:s:bounceRate"],
        description="Metrics to retrieve (e.g., ym:s:visits, ym:s:users, ym:s:pageviews)"
    )
    dimensions: Optional[List[str]] = Field(
        default=None,
        description="Dimensions for grouping (e.g., ym:s:date, ym:s:trafficSource)"
    )
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date (YYYY-MM-DD), defaults to 7 days ago"
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date (YYYY-MM-DD), defaults to today"
    )
    filters: Optional[str] = Field(
        default=None,
        description="Filter expression (e.g., ym:s:trafficSource=='organic')"
    )
    sort: Optional[str] = Field(
        default=None,
        description="Sort field with optional '-' prefix for descending"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=100000,
        description="Maximum rows to return"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


class MetrikaByTimeInput(BaseModel):
    """Input for time-based Metrika report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    
    counter_id: int = Field(
        ...,
        description="Metrika counter ID"
    )
    metrics: List[str] = Field(
        default_factory=lambda: ["ym:s:visits"],
        description="Metrics to retrieve"
    )
    dimensions: Optional[List[str]] = Field(
        default=None,
        description="Dimensions for grouping"
    )
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date (YYYY-MM-DD)"
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date (YYYY-MM-DD)"
    )
    group: MetrikaGroupType = Field(
        default=MetrikaGroupType.DAY,
        description="Time grouping: day, week, month, quarter, year, hour, minute"
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


# =============================================================================
# Yandex Direct Tools
# =============================================================================

@mcp.tool(
    name="direct_get_campaigns",
    annotations={
        "title": "Get Yandex Direct Campaigns",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_get_campaigns(params: GetCampaignsInput) -> str:
    """Get list of advertising campaigns from Yandex Direct.
    
    Retrieves campaigns with their settings, statistics, and current status.
    Supports filtering by IDs, states, statuses, and types.
    
    Args:
        params: Filter and pagination parameters
    
    Returns:
        Campaign list in markdown or JSON format
    """
    try:
        selection_criteria = {}
        
        if params.campaign_ids:
            selection_criteria["Ids"] = params.campaign_ids
        if params.states:
            selection_criteria["States"] = [s.value for s in params.states]
        if params.statuses:
            selection_criteria["Statuses"] = [s.value for s in params.statuses]
        if params.types:
            selection_criteria["Types"] = [t.value for t in params.types]
        
        request_params = {
            "SelectionCriteria": selection_criteria,
            "FieldNames": [
                "Id", "Name", "Type", "State", "Status", "StatusPayment",
                "StartDate", "EndDate", "DailyBudget", "Statistics"
            ],
            "TextCampaignFieldNames": ["BiddingStrategy", "Settings"],
            "Page": {
                "Limit": params.limit,
                "Offset": params.offset
            }
        }
        
        result = await api_client.direct_request("campaigns", "get", request_params)
        campaigns = result.get("result", {}).get("Campaigns", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"campaigns": campaigns, "total": len(campaigns)}, indent=2, ensure_ascii=False)
        
        return _format_campaigns_markdown(campaigns)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_suspend_campaigns",
    annotations={
        "title": "Suspend Yandex Direct Campaigns",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_suspend_campaigns(params: ManageCampaignInput) -> str:
    """Suspend (pause) advertising campaigns.
    
    Suspended campaigns stop showing ads but retain all settings.
    Can be resumed later with direct_resume_campaigns.
    
    Args:
        params: Campaign IDs to suspend
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.campaign_ids}
        }
        
        result = await api_client.direct_request("campaigns", "suspend", request_params)
        suspend_results = result.get("result", {}).get("SuspendResults", [])
        
        success = []
        errors = []
        for r in suspend_results:
            if r.get("Id"):
                success.append(r["Id"])
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully suspended {len(success)} campaign(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_resume_campaigns",
    annotations={
        "title": "Resume Yandex Direct Campaigns",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_resume_campaigns(params: ManageCampaignInput) -> str:
    """Resume suspended advertising campaigns.
    
    Resumes campaigns that were previously suspended.
    Campaigns will start showing ads again.
    
    Args:
        params: Campaign IDs to resume
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.campaign_ids}
        }
        
        result = await api_client.direct_request("campaigns", "resume", request_params)
        resume_results = result.get("result", {}).get("ResumeResults", [])
        
        success = []
        errors = []
        for r in resume_results:
            if r.get("Id"):
                success.append(r["Id"])
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully resumed {len(success)} campaign(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_archive_campaigns",
    annotations={
        "title": "Archive Yandex Direct Campaigns",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_archive_campaigns(params: ManageCampaignInput) -> str:
    """Archive advertising campaigns.
    
    Archived campaigns are hidden from the main list but can be restored.
    Use this for campaigns you no longer need but want to keep for reference.
    
    Args:
        params: Campaign IDs to archive
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.campaign_ids}
        }
        
        result = await api_client.direct_request("campaigns", "archive", request_params)
        archive_results = result.get("result", {}).get("ArchiveResults", [])
        
        success = [r["Id"] for r in archive_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in archive_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully archived {len(success)} campaign(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_unarchive_campaigns",
    annotations={
        "title": "Unarchive Yandex Direct Campaigns",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_unarchive_campaigns(params: ManageCampaignInput) -> str:
    """Restore archived campaigns.
    
    Unarchives campaigns and makes them visible in the main campaign list.
    
    Args:
        params: Campaign IDs to unarchive
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.campaign_ids}
        }
        
        result = await api_client.direct_request("campaigns", "unarchive", request_params)
        unarchive_results = result.get("result", {}).get("UnarchiveResults", [])
        
        success = [r["Id"] for r in unarchive_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in unarchive_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully unarchived {len(success)} campaign(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_delete_campaigns",
    annotations={
        "title": "Delete Yandex Direct Campaigns",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_delete_campaigns(params: ManageCampaignInput) -> str:
    """Delete advertising campaigns permanently.
    
    WARNING: This action is irreversible. Deleted campaigns cannot be restored.
    Consider archiving campaigns instead if you might need them later.
    
    Args:
        params: Campaign IDs to delete
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.campaign_ids}
        }
        
        result = await api_client.direct_request("campaigns", "delete", request_params)
        delete_results = result.get("result", {}).get("DeleteResults", [])
        
        success = [r["Id"] for r in delete_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in delete_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully deleted {len(success)} campaign(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_update_campaign",
    annotations={
        "title": "Update Yandex Direct Campaign",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_update_campaign(params: UpdateCampaignInput) -> str:
    """Update campaign settings.
    
    Allows updating campaign name, daily budget, dates, and negative keywords.
    Only specified fields will be updated.
    
    Args:
        params: Campaign ID and fields to update
    
    Returns:
        Operation result
    """
    try:
        campaign_update = {"Id": params.campaign_id}
        
        if params.name:
            campaign_update["Name"] = params.name
        
        if params.daily_budget_amount is not None:
            campaign_update["DailyBudget"] = {
                "Amount": int(params.daily_budget_amount * 1_000_000),
                "Mode": params.daily_budget_mode.value if params.daily_budget_mode else "DISTRIBUTED"
            }
        
        if params.start_date:
            campaign_update["StartDate"] = params.start_date
        
        if params.end_date:
            campaign_update["EndDate"] = params.end_date
        
        if params.negative_keywords is not None:
            campaign_update["NegativeKeywords"] = {"Items": params.negative_keywords}
        
        request_params = {
            "Campaigns": [campaign_update]
        }
        
        result = await api_client.direct_request("campaigns", "update", request_params)
        update_results = result.get("result", {}).get("UpdateResults", [])
        
        errors = []
        for r in update_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
            if r.get("Warnings"):
                errors.extend([f"Warning: {w.get('Message', 'Unknown warning')}" for w in r["Warnings"]])
        
        if errors:
            return f"Update completed with issues:\n" + "\n".join(f"- {e}" for e in errors)
        
        return f"Campaign {params.campaign_id} updated successfully."
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_get_adgroups",
    annotations={
        "title": "Get Yandex Direct Ad Groups",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_get_adgroups(params: GetAdGroupsInput) -> str:
    """Get list of ad groups from Yandex Direct.
    
    Retrieves ad groups with their settings and targeting information.
    
    Args:
        params: Filter and pagination parameters
    
    Returns:
        Ad groups list in markdown or JSON format
    """
    try:
        selection_criteria = {}
        
        if params.campaign_ids:
            selection_criteria["CampaignIds"] = params.campaign_ids
        if params.adgroup_ids:
            selection_criteria["Ids"] = params.adgroup_ids
        
        request_params = {
            "SelectionCriteria": selection_criteria,
            "FieldNames": ["Id", "Name", "CampaignId", "RegionIds", "Type", "Status", "ServingStatus"],
            "Page": {
                "Limit": params.limit,
                "Offset": params.offset
            }
        }
        
        result = await api_client.direct_request("adgroups", "get", request_params)
        groups = result.get("result", {}).get("AdGroups", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"ad_groups": groups, "total": len(groups)}, indent=2, ensure_ascii=False)
        
        return _format_adgroups_markdown(groups)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_create_adgroup",
    annotations={
        "title": "Create Yandex Direct Ad Group",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_create_adgroup(params: CreateAdGroupInput) -> str:
    """Create a new ad group in a campaign.
    
    Creates an ad group with specified name and targeting regions.
    
    Args:
        params: Ad group settings
    
    Returns:
        Created ad group ID
    """
    try:
        adgroup = {
            "Name": params.name,
            "CampaignId": params.campaign_id,
            "RegionIds": params.region_ids
        }
        
        if params.negative_keywords:
            adgroup["NegativeKeywords"] = {"Items": params.negative_keywords}
        
        request_params = {
            "AdGroups": [adgroup]
        }
        
        result = await api_client.direct_request("adgroups", "add", request_params)
        add_results = result.get("result", {}).get("AddResults", [])
        
        if add_results and add_results[0].get("Id"):
            return f"Ad group created successfully. ID: {add_results[0]['Id']}"
        
        errors = []
        for r in add_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
        
        return f"Failed to create ad group:\n" + "\n".join(f"- {e}" for e in errors)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_get_ads",
    annotations={
        "title": "Get Yandex Direct Ads",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_get_ads(params: GetAdsInput) -> str:
    """Get list of ads from Yandex Direct.
    
    Retrieves ads with their content and moderation status.
    
    Args:
        params: Filter and pagination parameters
    
    Returns:
        Ads list in markdown or JSON format
    """
    try:
        selection_criteria = {}
        
        if params.campaign_ids:
            selection_criteria["CampaignIds"] = params.campaign_ids
        if params.adgroup_ids:
            selection_criteria["AdGroupIds"] = params.adgroup_ids
        if params.ad_ids:
            selection_criteria["Ids"] = params.ad_ids
        if params.states:
            selection_criteria["States"] = [s.value for s in params.states]
        if params.statuses:
            selection_criteria["Statuses"] = [s.value for s in params.statuses]
        
        request_params = {
            "SelectionCriteria": selection_criteria,
            "FieldNames": ["Id", "AdGroupId", "CampaignId", "Type", "State", "Status", "StatusClarification"],
            "TextAdFieldNames": ["Title", "Title2", "Text", "Href", "Mobile", "DisplayDomain"],
            "Page": {
                "Limit": params.limit,
                "Offset": params.offset
            }
        }
        
        result = await api_client.direct_request("ads", "get", request_params)
        ads = result.get("result", {}).get("Ads", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"ads": ads, "total": len(ads)}, indent=2, ensure_ascii=False)
        
        return _format_ads_markdown(ads)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_create_text_ad",
    annotations={
        "title": "Create Yandex Direct Text Ad",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_create_text_ad(params: CreateTextAdInput) -> str:
    """Create a new text ad.
    
    Creates a text ad in the specified ad group.
    The ad will be in DRAFT status until moderated.
    
    Args:
        params: Ad content and settings
    
    Returns:
        Created ad ID
    """
    try:
        text_ad = {
            "Title": params.title,
            "Text": params.text,
            "Href": params.href,
            "Mobile": "YES" if params.mobile else "NO"
        }
        
        if params.title2:
            text_ad["Title2"] = params.title2
        
        ad = {
            "AdGroupId": params.adgroup_id,
            "TextAd": text_ad
        }
        
        request_params = {
            "Ads": [ad]
        }
        
        result = await api_client.direct_request("ads", "add", request_params)
        add_results = result.get("result", {}).get("AddResults", [])
        
        if add_results and add_results[0].get("Id"):
            return f"Ad created successfully. ID: {add_results[0]['Id']}\n\nNote: Submit for moderation using direct_moderate_ads."
        
        errors = []
        for r in add_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
        
        return f"Failed to create ad:\n" + "\n".join(f"- {e}" for e in errors)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_moderate_ads",
    annotations={
        "title": "Submit Ads for Moderation",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_moderate_ads(params: ManageAdInput) -> str:
    """Submit ads for moderation.
    
    Sends ads with DRAFT status to Yandex moderators for review.
    
    Args:
        params: Ad IDs to moderate
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }
        
        result = await api_client.direct_request("ads", "moderate", request_params)
        moderate_results = result.get("result", {}).get("ModerateResults", [])
        
        success = [r["Id"] for r in moderate_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in moderate_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])
        
        response = f"Successfully submitted {len(success)} ad(s) for moderation."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_suspend_ads",
    annotations={
        "title": "Suspend Yandex Direct Ads",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_suspend_ads(params: ManageAdInput) -> str:
    """Suspend (pause) ads.
    
    Args:
        params: Ad IDs to suspend
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }
        
        result = await api_client.direct_request("ads", "suspend", request_params)
        suspend_results = result.get("result", {}).get("SuspendResults", [])
        
        success = [r["Id"] for r in suspend_results if r.get("Id") and not r.get("Errors")]
        
        return f"Successfully suspended {len(success)} ad(s)."
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_resume_ads",
    annotations={
        "title": "Resume Yandex Direct Ads",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_resume_ads(params: ManageAdInput) -> str:
    """Resume suspended ads.
    
    Args:
        params: Ad IDs to resume
    
    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }
        
        result = await api_client.direct_request("ads", "resume", request_params)
        resume_results = result.get("result", {}).get("ResumeResults", [])
        
        success = [r["Id"] for r in resume_results if r.get("Id") and not r.get("Errors")]
        
        return f"Successfully resumed {len(success)} ad(s)."
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_get_keywords",
    annotations={
        "title": "Get Yandex Direct Keywords",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_get_keywords(params: GetKeywordsInput) -> str:
    """Get list of keywords from Yandex Direct.
    
    Retrieves keywords with their bids and status.
    
    Args:
        params: Filter and pagination parameters
    
    Returns:
        Keywords list in markdown or JSON format
    """
    try:
        selection_criteria = {}
        
        if params.campaign_ids:
            selection_criteria["CampaignIds"] = params.campaign_ids
        if params.adgroup_ids:
            selection_criteria["AdGroupIds"] = params.adgroup_ids
        if params.keyword_ids:
            selection_criteria["Ids"] = params.keyword_ids
        
        request_params = {
            "SelectionCriteria": selection_criteria,
            "FieldNames": ["Id", "Keyword", "AdGroupId", "CampaignId", "Bid", "State", "Status"],
            "Page": {
                "Limit": params.limit,
                "Offset": params.offset
            }
        }
        
        result = await api_client.direct_request("keywords", "get", request_params)
        keywords = result.get("result", {}).get("Keywords", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"keywords": keywords, "total": len(keywords)}, indent=2, ensure_ascii=False)
        
        return _format_keywords_markdown(keywords)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_add_keywords",
    annotations={
        "title": "Add Keywords to Yandex Direct",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_add_keywords(params: AddKeywordsInput) -> str:
    """Add keywords to an ad group.
    
    Creates new keywords in the specified ad group.
    
    Args:
        params: Keywords to add
    
    Returns:
        Created keyword IDs
    """
    try:
        keywords_list = []
        for kw in params.keywords:
            keyword = {
                "Keyword": kw,
                "AdGroupId": params.adgroup_id
            }
            if params.bid:
                keyword["Bid"] = int(params.bid * 1_000_000)
            keywords_list.append(keyword)
        
        request_params = {
            "Keywords": keywords_list
        }
        
        result = await api_client.direct_request("keywords", "add", request_params)
        add_results = result.get("result", {}).get("AddResults", [])
        
        success_ids = [r["Id"] for r in add_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in add_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
        
        response = f"Successfully added {len(success_ids)} keyword(s)."
        if success_ids:
            response += f"\nIDs: {', '.join(map(str, success_ids))}"
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)
        
        return response
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_set_keyword_bids",
    annotations={
        "title": "Set Keyword Bids",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_set_keyword_bids(params: SetKeywordBidsInput) -> str:
    """Set bids for keywords.
    
    Updates search and/or network bids for specified keywords.
    
    Args:
        params: Keyword bid settings
    
    Returns:
        Operation result
    """
    try:
        keyword_bids = []
        for kb in params.keyword_bids:
            bid_item = {"KeywordId": kb["keyword_id"]}
            if kb.get("search_bid"):
                bid_item["SearchBid"] = int(kb["search_bid"] * 1_000_000)
            if kb.get("network_bid"):
                bid_item["NetworkBid"] = int(kb["network_bid"] * 1_000_000)
            keyword_bids.append(bid_item)
        
        request_params = {
            "KeywordBids": keyword_bids
        }
        
        result = await api_client.direct_request("keywordbids", "set", request_params)
        set_results = result.get("result", {}).get("SetResults", [])
        
        success = [r["KeywordId"] for r in set_results if r.get("KeywordId") and not r.get("Errors")]
        
        return f"Successfully updated bids for {len(success)} keyword(s)."
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_get_statistics",
    annotations={
        "title": "Get Yandex Direct Statistics",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_get_statistics(params: DirectReportInput) -> str:
    """Get campaign statistics report from Yandex Direct.

    Retrieves performance statistics for campaigns, ads, or keywords.

    Report types:
    - ACCOUNT_PERFORMANCE_REPORT - Account level stats
    - CAMPAIGN_PERFORMANCE_REPORT - Campaign level stats (default)
    - AD_PERFORMANCE_REPORT - Ad level stats
    - ADGROUP_PERFORMANCE_REPORT - Ad group level stats
    - CRITERIA_PERFORMANCE_REPORT - Keyword level stats

    Common fields:
    - CampaignName, CampaignId - Campaign info
    - Impressions, Clicks, Cost - Basic metrics
    - Ctr, AvgCpc, ConversionRate - Calculated metrics
    - Date - For daily breakdown

    Args:
        params: Report parameters including date range and fields

    Returns:
        Statistics report in markdown or JSON format
    """
    try:
        # Build report definition
        report_def = {
            "SelectionCriteria": {
                "DateFrom": params.date_from,
                "DateTo": params.date_to
            },
            "FieldNames": params.field_names,
            "ReportName": f"Report_{params.date_from}_{params.date_to}",
            "ReportType": params.report_type,
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES" if params.include_vat else "NO",
            "IncludeDiscount": "NO"
        }

        if params.campaign_ids:
            report_def["SelectionCriteria"]["Filter"] = [{
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": [str(cid) for cid in params.campaign_ids]
            }]

        # Get Direct token
        token = api_client._get_direct_token()
        if not token:
            raise ValueError("Yandex Direct API token not configured.")

        url = f"{api_client._get_direct_url()}/reports"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json",
            "processingMode": "auto",
            "returnMoneyInMicros": "false",
            "skipReportHeader": "true",
            "skipColumnHeader": "false",
            "skipReportSummary": "true"
        }

        if api_client.client_login:
            headers["Client-Login"] = api_client.client_login

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, json={"params": report_def}, headers=headers)

            if response.status_code == 200:
                # Parse TSV response
                lines = response.text.strip().split("\n")
                if len(lines) < 2:
                    return "No data found for the specified period."

                header = lines[0].split("\t")
                data_rows = [line.split("\t") for line in lines[1:] if line.strip()]

                if params.response_format == ResponseFormat.JSON:
                    result = []
                    for row in data_rows:
                        result.append(dict(zip(header, row)))
                    return json.dumps({"data": result, "total": len(result)}, indent=2, ensure_ascii=False)

                # Format as markdown
                md_lines = ["# Direct Statistics Report\n"]
                md_lines.append(f"**Period**: {params.date_from} — {params.date_to}")
                md_lines.append(f"**Report type**: {params.report_type}\n")

                md_lines.append("| " + " | ".join(header) + " |")
                md_lines.append("| " + " | ".join(["---"] * len(header)) + " |")

                for row in data_rows[:100]:  # Limit to 100 rows
                    md_lines.append("| " + " | ".join(row) + " |")

                if len(data_rows) > 100:
                    md_lines.append(f"\n*...and {len(data_rows) - 100} more rows*")

                return "\n".join(md_lines)

            elif response.status_code == 201 or response.status_code == 202:
                return "Report is being generated. Please try again in a few seconds."

            else:
                response.raise_for_status()

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_update_adgroup",
    annotations={
        "title": "Update Yandex Direct Ad Group",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_update_adgroup(params: UpdateAdGroupInput) -> str:
    """Update ad group settings.

    Allows updating ad group name, regions, negative keywords, and tracking params.
    Only specified fields will be updated.

    Args:
        params: Ad group ID and fields to update

    Returns:
        Operation result
    """
    try:
        adgroup_update = {"Id": params.adgroup_id}

        if params.name:
            adgroup_update["Name"] = params.name

        if params.region_ids:
            adgroup_update["RegionIds"] = params.region_ids

        if params.negative_keywords is not None:
            adgroup_update["NegativeKeywords"] = {"Items": params.negative_keywords}

        if params.tracking_params:
            adgroup_update["TrackingParams"] = params.tracking_params

        request_params = {
            "AdGroups": [adgroup_update]
        }

        result = await api_client.direct_request("adgroups", "update", request_params)
        update_results = result.get("result", {}).get("UpdateResults", [])

        errors = []
        for r in update_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
            if r.get("Warnings"):
                errors.extend([f"Warning: {w.get('Message', 'Unknown warning')}" for w in r["Warnings"]])

        if errors:
            return f"Update completed with issues:\n" + "\n".join(f"- {e}" for e in errors)

        return f"Ad group {params.adgroup_id} updated successfully."

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_update_ad",
    annotations={
        "title": "Update Yandex Direct Ad",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_update_ad(params: UpdateTextAdInput) -> str:
    """Update a text ad.

    Allows updating ad title, text, and landing page URL.
    Only specified fields will be updated.
    Note: Updated ad will need to be re-moderated.

    Args:
        params: Ad ID and fields to update

    Returns:
        Operation result
    """
    try:
        text_ad_update = {}

        if params.title:
            text_ad_update["Title"] = params.title
        if params.title2:
            text_ad_update["Title2"] = params.title2
        if params.text:
            text_ad_update["Text"] = params.text
        if params.href:
            text_ad_update["Href"] = params.href

        if not text_ad_update:
            return "No fields specified for update."

        ad_update = {
            "Id": params.ad_id,
            "TextAd": text_ad_update
        }

        request_params = {
            "Ads": [ad_update]
        }

        result = await api_client.direct_request("ads", "update", request_params)
        update_results = result.get("result", {}).get("UpdateResults", [])

        errors = []
        for r in update_results:
            if r.get("Errors"):
                errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
            if r.get("Warnings"):
                errors.extend([f"Warning: {w.get('Message', 'Unknown warning')}" for w in r["Warnings"]])

        if errors:
            return f"Update completed with issues:\n" + "\n".join(f"- {e}" for e in errors)

        return f"Ad {params.ad_id} updated successfully. Note: Submit for moderation using direct_moderate_ads."

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_delete_keywords",
    annotations={
        "title": "Delete Yandex Direct Keywords",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_delete_keywords(params: ManageKeywordInput) -> str:
    """Delete keywords permanently.

    WARNING: This action is irreversible.

    Args:
        params: Keyword IDs to delete

    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.keyword_ids}
        }

        result = await api_client.direct_request("keywords", "delete", request_params)
        delete_results = result.get("result", {}).get("DeleteResults", [])

        success = [r["Id"] for r in delete_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in delete_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])

        response = f"Successfully deleted {len(success)} keyword(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)

        return response

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_delete_ads",
    annotations={
        "title": "Delete Yandex Direct Ads",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def direct_delete_ads(params: ManageAdInput) -> str:
    """Delete ads permanently.

    WARNING: This action is irreversible.
    Consider archiving ads instead if you might need them later.

    Args:
        params: Ad IDs to delete

    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }

        result = await api_client.direct_request("ads", "delete", request_params)
        delete_results = result.get("result", {}).get("DeleteResults", [])

        success = [r["Id"] for r in delete_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in delete_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])

        response = f"Successfully deleted {len(success)} ad(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)

        return response

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_archive_ads",
    annotations={
        "title": "Archive Yandex Direct Ads",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_archive_ads(params: ManageAdInput) -> str:
    """Archive ads.

    Archived ads are hidden from the main list but can be restored.

    Args:
        params: Ad IDs to archive

    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }

        result = await api_client.direct_request("ads", "archive", request_params)
        archive_results = result.get("result", {}).get("ArchiveResults", [])

        success = [r["Id"] for r in archive_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in archive_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])

        response = f"Successfully archived {len(success)} ad(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)

        return response

    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="direct_unarchive_ads",
    annotations={
        "title": "Unarchive Yandex Direct Ads",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def direct_unarchive_ads(params: ManageAdInput) -> str:
    """Restore archived ads.

    Unarchives ads and makes them visible in the main ad list.

    Args:
        params: Ad IDs to unarchive

    Returns:
        Operation result
    """
    try:
        request_params = {
            "SelectionCriteria": {"Ids": params.ad_ids}
        }

        result = await api_client.direct_request("ads", "unarchive", request_params)
        unarchive_results = result.get("result", {}).get("UnarchiveResults", [])

        success = [r["Id"] for r in unarchive_results if r.get("Id") and not r.get("Errors")]
        errors = []
        for r in unarchive_results:
            if r.get("Errors"):
                errors.extend([f"ID {r.get('Id', '?')}: {e.get('Message', 'Unknown error')}" for e in r["Errors"]])

        response = f"Successfully unarchived {len(success)} ad(s)."
        if errors:
            response += f"\n\nErrors:\n" + "\n".join(f"- {e}" for e in errors)

        return response

    except Exception as e:
        return _handle_api_error(e)


# =============================================================================
# Yandex Metrika Tools
# =============================================================================

@mcp.tool(
    name="metrika_get_counters",
    annotations={
        "title": "Get Yandex Metrika Counters",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_get_counters(params: GetCountersInput) -> str:
    """Get list of Metrika counters (tags).
    
    Retrieves all counters accessible to the user.
    
    Args:
        params: Filter parameters
    
    Returns:
        Counters list in markdown or JSON format
    """
    try:
        query_params = {}
        if params.favorite is not None:
            query_params["favorite"] = str(params.favorite).lower()
        if params.search_string:
            query_params["search_string"] = params.search_string
        
        result = await api_client.metrika_request(
            "/management/v1/counters",
            params=query_params
        )
        
        counters = result.get("counters", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"counters": counters, "total": result.get("rows", len(counters))}, indent=2, ensure_ascii=False)
        
        return _format_metrika_counters_markdown(counters)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_get_counter",
    annotations={
        "title": "Get Yandex Metrika Counter Details",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_get_counter(params: GetCounterInput) -> str:
    """Get detailed information about a specific counter.
    
    Retrieves full counter settings including code options, webvisor, and grants.
    
    Args:
        params: Counter ID
    
    Returns:
        Counter details in markdown or JSON format
    """
    try:
        result = await api_client.metrika_request(
            f"/management/v1/counter/{params.counter_id}"
        )
        
        counter = result.get("counter", {})
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps(counter, indent=2, ensure_ascii=False)
        
        lines = [f"# Counter: {counter.get('name', 'Unnamed')} (ID: {counter.get('id')})"]
        lines.append(f"\n## Basic Info")
        lines.append(f"- **Site**: {counter.get('site2', {}).get('site', 'N/A')}")
        lines.append(f"- **Status**: {counter.get('status', 'N/A')}")
        lines.append(f"- **Code Status**: {counter.get('code_status', 'N/A')}")
        lines.append(f"- **Owner**: {counter.get('owner_login', 'N/A')}")
        lines.append(f"- **Created**: {counter.get('create_time', 'N/A')}")
        
        if counter.get("webvisor"):
            webvisor = counter["webvisor"]
            lines.append(f"\n## Webvisor")
            lines.append(f"- **Version**: {webvisor.get('wv_version', 'N/A')}")
            lines.append(f"- **Enabled**: {webvisor.get('arch_enabled', False)}")
        
        goals = counter.get("goals", [])
        if goals:
            lines.append(f"\n## Goals ({len(goals)})")
            for goal in goals[:10]:
                lines.append(f"- {goal.get('name', 'Unnamed')} (ID: {goal.get('id')})")
        
        return "\n".join(lines)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_create_counter",
    annotations={
        "title": "Create Yandex Metrika Counter",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def metrika_create_counter(params: CreateCounterInput) -> str:
    """Create a new Metrika counter.
    
    Creates a counter for tracking website statistics.
    
    Args:
        params: Counter name and site URL
    
    Returns:
        Created counter ID and tracking code
    """
    try:
        data = {
            "counter": {
                "name": params.name,
                "site2": {"site": params.site}
            }
        }
        
        result = await api_client.metrika_request(
            "/management/v1/counters",
            method="POST",
            data=data
        )
        
        counter = result.get("counter", {})
        
        return f"""Counter created successfully!

**ID**: {counter.get('id')}
**Name**: {counter.get('name')}
**Site**: {counter.get('site2', {}).get('site')}

Add this tracking code to your website:
```html
<!-- Yandex.Metrika counter -->
<script type="text/javascript">
   (function(m,e,t,r,i,k,a){{m[i]=m[i]||function(){{(m[i].a=m[i].a||[]).push(arguments)}};
   m[i].l=1*new Date();
   for (var j = 0; j < document.scripts.length; j++) {{if (document.scripts[j].src === r) {{ return; }}}}
   k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)}})
   (window, document, "script", "https://mc.yandex.ru/metrika/tag.js", "ym");

   ym({counter.get('id')}, "init", {{
        clickmap:true,
        trackLinks:true,
        accurateTrackBounce:true
   }});
</script>
```"""
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_get_goals",
    annotations={
        "title": "Get Yandex Metrika Goals",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_get_goals(params: GetGoalsInput) -> str:
    """Get goals for a Metrika counter.
    
    Retrieves all configured goals for tracking conversions.
    
    Args:
        params: Counter ID
    
    Returns:
        Goals list in markdown or JSON format
    """
    try:
        result = await api_client.metrika_request(
            f"/management/v1/counter/{params.counter_id}/goals"
        )
        
        goals = result.get("goals", [])
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"goals": goals, "total": len(goals)}, indent=2, ensure_ascii=False)
        
        if not goals:
            return "No goals configured for this counter."
        
        lines = [f"# Goals for Counter {params.counter_id}\n"]
        for goal in goals:
            lines.append(f"## {goal.get('name', 'Unnamed')} (ID: {goal.get('id')})")
            lines.append(f"- **Type**: {goal.get('type', 'N/A')}")
            
            conditions = goal.get("conditions", [])
            if conditions:
                lines.append("- **Conditions**:")
                for cond in conditions:
                    lines.append(f"  - {cond.get('type', 'N/A')}: {cond.get('url', cond.get('value', 'N/A'))}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_create_goal",
    annotations={
        "title": "Create Yandex Metrika Goal",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False
    }
)
async def metrika_create_goal(params: CreateGoalInput) -> str:
    """Create a new goal for a Metrika counter.
    
    Goals track conversions like page visits, form submissions, clicks, etc.
    
    Args:
        params: Goal settings
    
    Returns:
        Created goal ID
    """
    try:
        data = {
            "goal": {
                "name": params.name,
                "type": params.goal_type,
                "conditions": params.conditions
            }
        }
        
        result = await api_client.metrika_request(
            f"/management/v1/counter/{params.counter_id}/goals",
            method="POST",
            data=data
        )
        
        goal = result.get("goal", {})
        
        return f"Goal created successfully!\n\n**ID**: {goal.get('id')}\n**Name**: {goal.get('name')}\n**Type**: {goal.get('type')}"
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_get_report",
    annotations={
        "title": "Get Yandex Metrika Statistics Report",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_get_report(params: MetrikaReportInput) -> str:
    """Get statistics report from Yandex Metrika.
    
    Retrieves traffic statistics with customizable metrics and dimensions.
    
    Common metrics:
    - ym:s:visits - Number of sessions
    - ym:s:users - Number of users
    - ym:s:pageviews - Page views
    - ym:s:bounceRate - Bounce rate
    - ym:s:avgVisitDurationSeconds - Average session duration
    
    Common dimensions:
    - ym:s:date - Date
    - ym:s:trafficSource - Traffic source
    - ym:s:searchEngine - Search engine
    - ym:s:regionCountry - Country
    - ym:s:deviceCategory - Device type
    
    Args:
        params: Report parameters
    
    Returns:
        Statistics data in markdown or JSON format
    """
    try:
        query_params = {
            "id": params.counter_id,
            "metrics": ",".join(params.metrics),
            "limit": params.limit
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
        
        result = await api_client.metrika_request(
            "/stat/v1/data",
            params=query_params
        )
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps(result, indent=2, ensure_ascii=False)
        
        return _format_metrika_report_markdown(result)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_get_report_by_time",
    annotations={
        "title": "Get Yandex Metrika Time-Based Report",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_get_report_by_time(params: MetrikaByTimeInput) -> str:
    """Get time-based statistics report from Yandex Metrika.
    
    Retrieves statistics grouped by time periods (day, week, month, etc.).
    Useful for tracking trends and building charts.
    
    Args:
        params: Report parameters with time grouping
    
    Returns:
        Time-series data in markdown or JSON format
    """
    try:
        query_params = {
            "id": params.counter_id,
            "metrics": ",".join(params.metrics),
            "group": params.group.value
        }
        
        if params.dimensions:
            query_params["dimensions"] = ",".join(params.dimensions)
        if params.date1:
            query_params["date1"] = params.date1
        if params.date2:
            query_params["date2"] = params.date2
        
        result = await api_client.metrika_request(
            "/stat/v1/data/bytime",
            params=query_params
        )
        
        if params.response_format == ResponseFormat.JSON:
            return json.dumps(result, indent=2, ensure_ascii=False)
        
        # Format time-based report
        lines = ["# Time-Based Report\n"]
        
        query = result.get("query", {})
        lines.append(f"**Period**: {query.get('date1', 'N/A')} — {query.get('date2', 'N/A')}")
        lines.append(f"**Grouping**: {params.group.value}\n")
        
        time_intervals = result.get("time_intervals", [])
        data = result.get("data", [])
        
        if data:
            for row in data:
                dims = row.get("dimensions", [])
                metrics = row.get("metrics", [[]])
                
                dim_str = " / ".join(
                    str(d.get("name", d.get("id", "N/A"))) if isinstance(d, dict) else str(d)
                    for d in dims
                ) if dims else "Total"
                
                lines.append(f"## {dim_str}")
                
                # Show time series
                if time_intervals and metrics:
                    for i, interval in enumerate(time_intervals):
                        interval_str = " — ".join(str(t) for t in interval) if isinstance(interval, list) else str(interval)
                        values = [m[i] if i < len(m) else 0 for m in metrics]
                        values_str = ", ".join(f"{v:,.2f}" for v in values)
                        lines.append(f"- {interval_str}: {values_str}")
                
                lines.append("")
        
        return "\n".join(lines)
    
    except Exception as e:
        return _handle_api_error(e)


@mcp.tool(
    name="metrika_delete_counter",
    annotations={
        "title": "Delete Yandex Metrika Counter",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def metrika_delete_counter(params: GetCounterInput) -> str:
    """Delete a Metrika counter.
    
    WARNING: This action is irreversible. All historical data will be lost.
    
    Args:
        params: Counter ID to delete
    
    Returns:
        Operation result
    """
    try:
        await api_client.metrika_request(
            f"/management/v1/counter/{params.counter_id}",
            method="DELETE"
        )
        
        return f"Counter {params.counter_id} deleted successfully."
    
    except Exception as e:
        return _handle_api_error(e)


# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()
