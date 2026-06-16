"""Pydantic models for Yandex Direct API."""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .common import ResponseFormat


# =============================================================================
# Enums
# =============================================================================

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


class BiddingStrategyType(str, Enum):
    """Bidding strategy type for search."""
    HIGHEST_POSITION = "HIGHEST_POSITION"
    WB_MAXIMUM_CLICKS = "WB_MAXIMUM_CLICKS"
    WB_MAXIMUM_CONVERSION_RATE = "WB_MAXIMUM_CONVERSION_RATE"
    AVERAGE_CPC = "AVERAGE_CPC"
    AVERAGE_CPA = "AVERAGE_CPA"
    AVERAGE_ROI = "AVERAGE_ROI"
    WEEKLY_CLICK_PACKAGE = "WEEKLY_CLICK_PACKAGE"
    PAY_FOR_CONVERSION = "PAY_FOR_CONVERSION"
    PAY_FOR_CONVERSION_CRR = "PAY_FOR_CONVERSION_CRR"
    SERVING_OFF = "SERVING_OFF"


# =============================================================================
# Campaign Models
# =============================================================================

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
    bidding_strategy_type: Optional[BiddingStrategyType] = Field(
        default=None,
        description="Bidding strategy type: WB_MAXIMUM_CLICKS, AVERAGE_CPC, etc."
    )
    weekly_spend_limit: Optional[float] = Field(
        default=None,
        gt=0,
        description="Weekly budget limit in currency units (for WB_MAXIMUM_CLICKS, AVERAGE_CPC)"
    )
    bid_ceiling: Optional[float] = Field(
        default=None,
        gt=0,
        description="Maximum bid in currency units (for WB_MAXIMUM_CLICKS)"
    )
    average_cpc: Optional[float] = Field(
        default=None,
        gt=0,
        description="Target average CPC in currency units (for AVERAGE_CPC strategy)"
    )
    goal_id: Optional[int] = Field(
        default=None,
        description="Metrika goal ID for conversion optimization strategies"
    )
    counter_ids: Optional[List[int]] = Field(
        default=None,
        description="Metrika counter IDs to attach to the campaign"
    )
    add_metrica_tag: Optional[bool] = Field(
        default=None,
        description="Enable Metrika URL tagging (yclid parameter) for conversion tracking"
    )
    enable_site_monitoring: Optional[bool] = Field(
        default=None,
        description="Enable site availability monitoring"
    )
    enable_extended_ad_title: Optional[bool] = Field(
        default=None,
        description="Enable extended ad title"
    )
    priority_goals: Optional[List[dict]] = Field(
        default=None,
        description="Priority goals list: [{'goal_id': 123, 'value': 7000}, ...]. Value in currency units."
    )
    excluded_sites: Optional[List[str]] = Field(
        default=None,
        description="Excluded placement sites/apps for РСЯ (e.g. ['site.com', 'app.id'])"
    )
    attribution_model: Optional[str] = Field(
        default=None,
        description="Attribution model: FC, LC, LSC, LYDC, FCCD, LSCCD, LYDCCD, AUTO"
    )


class NetworkStrategyType(str, Enum):
    """Network (РСЯ) bidding strategy type."""
    SERVING_OFF = "SERVING_OFF"
    NETWORK_DEFAULT = "NETWORK_DEFAULT"
    WB_MAXIMUM_CLICKS = "WB_MAXIMUM_CLICKS"
    AVERAGE_CPC = "AVERAGE_CPC"
    WB_MAXIMUM_CONVERSION_RATE = "WB_MAXIMUM_CONVERSION_RATE"
    # Smart campaign specific
    AVERAGE_CPC_PER_CAMPAIGN = "AVERAGE_CPC_PER_CAMPAIGN"
    AVERAGE_CPC_PER_FILTER = "AVERAGE_CPC_PER_FILTER"
    AVERAGE_CPA_PER_CAMPAIGN = "AVERAGE_CPA_PER_CAMPAIGN"
    AVERAGE_CPA_PER_FILTER = "AVERAGE_CPA_PER_FILTER"


class CreateCampaignInput(BaseModel):
    """Input for creating a new campaign."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    name: str = Field(
        ...,
        max_length=255,
        description="Campaign name"
    )
    start_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Campaign start date (YYYY-MM-DD)"
    )
    campaign_type: CampaignType = Field(
        default=CampaignType.TEXT_CAMPAIGN,
        description="Campaign type: TEXT_CAMPAIGN, CPM_BANNER_CAMPAIGN, etc."
    )
    end_date: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Campaign end date (YYYY-MM-DD)"
    )
    daily_budget_amount: Optional[float] = Field(
        default=None,
        gt=0,
        description="Daily budget in currency units"
    )
    daily_budget_mode: DailyBudgetMode = Field(
        default=DailyBudgetMode.DISTRIBUTED,
        description="Daily budget mode: STANDARD or DISTRIBUTED"
    )
    negative_keywords: Optional[List[str]] = Field(
        default=None,
        description="Campaign-level negative keywords"
    )
    counter_ids: Optional[List[int]] = Field(
        default=None,
        description="Metrika counter IDs"
    )
    goal_id: Optional[int] = Field(
        default=None,
        description="Priority Metrika goal ID for optimization"
    )
    goal_value: Optional[float] = Field(
        default=None,
        gt=0,
        description="Conversion value in currency"
    )
    # Bidding strategy for search
    search_strategy_type: BiddingStrategyType = Field(
        default=BiddingStrategyType.WB_MAXIMUM_CLICKS,
        description="Search bidding strategy"
    )
    # Bidding strategy for networks (РСЯ)
    network_strategy_type: NetworkStrategyType = Field(
        default=NetworkStrategyType.SERVING_OFF,
        description="Network (РСЯ) bidding strategy. SERVING_OFF = disable networks"
    )
    weekly_spend_limit: Optional[float] = Field(
        default=None,
        gt=0,
        description="Weekly budget limit for WB strategies"
    )
    bid_ceiling: Optional[float] = Field(
        default=None,
        gt=0,
        description="Maximum bid in currency units"
    )


# =============================================================================
# Ad Group Models
# =============================================================================

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
    feed_id: Optional[int] = Field(
        default=None,
        description="Feed ID for dynamic/smart ad groups"
    )
    autotargeting_categories: Optional[List[str]] = Field(
        default=None,
        description="Autotargeting categories for dynamic ads: EXACT, ALTERNATIVE, COMPETITOR, BROADER, ACCESSORY"
    )
    is_smart: bool = Field(
        default=False,
        description="If True, creates SmartAdGroup (for SMART_CAMPAIGN) instead of DynamicTextAdGroup"
    )
    is_unified: bool = Field(
        default=False,
        description="If True, creates UnifiedAdGroup (for UNIFIED_CAMPAIGN, uses v501 API)"
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


# =============================================================================
# Ad Models
# =============================================================================

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
    ad_image_hash: Optional[str] = Field(
        default=None,
        description="Image hash for the ad (from uploaded images)"
    )


class CreateDynamicTextAdInput(BaseModel):
    """Input for creating a dynamic text ad."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    adgroup_id: int = Field(
        ...,
        description="Ad group ID (must be in DYNAMIC_TEXT_CAMPAIGN)"
    )
    text: str = Field(
        ...,
        min_length=1,
        max_length=81,
        description="Ad text (max 81 characters). Title is auto-generated from feed."
    )
    ad_image_hash: Optional[str] = Field(
        default=None,
        description="Image hash for the ad"
    )
    sitelink_set_id: Optional[int] = Field(
        default=None,
        description="Sitelink set ID"
    )


class FeedFilterCondition(BaseModel):
    """A feed filter condition for ShoppingAd."""
    operand: str = Field(..., description="Feed field name (e.g., 'categoryId', 'price', 'manufacturer')")
    operator: str = Field(..., description="Operator: EQUALS_ANY, CONTAINS_ANY, NOT_CONTAINS_ALL, GREATER_THAN, LESS_THAN, IN_RANGE, EXISTS")
    arguments: List[str] = Field(..., description="Values to match (max 10)")


class CreateShoppingAdInput(BaseModel):
    """Input for creating a shopping ad (for UnifiedCampaign with feed)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    adgroup_id: int = Field(
        ...,
        description="Ad group ID (must be in UNIFIED_CAMPAIGN)"
    )
    feed_id: int = Field(
        ...,
        description="Feed ID from Feeds service"
    )
    feed_filter_conditions: Optional[List[FeedFilterCondition]] = Field(
        default=None,
        description="Filter conditions for selecting product offers (max 30)"
    )
    default_texts: Optional[List[str]] = Field(
        default=None,
        description="Fallback text when feed data unavailable"
    )
    business_id: Optional[int] = Field(
        default=None,
        description="Business profile ID (организация)"
    )
    sitelink_set_id: Optional[int] = Field(
        default=None,
        description="Sitelink set ID"
    )


class CreateImageAdInput(BaseModel):
    """Input for creating an image ad (banner)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    adgroup_id: int = Field(
        ...,
        description="Ad group ID to create ad in"
    )
    ad_image_hash: str = Field(
        ...,
        description="Image hash of the uploaded banner"
    )
    href: str = Field(
        ...,
        description="Landing page URL"
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
    ad_image_hash: Optional[str] = Field(
        default=None,
        description="Image hash to attach to the ad"
    )
    video_extension_creative_id: Optional[int] = Field(
        default=None,
        description="CreativeId for video extension (from direct_create_video_creative)"
    )
    sitelink_set_id: Optional[int] = Field(
        default=None,
        description="Sitelink set ID to attach to the ad"
    )


class ManageAdInput(BaseModel):
    """Input for managing ad state (suspend/resume/archive/unarchive/delete/moderate)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    ad_ids: Optional[List[int]] = Field(
        default=None,
        max_length=1000,
        description="Ad IDs to manage"
    )
    campaign_id: Optional[int] = Field(
        default=None,
        description="Campaign ID — moderate ALL draft ads in this campaign (workaround for large ad IDs)"
    )


# =============================================================================
# Keyword Models
# =============================================================================

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


class ManageKeywordInput(BaseModel):
    """Input for managing keywords (suspend/resume/delete)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    keyword_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="Keyword IDs to manage"
    )


# =============================================================================
# Statistics Models
# =============================================================================

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
