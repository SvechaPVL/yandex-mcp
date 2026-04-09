"""Pydantic models for Yandex AppMetrica API."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .common import ResponseFormat


# =============================================================================
# Enums
# =============================================================================

class AppMetricaGroupType(str, Enum):
    """Time grouping for AppMetrica reports."""
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    HOUR = "hour"
    MINUTE = "minute"


class LogsExportType(str, Enum):
    """Available Logs API export types."""
    CLICKS = "clicks"
    INSTALLATIONS = "installations"
    POSTBACKS = "postbacks"
    EVENTS = "events"
    SESSIONS_STARTS = "sessions_starts"
    CRASHES = "crashes"
    ERRORS = "errors"
    PUSH_TOKENS = "push_tokens"
    DEEPLINKS = "deeplinks"
    PROFILES = "profiles_v2"
    REVENUE_EVENTS = "revenue_events"
    ECOMMERCE_EVENTS = "ecommerce_events"
    AD_REVENUE_EVENTS = "ad_revenue_events"


# =============================================================================
# Management Models
# =============================================================================

class GetApplicationsInput(BaseModel):
    """Input for listing AppMetrica applications."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class GetApplicationInput(BaseModel):
    """Input for getting a single AppMetrica application."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


# =============================================================================
# Reporting Models
# =============================================================================

class AppMetricaReportInput(BaseModel):
    """Input for AppMetrica table report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    metrics: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Metric identifiers. All must share the same prefix. "
            "ym:ge: — users, sessions. "
            "ym:ce: — users. "
            "ym:i: — users, devices, installDevices. "
            "ym:cr: — users, crashes, crashDevices. "
            "ym:s: — users. ym:u: — users, devices. ym:p: — users, devices. "
            "Example: ['ym:ge:users', 'ym:ge:sessions']"
        ),
    )
    dimensions: Optional[List[str]] = Field(
        default=None,
        description=(
            "Dimension identifiers for grouping (same prefix as metrics). "
            "ym:ge: — date, regionCountry, regionCity, operatingSystemInfo, "
            "mobileDeviceBranding, mobileDeviceModel, appVersion, gender, "
            "ageInterval, screenResolution. "
            "ym:ce: — eventLabel, eventType. "
            "ym:i: — date, regionCountry, operatingSystemInfo, "
            "mobileDeviceBranding, appVersion, publisher. "
            "ym:cr: — date, operatingSystemInfo, appVersion, "
            "crashGroupName, mobileDeviceBranding. "
            "ym:s: — date, regionCountry, operatingSystemInfo."
        ),
    )
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in YYYY-MM-DD format",
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in YYYY-MM-DD format",
    )
    filters: Optional[str] = Field(
        default=None,
        description="Filter expression for data segmentation",
    )
    sort: Optional[str] = Field(
        default=None,
        description="Sort field. Prefix with '-' for descending",
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=100000,
        description="Maximum number of rows to return",
    )
    offset: int = Field(
        default=1,
        ge=1,
        description="Offset for pagination (1-based)",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class AppMetricaByTimeInput(BaseModel):
    """Input for AppMetrica time-based report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    metrics: List[str] = Field(
        ...,
        min_length=1,
        description="Metric identifiers (e.g. ['ym:ge:users'])",
    )
    dimensions: Optional[List[str]] = Field(
        default=None,
        description="Dimension identifiers for grouping",
    )
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in YYYY-MM-DD format",
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in YYYY-MM-DD format",
    )
    group: AppMetricaGroupType = Field(
        default=AppMetricaGroupType.DAY,
        description="Time grouping: day, week, month, hour, minute",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class AppMetricaDrilldownInput(BaseModel):
    """Input for AppMetrica drilldown report."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    metrics: List[str] = Field(
        ...,
        min_length=1,
        description="Metric identifiers",
    )
    dimensions: List[str] = Field(
        ...,
        min_length=1,
        description="Dimension identifiers for hierarchical grouping",
    )
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in YYYY-MM-DD format",
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in YYYY-MM-DD format",
    )
    filters: Optional[str] = Field(
        default=None,
        description="Filter expression",
    )
    parent_id: Optional[List[str]] = Field(
        default=None,
        description="Parent ID for drilling down into a branch",
    )
    limit: int = Field(default=100, ge=1, le=100000)
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


# =============================================================================
# Logs API Models
# =============================================================================

class AppMetricaLogsExportInput(BaseModel):
    """Input for AppMetrica Logs API export."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    export_type: LogsExportType = Field(
        ...,
        description=(
            "Data type to export: clicks, installations, postbacks, events, "
            "sessions_starts, crashes, errors, push_tokens, deeplinks, "
            "profiles_v2, revenue_events, ecommerce_events, ad_revenue_events"
        ),
    )
    date_since: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="Start date/datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    date_until: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="End date/datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    fields: Optional[List[str]] = Field(
        default=None,
        description="Specific fields to include in export. If empty, all fields returned.",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


# =============================================================================
# Events & Analytics Models
# =============================================================================

class AppMetricaEventsInput(BaseModel):
    """Input for getting event statistics."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in YYYY-MM-DD format",
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in YYYY-MM-DD format",
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=100000,
        description="Maximum number of events to return",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class AppMetricaProfilesInput(BaseModel):
    """Input for exporting user profiles."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    date_since: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    date_until: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    fields: Optional[List[str]] = Field(
        default=None,
        description=(
            "Specific profile fields. Defaults to: appmetrica_device_id, "
            "profile_id, os_name, device_manufacturer, device_model, city, country_iso_code"
        ),
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class AppMetricaCrashesInput(BaseModel):
    """Input for getting crash statistics."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    date1: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date in YYYY-MM-DD format",
    )
    date2: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date in YYYY-MM-DD format",
    )
    group_by: Optional[List[str]] = Field(
        default=None,
        description=(
            "Dimensions to group crashes by. "
            "Examples: ym:cr:operatingSystemInfo, ym:cr:appVersion, "
            "ym:cr:crashGroupName, ym:cr:mobileDeviceBranding"
        ),
    )
    limit: int = Field(default=100, ge=1, le=100000)
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class AppMetricaFunnelInput(BaseModel):
    """Input for building a conversion funnel from events."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    steps: List[str] = Field(
        ...,
        min_length=2,
        max_length=10,
        description=(
            "Ordered list of event names representing funnel steps. "
            "Example: ['app_open', 'view_catalog', 'add_to_cart', 'purchase']"
        ),
    )
    date_since: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="Start date (YYYY-MM-DD)",
    )
    date_until: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}",
        description="End date (YYYY-MM-DD)",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


# =============================================================================
# Push API Models
# =============================================================================

class CreatePushGroupInput(BaseModel):
    """Input for creating a push notification group."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    app_id: int = Field(..., description="Application ID in AppMetrica")
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique group name for organizing push sendings",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class GetPushStatusInput(BaseModel):
    """Input for checking push sending status."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    transfer_id: int = Field(..., description="Transfer ID returned from send-batch")
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )
