"""Pydantic models for Yandex Webmaster API v4."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field
from .common import ResponseFormat


class WebmasterUserIdInput(BaseModel):
    """Input for resolving the Webmaster UserID of the token owner."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")


class WebmasterHostsInput(BaseModel):
    """Input for listing verified hosts."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )


class WebmasterHostSummaryInput(BaseModel):
    """Input for getting a host summary (SQI, indexed/excluded pages)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )


class WebmasterPopularQueriesInput(BaseModel):
    """Input for getting popular search queries for a host."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    order_by: str = Field(
        default="TOTAL_SHOWS",
        description="Sort order: TOTAL_SHOWS or TOTAL_CLICKS",
    )
    query_indicator: List[str] = Field(
        default_factory=lambda: [
            "TOTAL_SHOWS",
            "TOTAL_CLICKS",
            "AVG_SHOW_POSITION",
            "AVG_CLICK_POSITION",
        ],
        description="Indicators to return: TOTAL_SHOWS, TOTAL_CLICKS, AVG_SHOW_POSITION, AVG_CLICK_POSITION",
    )
    limit: int = Field(default=100, ge=1, le=500, description="Number of queries per page (max 500)")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )
