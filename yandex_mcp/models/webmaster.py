"""Pydantic models for Yandex Webmaster API v4."""

import re
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator
from .common import ResponseFormat

# Yandex Webmaster host IDs look like 'https:asiapk.ru:443'. Restricting the
# value to this shape keeps it from breaking out of the URL path (no slashes,
# no '..' segments) when it is interpolated into the API endpoint.
_HOST_ID_RE = re.compile(r"^[a-z]+:[a-zA-Z0-9.\-]+(:\d+)?$")


def _validate_host_id(value: str) -> str:
    if not _HOST_ID_RE.fullmatch(value):
        raise ValueError(
            "Invalid host_id format. Expected e.g. 'https:asiapk.ru:443' "
            "(see webmaster_get_hosts)."
        )
    return value


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

    _check_host_id = field_validator("host_id")(_validate_host_id)


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

    _check_host_id = field_validator("host_id")(_validate_host_id)
