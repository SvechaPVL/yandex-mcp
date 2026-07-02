"""Pydantic models for Yandex Webmaster API v4."""

import re
from typing import List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator
from .common import ResponseFormat

# Yandex Webmaster host IDs look like 'https:asiapk.ru:443'. Restricting the
# value to this shape keeps it from breaking out of the URL path (no slashes,
# no '..' segments) when it is interpolated into the API endpoint.
_HOST_ID_RE = re.compile(r"^[a-z]+:[a-zA-Z0-9.\-]+(:\d+)?$")

# Sitemap IDs are opaque tokens returned by the API (see webmaster_list_sitemaps)
# and are interpolated into the URL path the same way host_id is. No slashes or
# '..' segments allowed.
_SITEMAP_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")

# host_url is sent as a JSON body value (not interpolated into a path), but the
# Webmaster API requires an absolute http(s) URL - reject anything else early
# with a clear error instead of letting the API return an opaque 400.
_HOST_URL_RE = re.compile(r"^https?://[^\s]+$")

VerificationType = Literal["DNS", "HTML_FILE", "META_TAG"]


def _validate_host_id(value: str) -> str:
    if not _HOST_ID_RE.fullmatch(value):
        raise ValueError(
            "Invalid host_id format. Expected e.g. 'https:asiapk.ru:443' "
            "(see webmaster_get_hosts)."
        )
    return value


def _validate_sitemap_id(value: str) -> str:
    if not _SITEMAP_ID_RE.fullmatch(value):
        raise ValueError(
            "Invalid sitemap_id format. Expected an opaque alphanumeric ID "
            "(see webmaster_list_sitemaps)."
        )
    return value


def _validate_host_url(value: str) -> str:
    if not _HOST_URL_RE.fullmatch(value):
        raise ValueError(
            "Invalid host_url format. Expected an absolute URL with scheme, "
            "e.g. 'https://asiapk.ru'."
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


# =============================================================================
# Host management (add / delete / verification)
# =============================================================================

class WebmasterAddHostInput(BaseModel):
    """Input for adding a new host to Yandex Webmaster."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_url: str = Field(
        ..., description="Absolute URL of the site to add, e.g. 'https://asiapk.ru'"
    )
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )

    _check_host_url = field_validator("host_url")(_validate_host_url)


class WebmasterDeleteHostInput(BaseModel):
    """Input for permanently removing a host from Yandex Webmaster.

    WARNING: this only unlinks the site from the Webmaster account - it does
    NOT affect the live site, robots.txt, or Yandex's search index. It does
    remove all history/statistics Webmaster has collected for the host, and
    re-adding the host later requires re-verification.
    """
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)


class WebmasterGetVerificationInput(BaseModel):
    """Input for getting the current verification status of a host."""
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


class WebmasterVerifyHostInput(BaseModel):
    """Input for starting the rights-verification procedure for a host."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    verification_type: VerificationType = Field(
        ...,
        description=(
            "Verification method to run: 'DNS' (TXT record), 'HTML_FILE' "
            "(upload yandex_<uin>.html to site root), or 'META_TAG' "
            "(meta tag in the homepage <head>). Call webmaster_get_verification "
            "first if you need the confirmation code/instructions before "
            "starting."
        ),
    )
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)


# =============================================================================
# User-added sitemaps
# =============================================================================

class WebmasterListSitemapsInput(BaseModel):
    """Input for listing sitemap files added by the user for a host."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )
    limit: int = Field(default=100, ge=1, le=100, description="Number of sitemaps per page (max 100)")
    offset: str = Field(
        default="0",
        description="Return sitemaps after this sitemap_id (pagination cursor, see API 'from' param).",
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'"
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)


class WebmasterAddSitemapInput(BaseModel):
    """Input for registering a Sitemap file for a host."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    sitemap_url: str = Field(
        ..., description="Absolute URL of the sitemap.xml file, e.g. 'https://asiapk.ru/sitemap.xml'"
    )
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)
    _check_sitemap_url = field_validator("sitemap_url")(_validate_host_url)


class WebmasterDeleteSitemapInput(BaseModel):
    """Input for removing a user-added Sitemap file from a host.

    WARNING: this stops Yandex from using this sitemap as a source of URLs to
    crawl. It does not remove the file from the site itself and does not
    de-index any pages that were already discovered through it.
    """
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    sitemap_id: str = Field(..., description="Sitemap ID (see webmaster_list_sitemaps)")
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)
    _check_sitemap_id = field_validator("sitemap_id")(_validate_sitemap_id)


# =============================================================================
# Recrawl
# =============================================================================

class WebmasterRecrawlUrlInput(BaseModel):
    """Input for submitting a single URL for priority recrawl."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    host_id: str = Field(..., description="Host ID, e.g. 'https:asiapk.ru:443' (see webmaster_get_hosts)")
    url: str = Field(
        ..., description="Absolute page URL to submit for recrawl, e.g. 'https://asiapk.ru/catalog/'"
    )
    user_id: Optional[int] = Field(
        default=None,
        description="Webmaster UserID. Resolved automatically from the token if omitted.",
    )

    _check_host_id = field_validator("host_id")(_validate_host_id)
    _check_url = field_validator("url")(_validate_host_url)


class WebmasterRecrawlQuotaInput(BaseModel):
    """Input for checking the daily recrawl quota for a host."""
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
