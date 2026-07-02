"""Configuration constants for Yandex MCP Server."""

# API Endpoints
YANDEX_DIRECT_API_URL = "https://api.direct.yandex.com/json/v5"
YANDEX_DIRECT_API_URL_V501 = "https://api.direct.yandex.com/json/v501"
YANDEX_DIRECT_SANDBOX_URL = "https://api-sandbox.direct.yandex.com/json/v5"
YANDEX_METRIKA_API_URL = "https://api-metrika.yandex.net"

# Default timeout for API requests
DEFAULT_TIMEOUT = 30.0
REPORT_TIMEOUT = 120.0

# Wordstat moved to Yandex Cloud Search API v2 (the old api.wordstat.yandex.net
# host was decommissioned). Auth: "Authorization: Api-Key <key>" header.
YANDEX_WORDSTAT_API_URL = "https://searchapi.api.cloud.yandex.net/v2/wordstat"

YANDEX_WEBMASTER_API_URL = "https://api.webmaster.yandex.net/v4"
