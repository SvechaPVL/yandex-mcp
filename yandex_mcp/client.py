"""Unified API client for Yandex Direct and Metrika APIs."""

import os
from typing import Any, Dict, Optional

import httpx

from .config import (
    DEFAULT_TIMEOUT,
    YANDEX_DIRECT_API_URL,
    YANDEX_DIRECT_API_URL_V501,
    YANDEX_DIRECT_SANDBOX_URL,
    YANDEX_METRIKA_API_URL,
)


class YandexAPIClient:
    """Unified client for Yandex Direct and Metrika APIs."""

    def __init__(self):
        self.direct_token = os.environ.get("YANDEX_DIRECT_TOKEN", "")
        self.metrika_token = os.environ.get("YANDEX_METRIKA_TOKEN", "")
        self.webmaster_token = os.environ.get("YANDEX_WEBMASTER_TOKEN", "")
        # Wordstat lives on Yandex Cloud Search API v2 and uses a service-account
        # Api-Key (NOT OAuth). Folder ID is optional: keys bound to a service
        # account in a folder work without it.
        self.wordstat_api_key = os.environ.get("YANDEX_WORDSTAT_API_KEY", "")
        self.cloud_folder_id = os.environ.get("YANDEX_CLOUD_FOLDER_ID", "")
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

    def _get_wordstat_api_key(self) -> str:
        """Get the Yandex Cloud Api-Key for the Wordstat (Search API v2)."""
        return self.wordstat_api_key

    def _get_webmaster_token(self) -> str:
        """Get token for Webmaster API."""
        return self.webmaster_token or self.unified_token

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
        use_v501: bool = False,
        timeout: Optional[float] = None
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

        req_timeout = timeout or DEFAULT_TIMEOUT
        async with httpx.AsyncClient(timeout=req_timeout) as client:
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

    async def wordstat_request(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a request to Wordstat via Yandex Cloud Search API v2.

        ``endpoint`` is the path after ``/v2/wordstat``, e.g. ``/topRequests``.
        Auth is an ``Api-Key`` header (service-account key), not OAuth.
        ``folderId`` is added to the body when YANDEX_CLOUD_FOLDER_ID is set;
        keys bound to a service account inside a folder work without it.
        """
        api_key = self._get_wordstat_api_key()
        if not api_key:
            raise ValueError(
                "Yandex Wordstat API key not configured. "
                "Set YANDEX_WORDSTAT_API_KEY environment variable "
                "(Yandex Cloud service-account API key with Search API access). "
                "The old OAuth-based api.wordstat.yandex.net is decommissioned."
            )

        from .config import YANDEX_WORDSTAT_API_URL
        url = f"{YANDEX_WORDSTAT_API_URL}{endpoint}"
        headers = {
            "Authorization": f"Api-Key {api_key}",
            "Content-Type": "application/json;charset=utf-8",
        }

        payload: Dict[str, Any] = dict(data or {})
        if self.cloud_folder_id and "folderId" not in payload:
            payload["folderId"] = self.cloud_folder_id

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()

    async def webmaster_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a GET request to Yandex Webmaster API v4.

        ``endpoint`` is the path after the API version, e.g.
        ``/user/{user_id}/hosts/``. List values in ``params`` are encoded as
        repeated query keys (required by the search-queries endpoints).
        """
        token = self._get_webmaster_token()
        if not token:
            raise ValueError(
                "Yandex Webmaster API token not configured. "
                "Set YANDEX_WEBMASTER_TOKEN or YANDEX_TOKEN environment variable."
            )

        from .config import YANDEX_WEBMASTER_API_URL
        url = f"{YANDEX_WEBMASTER_API_URL}{endpoint}"
        headers = {"Authorization": f"OAuth {token}"}

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    async def webmaster_user_id(self) -> int:
        """Resolve the Webmaster UserID of the token owner."""
        result = await self.webmaster_request("/user/")
        return result["user_id"]


# Global API client instance
api_client = YandexAPIClient()
