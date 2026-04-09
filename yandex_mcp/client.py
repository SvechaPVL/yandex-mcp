"""Unified API client for Yandex Direct and Metrika APIs."""

import os
from typing import Any, Dict, Optional

import httpx

from .config import (
    DEFAULT_TIMEOUT,
    YANDEX_APPMETRICA_API_URL,
    YANDEX_APPMETRICA_PUSH_API_URL,
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
        # Allow single token for both services
        self.unified_token = os.environ.get("YANDEX_TOKEN", "")
        self.client_login = os.environ.get("YANDEX_CLIENT_LOGIN", "")
        self.appmetrica_token = os.environ.get("YANDEX_APPMETRICA_TOKEN", "")
        self.use_sandbox = os.environ.get("YANDEX_USE_SANDBOX", "false").lower() == "true"

    def _get_appmetrica_token(self) -> str:
        """Get token for AppMetrica API."""
        return self.appmetrica_token or self.unified_token

    def _get_direct_token(self) -> str:
        """Get token for Direct API."""
        return self.direct_token or self.unified_token

    def _get_metrika_token(self) -> str:
        """Get token for Metrika API."""
        return self.metrika_token or self.unified_token

    def _get_wordstat_token(self) -> str:
        """Get token for Wordstat API."""
        return self.direct_token or self.unified_token

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
        """Make a request to Yandex Wordstat API."""
        token = self._get_wordstat_token()
        if not token:
            raise ValueError(
                "Yandex Wordstat API token not configured. "
                "Set YANDEX_DIRECT_TOKEN or YANDEX_TOKEN environment variable."
            )

        from .config import YANDEX_WORDSTAT_API_URL
        url = f"{YANDEX_WORDSTAT_API_URL}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.post(url, json=data or {}, headers=headers)
            response.raise_for_status()
            return response.json()

    async def appmetrica_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        use_push_api: bool = False,
    ) -> Dict[str, Any]:
        """Make a request to Yandex AppMetrica API."""
        token = self._get_appmetrica_token()
        if not token:
            raise ValueError(
                "Yandex AppMetrica API token not configured. "
                "Set YANDEX_APPMETRICA_TOKEN or YANDEX_TOKEN environment variable."
            )

        base_url = YANDEX_APPMETRICA_PUSH_API_URL if use_push_api else YANDEX_APPMETRICA_API_URL
        url = f"{base_url}{endpoint}"
        headers = {
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json",
        }

        req_timeout = timeout or DEFAULT_TIMEOUT
        async with httpx.AsyncClient(timeout=req_timeout) as client:
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
