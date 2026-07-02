"""Utility functions for Yandex MCP Server."""

import httpx


def handle_api_error(e: Exception) -> str:
    """Format API errors into actionable messages."""
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        try:
            error_body = e.response.json()
            error_msg = error_body.get("error", {}).get("error_string", "")
            error_detail = error_body.get("error", {}).get("error_detail", "")
            if error_msg:
                return f"API Error ({status}): {error_msg}. {error_detail}".strip()
            # Yandex Cloud (Search API v2 / Wordstat) error shape: {"code", "message"}
            cloud_msg = error_body.get("message", "")
            if cloud_msg:
                return f"API Error ({status}): {cloud_msg}"
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
