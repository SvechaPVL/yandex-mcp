"""Utility functions for Yandex MCP Server."""

import httpx


def handle_api_error(e: Exception) -> str:
    """Format API errors into actionable messages."""
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        try:
            error_body = e.response.json()
            # Yandex Direct format: {"error": {"error_string": ..., "error_detail": ...}}
            error_obj = error_body.get("error", {})
            if isinstance(error_obj, dict) and error_obj.get("error_string"):
                error_msg = error_obj["error_string"]
                error_detail = error_obj.get("error_detail", "")
                return f"API Error ({status}): {error_msg}. {error_detail}".strip()
            # AppMetrica format: {"errors": [{"error_type": ..., "message": ...}], "message": ...}
            if "errors" in error_body:
                messages = [err.get("message", "") for err in error_body["errors"]]
                return f"API Error ({status}): {'; '.join(messages)}"
            # Fallback: top-level message
            if "message" in error_body:
                return f"API Error ({status}): {error_body['message']}"
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
