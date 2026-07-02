"""Tests for the Yandex Webmaster API v4 write tools (hosts, verification,
user-added sitemaps, recrawl).
"""

import httpx
import pydantic
import pytest

from yandex_mcp.client import YandexAPIClient
from yandex_mcp.models.webmaster import (
    WebmasterAddHostInput,
    WebmasterAddSitemapInput,
    WebmasterDeleteHostInput,
    WebmasterDeleteSitemapInput,
    WebmasterListSitemapsInput,
    WebmasterRecrawlQuotaInput,
    WebmasterRecrawlUrlInput,
    WebmasterVerifyHostInput,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content=b"{}"):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://api.webmaster.yandex.net/v4/x")
            response = httpx.Response(self.status_code, request=request, json=self._payload)
            raise httpx.HTTPStatusError("error", request=request, response=response)

    def json(self):
        return self._payload


class FakeAsyncClient:
    """Records the last request made through httpx.AsyncClient."""

    last_call = {}

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        FakeAsyncClient.last_call = {"method": "GET", "url": url, "params": params, "headers": headers}
        return FakeAsyncClient.response

    async def post(self, url, params=None, json=None, headers=None):
        FakeAsyncClient.last_call = {
            "method": "POST", "url": url, "params": params, "json": json, "headers": headers,
        }
        return FakeAsyncClient.response

    async def delete(self, url, params=None, headers=None):
        FakeAsyncClient.last_call = {"method": "DELETE", "url": url, "params": params, "headers": headers}
        return FakeAsyncClient.response


@pytest.fixture
def fake_client(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    return FakeAsyncClient


class TestModelValidation:
    """host_id / sitemap_id are interpolated into the API path; host_url /
    sitemap_url / url are sent as JSON body values. Both need validation.
    """

    def test_add_host_accepts_valid_url(self):
        assert WebmasterAddHostInput(host_url="https://asiapk.ru").host_url == "https://asiapk.ru"

    @pytest.mark.parametrize("bad", ["asiapk.ru", "ftp://asiapk.ru", "", "javascript:alert(1)"])
    def test_add_host_rejects_invalid_url(self, bad):
        with pytest.raises(pydantic.ValidationError):
            WebmasterAddHostInput(host_url=bad)

    def test_verify_host_rejects_unknown_verification_type(self):
        with pytest.raises(pydantic.ValidationError):
            WebmasterVerifyHostInput(host_id="https:asiapk.ru:443", verification_type="WHOIS")

    @pytest.mark.parametrize("vtype", ["DNS", "HTML_FILE", "META_TAG"])
    def test_verify_host_accepts_known_verification_types(self, vtype):
        model = WebmasterVerifyHostInput(host_id="https:asiapk.ru:443", verification_type=vtype)
        assert model.verification_type == vtype

    def test_delete_host_rejects_path_traversal_host_id(self):
        with pytest.raises(pydantic.ValidationError):
            WebmasterDeleteHostInput(host_id="https:asiapk.ru:443/../../hosts")

    @pytest.mark.parametrize("bad", ["../etc/passwd", "abc/def", ""])
    def test_delete_sitemap_rejects_bad_sitemap_id(self, bad):
        with pytest.raises(pydantic.ValidationError):
            WebmasterDeleteSitemapInput(host_id="https:asiapk.ru:443", sitemap_id=bad)

    def test_delete_sitemap_accepts_opaque_id(self):
        model = WebmasterDeleteSitemapInput(host_id="https:asiapk.ru:443", sitemap_id="12345")
        assert model.sitemap_id == "12345"

    def test_add_sitemap_rejects_invalid_sitemap_url(self):
        with pytest.raises(pydantic.ValidationError):
            WebmasterAddSitemapInput(host_id="https:asiapk.ru:443", sitemap_url="not-a-url")

    def test_recrawl_url_rejects_invalid_url(self):
        with pytest.raises(pydantic.ValidationError):
            WebmasterRecrawlUrlInput(host_id="https:asiapk.ru:443", url="not-a-url")


class TestClientRequestMapping:
    """The generic client.webmaster_request() must route to the right HTTP
    verb and normalize empty/204 bodies to a dict.
    """

    async def test_missing_token_raises(self):
        client = YandexAPIClient()
        client.webmaster_token = ""
        client.unified_token = ""
        with pytest.raises(ValueError, match="YANDEX_WEBMASTER_TOKEN"):
            await client.webmaster_request("/user/")

    async def test_get_is_default_method(self, fake_client):
        fake_client.response = FakeResponse(payload={"user_id": 1})
        client = YandexAPIClient()
        client.webmaster_token = "test-token"
        result = await client.webmaster_request("/user/")
        assert fake_client.last_call["method"] == "GET"
        assert result == {"user_id": 1}

    async def test_post_sends_json_body_and_oauth_header(self, fake_client):
        fake_client.response = FakeResponse(status_code=201, payload={"host_id": "https:asiapk.ru:443"})
        client = YandexAPIClient()
        client.webmaster_token = "test-token"
        result = await client.webmaster_request(
            "/user/1/hosts/", method="POST", json_body={"host_url": "https://asiapk.ru"}
        )
        assert fake_client.last_call["method"] == "POST"
        assert fake_client.last_call["json"] == {"host_url": "https://asiapk.ru"}
        assert fake_client.last_call["headers"]["Authorization"] == "OAuth test-token"
        assert result == {"host_id": "https:asiapk.ru:443"}

    async def test_delete_with_empty_204_body_normalizes_to_success(self, fake_client):
        fake_client.response = FakeResponse(status_code=204, content=b"")
        client = YandexAPIClient()
        client.webmaster_token = "test-token"
        result = await client.webmaster_request("/user/1/hosts/https:asiapk.ru:443/", method="DELETE")
        assert fake_client.last_call["method"] == "DELETE"
        assert result == {"success": True}

    async def test_unsupported_method_raises(self):
        client = YandexAPIClient()
        client.webmaster_token = "test-token"
        with pytest.raises(ValueError, match="Unsupported HTTP method"):
            await client.webmaster_request("/user/", method="PATCH")

    async def test_verification_post_sends_verification_type_as_query_param(self, fake_client):
        fake_client.response = FakeResponse(
            payload={"verification_uin": "abc", "verification_state": "IN_PROGRESS"}
        )
        client = YandexAPIClient()
        client.webmaster_token = "test-token"
        await client.webmaster_request(
            "/user/1/hosts/https:asiapk.ru:443/verification/",
            method="POST",
            params={"verification_type": "DNS"},
        )
        assert fake_client.last_call["params"] == {"verification_type": "DNS"}
        assert fake_client.last_call["method"] == "POST"


class TestFormatters:
    def test_verification_markdown_includes_uin_and_state(self):
        from yandex_mcp.formatters.webmaster import format_webmaster_verification_markdown

        md = format_webmaster_verification_markdown(
            {
                "verification_uin": "yandex_abc123",
                "verification_state": "IN_PROGRESS",
                "verification_type": "DNS",
                "applicable_verifiers": ["DNS", "HTML_FILE", "META_TAG"],
            }
        )
        assert "yandex_abc123" in md
        assert "IN_PROGRESS" in md
        assert "DNS" in md

    def test_sitemaps_markdown_lists_each_sitemap(self):
        from yandex_mcp.formatters.webmaster import format_webmaster_sitemaps_markdown

        md = format_webmaster_sitemaps_markdown(
            {
                "sitemaps": [
                    {"sitemap_id": "1", "sitemap_url": "https://asiapk.ru/sitemap.xml", "added_date": "2026-01-01"}
                ],
                "count": 1,
            }
        )
        assert "sitemap.xml" in md
        assert "1" in md

    def test_sitemaps_markdown_empty(self):
        from yandex_mcp.formatters.webmaster import format_webmaster_sitemaps_markdown

        assert "No user-added sitemaps" in format_webmaster_sitemaps_markdown({"sitemaps": []})

    def test_recrawl_quota_markdown(self):
        from yandex_mcp.formatters.webmaster import format_webmaster_recrawl_quota_markdown

        md = format_webmaster_recrawl_quota_markdown({"daily_quota": 100, "quota_remainder": 97})
        assert "100" in md
        assert "97" in md


class TestToolRegistration:
    def test_list_sitemaps_input_defaults(self):
        model = WebmasterListSitemapsInput(host_id="https:asiapk.ru:443")
        assert model.limit == 100
        assert model.offset == "0"

    def test_recrawl_quota_input_requires_host_id(self):
        with pytest.raises(pydantic.ValidationError):
            WebmasterRecrawlQuotaInput()
