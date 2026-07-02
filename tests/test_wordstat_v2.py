"""Tests for the Wordstat migration to Yandex Cloud Search API v2."""

import httpx
import pytest

from yandex_mcp.client import YandexAPIClient
from yandex_mcp.config import YANDEX_WORDSTAT_API_URL
from yandex_mcp.tools.wordstat import (
    date_to_rfc3339,
    map_devices,
    map_dynamics_response,
    map_period,
    map_region_type,
    map_regions_response,
    map_top_requests_response,
)


class TestConfig:
    def test_wordstat_url_points_to_search_api_v2(self):
        assert YANDEX_WORDSTAT_API_URL == "https://searchapi.api.cloud.yandex.net/v2/wordstat"


class TestRequestMapping:
    def test_map_devices_v1_names(self):
        assert map_devices(["all", "desktop", "phone", "tablet"]) == [
            "DEVICE_ALL", "DEVICE_DESKTOP", "DEVICE_PHONE", "DEVICE_TABLET",
        ]

    def test_map_devices_v2_names_pass_through(self):
        assert map_devices(["DEVICE_PHONE"]) == ["DEVICE_PHONE"]

    def test_map_devices_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown device type"):
            map_devices(["smartwatch"])

    def test_map_period(self):
        assert map_period("monthly") == "PERIOD_MONTHLY"
        assert map_period("weekly") == "PERIOD_WEEKLY"
        assert map_period("daily") == "PERIOD_DAILY"
        assert map_period("PERIOD_DAILY") == "PERIOD_DAILY"

    def test_map_period_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown period"):
            map_period("yearly")

    def test_map_region_type(self):
        assert map_region_type("all") == "REGION_ALL"
        assert map_region_type("cities") == "REGION_CITIES"
        assert map_region_type("regions") == "REGION_REGIONS"
        assert map_region_type("REGION_CITIES") == "REGION_CITIES"

    def test_map_region_type_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown region_type"):
            map_region_type("countries")

    def test_date_to_rfc3339(self):
        assert date_to_rfc3339("2026-03-01") == "2026-03-01T00:00:00Z"


class TestResponseMapping:
    """v2 serializes int64 as JSON strings and omits zero-valued fields."""

    def test_top_requests_maps_to_v1_shape(self):
        v2 = {
            "results": [{"phrase": "авто из кореи", "count": "178254"}],
            "associations": [{"phrase": "кореана", "count": "73559"}],
            "totalCount": "178254",
        }
        mapped = map_top_requests_response("авто из кореи", v2)
        assert mapped == {
            "requestPhrase": "авто из кореи",
            "totalCount": 178254,
            "topRequests": [{"phrase": "авто из кореи", "count": 178254}],
            "associations": [{"phrase": "кореана", "count": 73559}],
        }

    def test_top_requests_omitted_zero_fields(self):
        # proto3 JSON omits zero values entirely
        mapped = map_top_requests_response("x", {"results": [{"phrase": "y"}]})
        assert mapped["totalCount"] == 0
        assert mapped["topRequests"] == [{"phrase": "y", "count": 0}]
        assert mapped["associations"] == []

    def test_dynamics_maps_to_v1_shape(self):
        v2 = {
            "results": [
                {"date": "2026-03-01T00:00:00Z", "count": "74766", "share": 0.00068},
            ]
        }
        mapped = map_dynamics_response(v2)
        assert mapped == {
            "dynamics": [{"date": "2026-03-01", "count": 74766, "share": 0.00068}]
        }

    def test_regions_maps_to_v1_shape(self):
        v2 = {
            "results": [
                {"region": "1", "count": "24680", "share": 0.00126, "affinityIndex": 73.6},
            ]
        }
        mapped = map_regions_response(v2)
        assert mapped == {
            "regions": [
                {"regionId": "1", "count": 24680, "share": 0.00126, "affinityIndex": 73.6}
            ]
        }

    def test_mapped_shapes_feed_existing_formatters(self):
        from yandex_mcp.formatters.wordstat import (
            format_wordstat_dynamics_markdown,
            format_wordstat_regions_markdown,
            format_wordstat_top_requests_markdown,
        )
        top = map_top_requests_response(
            "тест", {"results": [{"phrase": "тест", "count": "5"}], "totalCount": "5"}
        )
        assert "**тест**: 5" in format_wordstat_top_requests_markdown(top)

        dyn = map_dynamics_response(
            {"results": [{"date": "2026-03-01T00:00:00Z", "count": "7", "share": 0.5}]}
        )
        assert "2026-03-01" in format_wordstat_dynamics_markdown(dyn)

        reg = map_regions_response(
            {"results": [{"region": "213", "count": "9", "share": 0.1, "affinityIndex": 1.2}]}
        )
        assert "213" in format_wordstat_regions_markdown(reg)


class TestClient:
    async def test_missing_api_key_raises(self):
        client = YandexAPIClient()
        client.wordstat_api_key = ""
        with pytest.raises(ValueError, match="YANDEX_WORDSTAT_API_KEY"):
            await client.wordstat_request("/topRequests", {"phrase": "x"})

    async def test_api_key_header_and_folder_id(self, monkeypatch):
        captured = {}

        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"results": []}

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def post(self, url, json=None, headers=None):
                captured["url"] = url
                captured["json"] = json
                captured["headers"] = headers
                return FakeResponse()

        monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

        client = YandexAPIClient()
        client.wordstat_api_key = "test-api-key"
        client.cloud_folder_id = "test-folder"
        await client.wordstat_request("/topRequests", {"phrase": "x"})

        assert captured["url"] == (
            "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests"
        )
        assert captured["headers"]["Authorization"] == "Api-Key test-api-key"
        assert captured["json"] == {"phrase": "x", "folderId": "test-folder"}

    async def test_folder_id_not_injected_when_unset(self, monkeypatch):
        captured = {}

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {}

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def post(self, url, json=None, headers=None):
                captured["json"] = json
                return FakeResponse()

        monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

        client = YandexAPIClient()
        client.wordstat_api_key = "test-api-key"
        client.cloud_folder_id = ""
        await client.wordstat_request("/getRegionsTree")

        assert captured["json"] == {}
