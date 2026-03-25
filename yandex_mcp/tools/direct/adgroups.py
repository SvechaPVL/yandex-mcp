"""Yandex Direct ad group tools."""

import json
from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.common import ResponseFormat
from ...models.direct import (
    GetAdGroupsInput,
    CreateAdGroupInput,
    UpdateAdGroupInput,
)
from ...formatters.direct import format_adgroups_markdown
from ...utils import handle_api_error


def register(mcp: FastMCP) -> None:
    """Register ad group tools."""

    @mcp.tool(
        name="direct_get_adgroups",
        annotations={
            "title": "Get Yandex Direct Ad Groups",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def direct_get_adgroups(params: GetAdGroupsInput) -> str:
        """Get list of ad groups from Yandex Direct.

        Retrieves ad groups with their settings and targeting information.
        """
        try:
            selection_criteria = {}

            if params.campaign_ids:
                selection_criteria["CampaignIds"] = params.campaign_ids
            if params.adgroup_ids:
                selection_criteria["Ids"] = params.adgroup_ids

            request_params = {
                "SelectionCriteria": selection_criteria,
                "FieldNames": ["Id", "Name", "CampaignId", "RegionIds", "Type", "Status", "ServingStatus"],
                "UnifiedAdGroupFieldNames": ["OfferRetargeting"],
                "Page": {
                    "Limit": params.limit,
                    "Offset": params.offset
                }
            }

            result = await api_client.direct_request("adgroups", "get", request_params, use_v501=True)
            groups = result.get("result", {}).get("AdGroups", [])

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({"ad_groups": groups, "total": len(groups)}, indent=2, ensure_ascii=False)

            return format_adgroups_markdown(groups)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="direct_create_adgroup",
        annotations={
            "title": "Create Yandex Direct Ad Group",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False
        }
    )
    async def direct_create_adgroup(params: CreateAdGroupInput) -> str:
        """Create a new ad group in a campaign.

        Creates an ad group with specified name and targeting regions.
        """
        try:
            adgroup = {
                "Name": params.name,
                "CampaignId": params.campaign_id,
                "RegionIds": params.region_ids
            }

            if params.negative_keywords:
                adgroup["NegativeKeywords"] = {"Items": params.negative_keywords}

            if params.is_unified:
                # UnifiedAdGroup for UNIFIED_CAMPAIGN
                adgroup["UnifiedAdGroup"] = {
                    "OfferRetargeting": "NO"
                }
            elif params.feed_id:
                if params.is_smart:
                    # SmartAdGroup for SMART_CAMPAIGN
                    adgroup["SmartAdGroup"] = {
                        "FeedId": params.feed_id
                    }
                else:
                    # DynamicTextAdGroup for DYNAMIC_TEXT_CAMPAIGN
                    adgroup["DynamicTextAdGroup"] = {
                        "DomainUrl": "",
                        "FeedId": params.feed_id
                    }

            if params.autotargeting_categories:
                items = []
                for cat in params.autotargeting_categories:
                    items.append({"Category": cat, "Value": "YES"})
                adgroup["DynamicTextFeedAdGroup"] = {
                    "AutotargetingCategories": {"Items": items}
                }

            request_params = {
                "AdGroups": [adgroup]
            }

            # UnifiedAdGroup requires v501 API
            use_v501 = params.is_unified
            result = await api_client.direct_request("adgroups", "add", request_params, use_v501=use_v501)
            add_results = result.get("result", {}).get("AddResults", [])

            if add_results and add_results[0].get("Id"):
                return f"Ad group created successfully. ID: {add_results[0]['Id']}"

            errors = []
            for r in add_results:
                if r.get("Errors"):
                    errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])

            return f"Failed to create ad group:\n" + "\n".join(f"- {e}" for e in errors)

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="direct_update_adgroup",
        annotations={
            "title": "Update Yandex Direct Ad Group",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def direct_update_adgroup(params: UpdateAdGroupInput) -> str:
        """Update ad group settings.

        Allows updating ad group name, regions, negative keywords, and tracking params.
        Only specified fields will be updated.
        """
        try:
            adgroup_update = {"Id": params.adgroup_id}

            if params.name:
                adgroup_update["Name"] = params.name

            if params.region_ids:
                adgroup_update["RegionIds"] = params.region_ids

            if params.negative_keywords is not None:
                adgroup_update["NegativeKeywords"] = {"Items": params.negative_keywords}

            if params.tracking_params:
                adgroup_update["TrackingParams"] = params.tracking_params

            request_params = {
                "AdGroups": [adgroup_update]
            }

            result = await api_client.direct_request("adgroups", "update", request_params)
            update_results = result.get("result", {}).get("UpdateResults", [])

            errors = []
            for r in update_results:
                if r.get("Errors"):
                    errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
                if r.get("Warnings"):
                    errors.extend([f"Warning: {w.get('Message', 'Unknown warning')}" for w in r["Warnings"]])

            if errors:
                return f"Update completed with issues:\n" + "\n".join(f"- {e}" for e in errors)

            return f"Ad group {params.adgroup_id} updated successfully."

        except Exception as e:
            return handle_api_error(e)
