"""Yandex Direct campaign tools."""

import json
from mcp.server.fastmcp import FastMCP

from ...client import api_client
from ...models.common import ResponseFormat
from ...models.direct import (
    GetCampaignsInput,
    ManageCampaignInput,
    UpdateCampaignInput,
    CreateCampaignInput,
    CampaignType,
)
from ...formatters.direct import format_campaigns_markdown
from ...utils import handle_api_error
from ._helpers import register_manage_tool


def register(mcp: FastMCP) -> None:
    """Register campaign tools."""

    @mcp.tool(
        name="direct_get_campaigns",
        annotations={
            "title": "Get Yandex Direct Campaigns",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def direct_get_campaigns(params: GetCampaignsInput) -> str:
        """Get list of advertising campaigns from Yandex Direct.

        Retrieves campaigns with their settings, statistics, and current status.
        Supports filtering by IDs, states, statuses, and types.
        """
        try:
            selection_criteria = {}

            if params.campaign_ids:
                selection_criteria["Ids"] = params.campaign_ids
            if params.states:
                selection_criteria["States"] = [s.value for s in params.states]
            if params.statuses:
                selection_criteria["Statuses"] = [s.value for s in params.statuses]
            if params.types:
                selection_criteria["Types"] = [t.value for t in params.types]

            request_params = {
                "SelectionCriteria": selection_criteria,
                "FieldNames": [
                    "Id", "Name", "Type", "State", "Status", "StatusPayment",
                    "StartDate", "EndDate", "DailyBudget", "Statistics",
                    "NegativeKeywords"
                ],
                "TextCampaignFieldNames": ["BiddingStrategy", "Settings"],
                "DynamicTextCampaignFieldNames": ["BiddingStrategy", "Settings"],
                "SmartCampaignFieldNames": ["BiddingStrategy", "Settings", "AttributionModel"],
                "UnifiedCampaignFieldNames": ["BiddingStrategy", "Settings", "AttributionModel"],
                "Page": {
                    "Limit": params.limit,
                    "Offset": params.offset
                }
            }

            result = await api_client.direct_request("campaigns", "get", request_params, use_v501=True)
            campaigns = result.get("result", {}).get("Campaigns", [])

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({"campaigns": campaigns, "total": len(campaigns)}, indent=2, ensure_ascii=False)

            return format_campaigns_markdown(campaigns)

        except Exception as e:
            return handle_api_error(e)

    for action in ("suspend", "resume", "archive", "unarchive", "delete"):
        register_manage_tool(
            mcp,
            service="campaigns",
            action=action,
            entity="campaign",
            input_model=ManageCampaignInput,
            ids_field="campaign_ids",
        )

    @mcp.tool(
        name="direct_update_campaign",
        annotations={
            "title": "Update Yandex Direct Campaign",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    )
    async def direct_update_campaign(params: UpdateCampaignInput) -> str:
        """Update campaign settings.

        Allows updating campaign name, daily budget, dates, and negative keywords.
        Only specified fields will be updated.
        """
        try:
            campaign_update = {"Id": params.campaign_id}

            if params.name:
                campaign_update["Name"] = params.name

            if params.daily_budget_amount is not None:
                campaign_update["DailyBudget"] = {
                    "Amount": int(params.daily_budget_amount * 1_000_000),
                    "Mode": params.daily_budget_mode.value if params.daily_budget_mode else "DISTRIBUTED"
                }

            if params.start_date:
                campaign_update["StartDate"] = params.start_date

            if params.end_date:
                campaign_update["EndDate"] = params.end_date

            if params.negative_keywords is not None:
                campaign_update["NegativeKeywords"] = {"Items": params.negative_keywords}

            # Handle counter IDs
            if params.counter_ids is not None:
                if "TextCampaign" not in campaign_update:
                    campaign_update["TextCampaign"] = {}
                campaign_update["TextCampaign"]["CounterIds"] = {"Items": params.counter_ids}

            # Handle Settings (ADD_METRICA_TAG, etc.)
            settings = []
            if params.add_metrica_tag is not None:
                settings.append({"Option": "ADD_METRICA_TAG", "Value": "YES" if params.add_metrica_tag else "NO"})
            if params.enable_site_monitoring is not None:
                settings.append({"Option": "ENABLE_SITE_MONITORING", "Value": "YES" if params.enable_site_monitoring else "NO"})
            if params.enable_extended_ad_title is not None:
                settings.append({"Option": "ENABLE_EXTENDED_AD_TITLE", "Value": "YES" if params.enable_extended_ad_title else "NO"})
            if settings:
                if "TextCampaign" not in campaign_update:
                    campaign_update["TextCampaign"] = {}
                campaign_update["TextCampaign"]["Settings"] = settings

            # Handle priority goals
            if params.priority_goals:
                goals_items = []
                for g in params.priority_goals:
                    goals_items.append({
                        "GoalId": g["goal_id"],
                        "Value": int(g.get("value", 7000) * 1_000_000)
                    })
                if "TextCampaign" not in campaign_update:
                    campaign_update["TextCampaign"] = {}
                campaign_update["TextCampaign"]["PriorityGoals"] = {"Items": goals_items}
            elif params.goal_id:
                if "TextCampaign" not in campaign_update:
                    campaign_update["TextCampaign"] = {}
                campaign_update["TextCampaign"]["PriorityGoals"] = {
                    "Items": [{"GoalId": params.goal_id, "Value": 7_000_000_000}]
                }

            # Handle excluded sites (placements blacklist)
            if params.excluded_sites is not None:
                campaign_update["ExcludedSites"] = {"Items": params.excluded_sites}

            # Handle attribution model
            if params.attribution_model is not None:
                if "TextCampaign" not in campaign_update:
                    campaign_update["TextCampaign"] = {}
                campaign_update["TextCampaign"]["AttributionModel"] = params.attribution_model

            # Handle bidding strategy update
            if params.bidding_strategy_type is not None:
                strategy_type = params.bidding_strategy_type.value
                search_strategy = {"BiddingStrategyType": strategy_type}

                if strategy_type == "WB_MAXIMUM_CLICKS":
                    wb_params = {}
                    if params.weekly_spend_limit:
                        wb_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                    if params.bid_ceiling:
                        wb_params["BidCeiling"] = int(params.bid_ceiling * 1_000_000)
                    if wb_params:
                        search_strategy["WbMaximumClicks"] = wb_params

                elif strategy_type == "AVERAGE_CPC":
                    avg_cpc_params = {}
                    if params.average_cpc:
                        avg_cpc_params["AverageCpc"] = int(params.average_cpc * 1_000_000)
                    if params.weekly_spend_limit:
                        avg_cpc_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                    if avg_cpc_params:
                        search_strategy["AverageCpc"] = avg_cpc_params

                elif strategy_type == "WB_MAXIMUM_CONVERSION_RATE":
                    conv_params = {}
                    if params.weekly_spend_limit:
                        conv_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                    if params.bid_ceiling:
                        conv_params["BidCeiling"] = int(params.bid_ceiling * 1_000_000)
                    if params.goal_id:
                        conv_params["GoalId"] = params.goal_id
                    if conv_params:
                        search_strategy["WbMaximumConversionRate"] = conv_params

                campaign_update["TextCampaign"] = {
                    "BiddingStrategy": {
                        "Search": search_strategy,
                        "Network": {"BiddingStrategyType": "SERVING_OFF"}
                    }
                }

            # Detect if campaign uses UnifiedCampaign block and mirror settings
            has_unified = "UnifiedCampaign" in campaign_update
            has_text = "TextCampaign" in campaign_update

            # If TextCampaign fields were set, also duplicate them for UnifiedCampaign
            # so the update works regardless of campaign type.
            # The API ignores blocks that don't match the campaign type.
            if has_text and not has_unified:
                campaign_update["UnifiedCampaign"] = dict(campaign_update["TextCampaign"])
                campaign_update["DynamicTextCampaign"] = dict(campaign_update["TextCampaign"])

            request_params = {
                "Campaigns": [campaign_update]
            }

            # Use v501 to support all campaign types including UNIFIED_CAMPAIGN
            result = await api_client.direct_request("campaigns", "update", request_params, use_v501=True)
            update_results = result.get("result", {}).get("UpdateResults", [])

            errors = []
            for r in update_results:
                if r.get("Errors"):
                    errors.extend([e.get("Message", "Unknown error") for e in r["Errors"]])
                if r.get("Warnings"):
                    errors.extend([f"Warning: {w.get('Message', 'Unknown warning')}" for w in r["Warnings"]])

            if errors:
                return f"Update completed with issues:\n" + "\n".join(f"- {e}" for e in errors)

            return f"Campaign {params.campaign_id} updated successfully."

        except Exception as e:
            return handle_api_error(e)

    @mcp.tool(
        name="direct_create_campaign",
        annotations={
            "title": "Create Yandex Direct Campaign",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False
        }
    )
    async def direct_create_campaign(params: CreateCampaignInput) -> str:
        """Create a new advertising campaign.

        Creates TEXT_CAMPAIGN or CPM_BANNER_CAMPAIGN with specified settings.
        For РСЯ campaigns, use CPM_BANNER_CAMPAIGN type and network_strategy_type.
        """
        try:
            campaign = {
                "Name": params.name,
                "StartDate": params.start_date
            }

            if params.end_date:
                campaign["EndDate"] = params.end_date

            if params.daily_budget_amount:
                campaign["DailyBudget"] = {
                    "Amount": int(params.daily_budget_amount * 1_000_000),
                    "Mode": params.daily_budget_mode.value
                }

            if params.negative_keywords:
                campaign["NegativeKeywords"] = {"Items": params.negative_keywords}

            # Build bidding strategy
            search_strategy = {"BiddingStrategyType": params.search_strategy_type.value}
            network_strategy = {"BiddingStrategyType": params.network_strategy_type.value}

            if params.search_strategy_type.value == "WB_MAXIMUM_CLICKS":
                wb_params = {}
                if params.weekly_spend_limit:
                    wb_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                if params.bid_ceiling:
                    wb_params["BidCeiling"] = int(params.bid_ceiling * 1_000_000)
                if wb_params:
                    search_strategy["WbMaximumClicks"] = wb_params

            # Handle network strategy for РСЯ / Smart
            if params.network_strategy_type.value == "NETWORK_DEFAULT":
                network_strategy["NetworkDefault"] = {}
            elif params.network_strategy_type.value == "WB_MAXIMUM_CLICKS":
                wb_params = {}
                if params.weekly_spend_limit:
                    wb_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                if params.bid_ceiling:
                    wb_params["BidCeiling"] = int(params.bid_ceiling * 1_000_000)
                if wb_params:
                    network_strategy["WbMaximumClicks"] = wb_params
            elif params.network_strategy_type.value == "AVERAGE_CPC_PER_CAMPAIGN":
                avg_params = {}
                if params.bid_ceiling:
                    avg_params["AverageCpc"] = int(params.bid_ceiling * 1_000_000)
                if params.weekly_spend_limit:
                    avg_params["WeeklySpendLimit"] = int(params.weekly_spend_limit * 1_000_000)
                if avg_params:
                    network_strategy["AverageCpcPerCampaign"] = avg_params

            # Campaign type specific settings
            if params.campaign_type == CampaignType.TEXT_CAMPAIGN:
                text_campaign = {
                    "BiddingStrategy": {
                        "Search": search_strategy,
                        "Network": network_strategy
                    }
                }

                if params.counter_ids:
                    text_campaign["CounterIds"] = {"Items": params.counter_ids}

                if params.goal_id:
                    text_campaign["PriorityGoals"] = {
                        "Items": [{
                            "GoalId": params.goal_id,
                            "Value": int(params.goal_value * 1_000_000) if params.goal_value else 7_000_000_000
                        }]
                    }

                campaign["TextCampaign"] = text_campaign

            elif params.campaign_type == CampaignType.DYNAMIC_TEXT_CAMPAIGN:
                dynamic_campaign = {
                    "BiddingStrategy": {
                        "Search": search_strategy,
                        "Network": network_strategy
                    },
                    "Settings": [
                        {"Option": "ADD_METRICA_TAG", "Value": "YES"},
                        {"Option": "ADD_OPENSTAT_TAG", "Value": "NO"},
                        {"Option": "ADD_TO_FAVORITES", "Value": "NO"},
                        {"Option": "ENABLE_AREA_OF_INTEREST_TARGETING", "Value": "YES"},
                        {"Option": "ENABLE_SITE_MONITORING", "Value": "YES"},
                        {"Option": "REQUIRE_SERVICING", "Value": "NO"},
                    ]
                }

                if params.counter_ids:
                    dynamic_campaign["CounterIds"] = {"Items": params.counter_ids}

                if params.goal_id:
                    dynamic_campaign["PriorityGoals"] = {
                        "Items": [{
                            "GoalId": params.goal_id,
                            "Value": int(params.goal_value * 1_000_000) if params.goal_value else 7_000_000_000
                        }]
                    }

                campaign["DynamicTextCampaign"] = dynamic_campaign

            elif params.campaign_type == CampaignType.SMART_CAMPAIGN:
                # Smart banners - network only (search always OFF)
                smart_campaign = {
                    "BiddingStrategy": {
                        "Search": {"BiddingStrategyType": "SERVING_OFF"},
                        "Network": network_strategy
                    }
                }

                # SmartCampaign uses CounterId (singular long), not CounterIds with Items
                if params.counter_ids:
                    smart_campaign["CounterId"] = params.counter_ids[0]

                if params.goal_id:
                    smart_campaign["PriorityGoals"] = {
                        "Items": [{
                            "GoalId": params.goal_id,
                            "Value": int(params.goal_value * 1_000_000) if params.goal_value else 7_000_000_000
                        }]
                    }

                smart_campaign["AttributionModel"] = "LC"

                campaign["SmartCampaign"] = smart_campaign

            elif params.campaign_type == CampaignType.UNIFIED_CAMPAIGN:
                # ЕПК (Единая Перфоманс-Кампания) - requires v501 API
                unified_campaign = {
                    "BiddingStrategy": {
                        "Search": search_strategy,
                        "Network": network_strategy
                    }
                }

                if params.counter_ids:
                    unified_campaign["CounterIds"] = {"Items": params.counter_ids}

                if params.goal_id:
                    unified_campaign["PriorityGoals"] = {
                        "Items": [{
                            "GoalId": params.goal_id,
                            "Value": int(params.goal_value * 1_000_000) if params.goal_value else 7_000_000_000,
                            "IsMetrikaSourceOfValue": "NO"
                        }]
                    }

                unified_campaign["Settings"] = [
                    {"Option": "ADD_METRICA_TAG", "Value": "YES"},
                    {"Option": "ENABLE_AREA_OF_INTEREST_TARGETING", "Value": "YES"},
                    {"Option": "ENABLE_SITE_MONITORING", "Value": "YES"},
                ]

                unified_campaign["AttributionModel"] = "LYDC"

                campaign["UnifiedCampaign"] = unified_campaign

            elif params.campaign_type == CampaignType.CPM_BANNER_CAMPAIGN:
                cpm_campaign = {
                    "BiddingStrategy": {
                        "Search": {"BiddingStrategyType": "SERVING_OFF"},
                        "Network": network_strategy
                    }
                }

                if params.counter_ids:
                    cpm_campaign["CounterIds"] = {"Items": params.counter_ids}

                campaign["CpmBannerCampaign"] = cpm_campaign

            request_params = {"Campaigns": [campaign]}

            # UnifiedCampaign requires v501 API endpoint
            use_v501 = params.campaign_type == CampaignType.UNIFIED_CAMPAIGN
            result = await api_client.direct_request("campaigns", "add", request_params, use_v501=use_v501)
            add_results = result.get("result", {}).get("AddResults", [])

            if add_results and add_results[0].get("Id"):
                return f"Campaign created successfully. ID: {add_results[0]['Id']}"

            errors = []
            for r in add_results:
                if r.get("Errors"):
                    errors.extend([f"{e.get('Code', '?')}: {e.get('Message', 'Unknown')} | {e.get('Details', '')}" for e in r["Errors"]])
                if r.get("Warnings"):
                    errors.extend([f"Warning {w.get('Code', '?')}: {w.get('Message', '')} | {w.get('Details', '')}" for w in r["Warnings"]])

            if errors:
                import json as _json
                req_dump = _json.dumps(request_params, ensure_ascii=False, indent=2)
                return f"Failed to create campaign:\n" + "\n".join(f"- {e}" for e in errors) + f"\n\nRequest sent:\n```json\n{req_dump}\n```"

            return f"Failed to create campaign. Full response: {result}"

        except Exception as e:
            return handle_api_error(e)
