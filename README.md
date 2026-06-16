# Yandex MCP Server

[🇷🇺 Русская версия](README.ru.md)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io/)

MCP (Model Context Protocol) server for **Yandex Direct**, **Yandex Metrika**, and **Yandex Wordstat** APIs. Provides **128 tools** for managing advertising campaigns, analytics, keyword research, and reporting through any MCP-compatible client.

> Manage Yandex advertising and analytics through AI

## Features

### Yandex Direct API v5 (80 tools)
- **Campaigns** — create, update, pause, resume, archive, delete
- **Ad Groups** — create, update with targeting settings
- **Ads** — text, image, dynamic, shopping ads with moderation
- **Keywords** — manage keywords and bids
- **Statistics** — detailed performance reports with async retry
- **Bid Modifiers** — mobile, desktop, demographics, regional adjustments
- **Retargeting** — retargeting lists and audience targets
- **Smart Ad Targets** — feed-based targeting filters
- **Sitelinks, VCards, Callouts** — ad extensions
- **Images** — upload, manage, and delete ad images
- **Feeds** — product feed management
- **Videos & Creatives** — video ad creation
- **Dictionaries** — regions, interests, categories
- **Negative Keywords** — shared negative keyword sets

### Yandex Wordstat API (5 tools)
- **Top Requests** — popular search queries and associations
- **Dynamics** — query frequency trends over time
- **Regions** — regional distribution of search queries
- **Regions Tree** — hierarchical region structure
- **User Info** — API quota and usage limits

### Yandex Metrika API (43 tools)
- **Counters** — create, configure, delete tracking counters
- **Goals** — conversion goal management
- **Reports** — analytics with custom metrics, time series, comparisons, drill-down
- **Segments** — audience segmentation
- **Filters** — traffic filtering rules
- **Grants** — access permission management
- **Offline Data** — upload conversions, calls, expenses, user parameters
- **Labels & Annotations** — organize counters and mark chart events
- **Delegates** — account delegation

## Quick Start

### 1. Install

```bash
git clone https://github.com/SvechaPVL/yandex-mcp.git
cd yandex-mcp
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

### 2. Set up your Yandex API token

Create a `.env` file:

```env
YANDEX_TOKEN=your_oauth_token_here
```

Get a token from [Yandex OAuth](https://oauth.yandex.ru/) with permissions for Direct and Metrika APIs (`direct:api`, `metrika:read`, `metrika:write`).

### 3. Configure your MCP client

Add to your MCP client settings:

```json
{
  "mcpServers": {
    "yandex": {
      "command": "python",
      "args": ["-m", "yandex_mcp"],
      "cwd": "/path/to/yandex-mcp",
      "env": {
        "YANDEX_TOKEN": "your_token"
      }
    }
  }
}
```

### 4. Done!

```
> Show all my campaigns in Direct
> Pause campaign 12345
> What are the site stats for the last week?
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `YANDEX_TOKEN` | Yes | Yandex OAuth token (used for both Direct and Metrika) |
| `YANDEX_DIRECT_TOKEN` | No | Separate token for Direct API |
| `YANDEX_METRIKA_TOKEN` | No | Separate token for Metrika API |
| `YANDEX_CLIENT_LOGIN` | No | Client login for agency accounts |
| `YANDEX_USE_SANDBOX` | No | Set to `true` for sandbox API |

## Tools (128)

### Yandex Direct (80 tools)

#### Campaigns (8)

| Tool | Description |
|------|-------------|
| `direct_get_campaigns` | Get campaigns with status, strategy, and budget info |
| `direct_create_campaign` | Create a new campaign (search, network, or both) |
| `direct_update_campaign` | Update campaign settings (strategy, budget, schedule, regions, attribution model) |
| `direct_suspend_campaigns` | Pause campaigns |
| `direct_resume_campaigns` | Resume paused campaigns |
| `direct_archive_campaigns` | Archive campaigns |
| `direct_unarchive_campaigns` | Restore archived campaigns |
| `direct_delete_campaigns` | Delete campaigns permanently |

#### Ad Groups (3)

| Tool | Description |
|------|-------------|
| `direct_get_adgroups` | Get ad groups with targeting settings |
| `direct_create_adgroup` | Create a new ad group in a campaign |
| `direct_update_adgroup` | Update ad group settings and targeting |

#### Ads (12)

| Tool | Description |
|------|-------------|
| `direct_get_ads` | Get ads with content and moderation status |
| `direct_create_text_ad` | Create a text ad with title, text, and URL |
| `direct_create_image_ad` | Create an image ad (banner) |
| `direct_create_dynamic_ad` | Create a dynamic text ad from feed data |
| `direct_create_shopping_ad` | Create a shopping ad (unified campaign format) |
| `direct_update_ad` | Update ad content (triggers re-moderation) |
| `direct_moderate_ads` | Submit ads for moderation |
| `direct_suspend_ads` | Pause ads |
| `direct_resume_ads` | Resume paused ads |
| `direct_archive_ads` | Archive ads |
| `direct_unarchive_ads` | Restore archived ads |
| `direct_delete_ads` | Delete ads permanently |

#### Keywords (6)

| Tool | Description |
|------|-------------|
| `direct_get_keywords` | Get keywords with bids and status |
| `direct_add_keywords` | Add keywords to an ad group |
| `direct_set_keyword_bids` | Set search and network bids |
| `direct_suspend_keywords` | Pause keywords |
| `direct_resume_keywords` | Resume paused keywords |
| `direct_delete_keywords` | Delete keywords permanently |

#### Statistics (1)

| Tool | Description |
|------|-------------|
| `direct_get_statistics` | Get performance reports (impressions, clicks, cost, CTR) |

#### Bid Modifiers (5)

| Tool | Description |
|------|-------------|
| `direct_get_bid_modifiers` | Get bid modifiers (mobile, desktop, demographics, regional) |
| `direct_add_bid_modifier` | Add a bid modifier to campaign or ad group |
| `direct_set_bid_modifier` | Set bid modifier value (0-1300%) |
| `direct_delete_bid_modifiers` | Delete bid modifiers |
| `direct_toggle_bid_modifiers` | Enable or disable bid modifiers |

#### Retargeting & Audience Targets (9)

| Tool | Description |
|------|-------------|
| `direct_get_retargeting_lists` | Get retargeting lists based on Metrika goals |
| `direct_add_retargeting_list` | Create a retargeting list with goal rules |
| `direct_update_retargeting_list` | Update retargeting list rules |
| `direct_delete_retargeting_lists` | Delete retargeting lists |
| `direct_get_audience_targets` | Get audience targets linking lists to ad groups |
| `direct_add_audience_target` | Add an audience target to an ad group |
| `direct_suspend_audience_targets` | Pause audience targets |
| `direct_resume_audience_targets` | Resume audience targets |
| `direct_delete_audience_targets` | Delete audience targets |

#### Smart Ad Targets (5)

| Tool | Description |
|------|-------------|
| `direct_get_smart_ad_targets` | Get smart ad target filters and conditions |
| `direct_add_smart_ad_target` | Add a targeting filter to a smart ad group |
| `direct_suspend_smart_ad_targets` | Pause smart ad targets |
| `direct_resume_smart_ad_targets` | Resume smart ad targets |
| `direct_delete_smart_ad_targets` | Delete smart ad targets |

#### Sitelinks (3)

| Tool | Description |
|------|-------------|
| `direct_get_sitelinks` | Get sitelink sets |
| `direct_add_sitelinks` | Create a sitelink set (up to 8 links) |
| `direct_delete_sitelinks` | Delete sitelink sets |

#### VCards (3)

| Tool | Description |
|------|-------------|
| `direct_get_vcards` | Get business card info (address, phone, hours) |
| `direct_add_vcard` | Create a business card for ads |
| `direct_delete_vcards` | Delete business cards |

#### Negative Keyword Sets (4)

| Tool | Description |
|------|-------------|
| `direct_get_negative_keyword_shared_sets` | Get shared negative keyword sets |
| `direct_add_negative_keyword_shared_set` | Create a shared negative keyword set |
| `direct_update_negative_keyword_shared_set` | Update a negative keyword set |
| `direct_delete_negative_keyword_shared_sets` | Delete negative keyword sets |

#### Ad Extensions (3)

| Tool | Description |
|------|-------------|
| `direct_get_adextensions` | Get ad extensions (callouts, etc.) |
| `direct_add_callouts` | Add callout extensions |
| `direct_link_callouts_to_ad` | Link callouts to an ad |

#### Videos & Creatives (4)

| Tool | Description |
|------|-------------|
| `direct_upload_video` | Upload a video for ad extensions |
| `direct_get_advideos` | Get uploaded ad videos |
| `direct_create_video_creative` | Create a video creative from uploaded video |
| `direct_get_creatives` | Get video creatives |

#### Feeds (4)

| Tool | Description |
|------|-------------|
| `direct_get_feeds` | Get product feeds |
| `direct_add_feed` | Add a product feed (URL or file) |
| `direct_update_feed` | Update feed settings |
| `direct_delete_feeds` | Delete feeds |

#### Images (3)

| Tool | Description |
|------|-------------|
| `direct_upload_image` | Upload a base64-encoded image (JPEG, GIF, PNG) |
| `direct_get_images` | Get image metadata, hashes, and association status |
| `direct_delete_images` | Delete unassociated images by hash |

#### Dictionaries & Regions (3)

| Tool | Description |
|------|-------------|
| `direct_get_dictionaries` | Get Direct dictionaries (ad categories, interests) |
| `direct_get_regions` | Get geographic region tree |
| `direct_get_interests` | Get interest categories for targeting |

#### Client & Changes (4)

| Tool | Description |
|------|-------------|
| `direct_get_client_info` | Get account info and settings |
| `direct_check_campaign_changes` | Check for changes in specific campaigns |
| `direct_check_all_changes` | Check for any changes in the account |
| `direct_get_recent_changes_timestamp` | Get timestamp of most recent changes |

### Yandex Metrika (43 tools)

#### Counters (5)

| Tool | Description |
|------|-------------|
| `metrika_get_counters` | List all Metrika counters |
| `metrika_get_counter` | Get detailed counter info (code status, webvisor, goals) |
| `metrika_create_counter` | Create a new counter with tracking code |
| `metrika_update_counter` | Update counter name, site, or favorite status |
| `metrika_delete_counter` | Delete a counter and all its data |

#### Goals (4)

| Tool | Description |
|------|-------------|
| `metrika_get_goals` | Get conversion goals for a counter |
| `metrika_create_goal` | Create a goal (URL, event, composite, etc.) |
| `metrika_update_goal` | Update goal conditions |
| `metrika_delete_goal` | Delete a goal |

#### Reports (4)

| Tool | Description |
|------|-------------|
| `metrika_get_report` | Get analytics report with custom metrics and dimensions |
| `metrika_get_report_by_time` | Get time-series report (daily, weekly, monthly) |
| `metrika_get_comparison_report` | Compare two date ranges |
| `metrika_get_drilldown_report` | Get hierarchical drill-down report |

#### Segments (4)

| Tool | Description |
|------|-------------|
| `metrika_get_segments` | Get audience segments |
| `metrika_create_segment` | Create a segment with filter expression |
| `metrika_update_segment` | Update segment definition |
| `metrika_delete_segment` | Delete a segment |

#### Filters (4)

| Tool | Description |
|------|-------------|
| `metrika_get_filters` | Get data filters for a counter |
| `metrika_create_filter` | Create a filter (include/exclude traffic) |
| `metrika_update_filter` | Update filter conditions |
| `metrika_delete_filter` | Delete a filter |

#### Grants (4)

| Tool | Description |
|------|-------------|
| `metrika_get_grants` | Get access permissions for a counter |
| `metrika_add_grant` | Grant access to another user |
| `metrika_update_grant` | Update grant permissions |
| `metrika_delete_grant` | Revoke access |

#### Offline Data (5)

| Tool | Description |
|------|-------------|
| `metrika_upload_offline_conversions` | Upload offline conversion data |
| `metrika_get_offline_conversions_status` | Check upload processing status |
| `metrika_upload_calls` | Upload call tracking data |
| `metrika_upload_expenses` | Upload advertising expense data |
| `metrika_upload_user_parameters` | Upload custom user parameters |

#### Labels (6)

| Tool | Description |
|------|-------------|
| `metrika_get_labels` | Get labels for organizing counters |
| `metrika_create_label` | Create a label |
| `metrika_update_label` | Rename a label |
| `metrika_delete_label` | Delete a label |
| `metrika_link_counter_to_label` | Link a counter to a label |
| `metrika_unlink_counter_from_label` | Unlink a counter from a label |

#### Annotations (4)

| Tool | Description |
|------|-------------|
| `metrika_get_annotations` | Get chart annotations for a counter |
| `metrika_create_annotation` | Create an annotation (mark events on charts) |
| `metrika_update_annotation` | Update annotation text |
| `metrika_delete_annotation` | Delete an annotation |

#### Delegates (3)

| Tool | Description |
|------|-------------|
| `metrika_get_delegates` | Get list of account delegates |
| `metrika_add_delegate` | Add a delegate with counter access |
| `metrika_delete_delegate` | Remove a delegate |

### Yandex Wordstat (5 tools)

| Tool | Description |
|------|-------------|
| `wordstat_top_requests` | Get popular search queries and associations for phrases |
| `wordstat_dynamics` | Get query frequency dynamics over time (daily/weekly/monthly) |
| `wordstat_regions` | Get regional distribution of search queries |
| `wordstat_regions_tree` | Get full hierarchical regions tree with IDs |
| `wordstat_user_info` | Get API quota and usage limits |

## Usage Examples

### Campaign management
```
Show all active campaigns
Pause campaign "Summer sale"
Set weekly budget of campaign 123 to 50000 rubles
```

### Keywords
```
Show keywords in ad group 456
Add keywords "buy iphone" and "iphone price" to group 456
Set bid 50 rubles on keyword 789
```

### Analytics
```
Show site stats for the last week
How many conversions on goal "Lead" for the last month?
Show traffic sources for counter 97538360
```

### Ad creation
```
Create a text ad in group 456:
- Title: Buy iPhone 15
- Text: Best prices! Free delivery
- URL: https://example.com/iphone
```

### Keyword research
```
Show top requests for "buy car from china"
Query dynamics for "electric car" over the last year
Regional distribution for "auto from japan"
```

## Alternative Run Methods

### Direct execution
```bash
python -m yandex_mcp
# or
python main.py
```

### MCP Inspector
```bash
npx @modelcontextprotocol/inspector python -m yandex_mcp
```

### Cursor IDE
Add to Cursor MCP settings in the same format as above.

## Project Structure

```
yandex_mcp/
├── __init__.py          # MCP server init and tool registration
├── client.py            # Async HTTP client for Direct, Metrika & Wordstat APIs
├── config.py            # Configuration and environment variables
├── utils.py             # Error handling utilities
├── models/              # Pydantic input models
│   ├── common.py
│   ├── direct.py
│   ├── direct_extended.py
│   ├── metrika.py
│   ├── metrika_extended.py
│   └── wordstat.py
├── formatters/          # Markdown output formatters
│   ├── direct.py
│   ├── metrika.py
│   └── wordstat.py
└── tools/               # MCP tool definitions
    ├── direct/          # 80 Yandex Direct tools
    │   ├── _helpers.py  # Shared manage-operation factory
    │   ├── campaigns.py
    │   ├── adgroups.py
    │   ├── ads.py
    │   ├── keywords.py
    │   ├── stats.py
    │   ├── images.py
    │   └── ...
    ├── metrika/         # 43 Yandex Metrika tools
    │   ├── counters.py
    │   ├── goals.py
    │   ├── reports.py
    │   └── ...
    └── wordstat.py      # 5 Yandex Wordstat tools
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Lint
ruff check .

# Type check
mypy yandex_mcp

# Test
pytest
```

## Security

- Store tokens in environment variables, not in code
- Use minimum required permissions
- Use sandbox for testing (`YANDEX_USE_SANDBOX=true`)
- Never commit `.env` files

## Links

- [Yandex Direct API docs](https://yandex.ru/dev/direct/doc/dg/concepts/about.html)
- [Yandex Metrika API docs](https://yandex.ru/dev/metrika/doc/api2/concept/about.html)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## License

MIT
