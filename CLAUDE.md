# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Shopee Executive Dashboard - An ETL pipeline and Streamlit dashboard for analyzing Shopee and TikTok e-commerce data. Processes orders, ads, live streaming, and video engagement data from Thai-language source files. Features AI-powered content analysis using Gemini.

## Commands

### Run the Data Pipeline
```bash
python data_pipeline.py
```
Must stop dashboard first (releases database lock), then restart dashboard after.

### Start Dashboard (Local)
```bash
python -m streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501
```

### Docker Deployment (24/7 Server)
```bash
# Build and start
docker compose up -d --build

# View logs
docker compose logs -f

# Stop
docker compose down
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Architecture

### Data Flow
```
Source Folders → data_pipeline.py → DuckDB Database → Streamlit Pages
    │                    │                  │              │
    ├─ Shopee orders/    ├─ load_*()        ├─ daily_sales ├─ dashboard.py
    ├─ Shopee Ad/        ├─ clean_*()       ├─ products    ├─ pages/1_🎬_Content_Commerce.py
    ├─ Shopee Live/      ├─ create_*_master ├─ ads         └─ pages/2_📈_Analytics.py
    ├─ Shopee Video/     └─ DuckDB tables   ├─ geographic
    ├─ Tiktok Live/                         ├─ tiktok_live
    └─ Tiktok Video/                        └─ tiktok_video
```

### Multipage Dashboard Structure

| Page | File | Purpose |
|------|------|---------|
| Main Dashboard | `dashboard.py` | Sales, Products, Ads, Geographic overview |
| Content Commerce | `pages/1_🎬_Content_Commerce.py` | Live/Video analysis, AI scoring, Gemini insights |
| Analytics | `pages/2_📈_Analytics.py` | ML models, forecasting, correlation analysis |

### Key Files

| File | Purpose |
|------|---------|
| `data_pipeline.py` | ETL: loads, cleans, transforms 6 data sources into DuckDB |
| `dashboard.py` | Main Streamlit dashboard with Plotly visualizations |
| `utils.py` | Shared utilities: `safe_float()`, `format_currency()`, data loaders, CSS styles |
| `processed_data/shopee_dashboard.duckdb` | DuckDB database (single source of truth) |
| `.streamlit/config.toml` | Streamlit theme configuration (light theme) |

### Database Tables

**Shopee Data:**
- `daily_sales` - Aggregated daily GMV, orders, AOV with growth metrics
- `products` - Product-level performance with segmentation
- `ads_performance` - Campaign-level ROAS, ACOS metrics
- `geographic` - Province-level GMV aggregation
- `daily_geographic` - Time-filtered geographic data
- `orders_raw` - Raw order records for time-filtered queries
- `combined_live` - Shopee Live streaming sessions with Report_Date
- `combined_video` - Shopee Video engagement data with Report_Date

**TikTok Data:**
- `tiktok_live` - TikTok Live session data (session-level granularity)
- `tiktok_video` - TikTok Video engagement data with Video_ID

## Deployment

### 24/7 Server (100.66.69.21)
Dashboard runs on a Linux/WSL2 server with Docker and Tailscale Funnel.

**Public URL:** https://com4.trout-adhara.ts.net

**SSH Access:** `ssh tk578@100.66.69.21`

**Server Commands:**
```bash
# Dashboard management
docker compose -f ~/Shopee-dashboard/docker-compose.yml up -d
docker compose -f ~/Shopee-dashboard/docker-compose.yml down
docker compose -f ~/Shopee-dashboard/docker-compose.yml logs -f

# Deploy updates (from local machine)
scp pages/1_🎬_Content_Commerce.py tk578@100.66.69.21:~/Shopee-dashboard/pages/
docker compose -f ~/Shopee-dashboard/docker-compose.yml up -d --build

# View pipeline logs
tail -f ~/Shopee-dashboard/pipeline.log

# File watcher status
sudo systemctl status shopee-watcher
sudo journalctl -u shopee-watcher -f
```

### Automated Database Updates
The server has a file watcher (inotify) that automatically:
1. Detects new files in source folders
2. Stops the Docker container (releases DB lock)
3. Runs `data_pipeline.py` to update the database
4. Restarts the Docker container

**Files:**
- `scripts/pipeline_updater.sh` - Handles Docker lifecycle and pipeline execution
- `scripts/start_watcher.sh` - Starts inotifywait monitoring
- `systemd/shopee-watcher.service` - Systemd service definition

**Reinstall watcher (if needed):**
```bash
sudo ~/Shopee-dashboard/scripts/install_watcher.sh
```

### Database File Lock
DuckDB locks the database file when in use. Stop the dashboard before running the pipeline manually.

## Data Processing Notes

### Thai Currency Handling
Source files contain values like `"฿1,208"` (string with symbol and commas). Always use `parse_thai_currency()` in pipeline or `safe_float()` in dashboard to convert to numeric before any aggregation.

**Critical**: When summing columns that may be VARCHAR, apply conversion to each value BEFORE summing:
```python
# WRONG - concatenates strings
total = safe_float(df[column].sum())

# CORRECT - converts each value first
total = df[column].apply(safe_float).sum()
```

### Column Name Mapping
Source files use Thai column names. The pipeline maps them to English via column map dictionaries:
- `ORDER_COLUMN_MAP` - Shopee orders
- `TIKTOK_LIVE_COLUMN_MAP` - TikTok live sessions
- `TIKTOK_VIDEO_COLUMN_MAP` - TikTok videos

New source files may have different column names - check actual columns if loading fails.

### Date Extraction
- Orders: Date from `Order_Date` column
- Live/Video: Date extracted from filename pattern `YYYY-MM-DD` in `File_Source`
- Ads: Campaign-level aggregated (no date granularity)

### Content Deduplication
When displaying top videos/live sessions, always deduplicate first:
```python
# Group by Video_ID and aggregate
deduped = tiktok_video.groupby('Video_ID', as_index=False).agg({
    'Views': 'sum',
    'Likes': 'sum',
    'Creator': 'first',
    'Video_Title': 'first',
})
```

### Video ID Display
TikTok Video_IDs are large integers that display as scientific notation. Convert to string:
```python
video_id = str(int(safe_float(row.get('Video_ID', 0))))
```

## AI Features

### Ad Boost Scoring Model
Content is ranked using weighted scoring in `pages/1_🎬_Content_Commerce.py`:

**Video Scoring** (no GMV data):
- Views (reach potential): 25%
- Engagement Rate (audience quality): 30%
- Conversion Rate (sales potential): 25%
- Like Rate (content quality): 20%

**Live Session Scoring:**
- GMV (revenue): 35%
- Engagement Rate: 25%
- Conversion Rate: 25%
- Follower Growth: 15%

### Gemini AI Integration
The Content Commerce page uses Gemini API for:
- Video performance analysis
- Live session insights
- Marketing strategy recommendations

API is configured in `pages/1_🎬_Content_Commerce.py` using `google-generativeai` package.

## Product Segmentation

Products are segmented by median GMV and quantity:
- Star: High GMV + High Volume
- Hero: High GMV only
- Volume: High Volume only
- Core: Average
