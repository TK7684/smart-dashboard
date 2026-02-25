# Shopee Dashboard

A comprehensive data pipeline and analytics dashboard for Shopee e-commerce operations. This project processes data from multiple Shopee sources (Orders, Ads, Live, Video) and provides actionable insights through an interactive Streamlit dashboard.

![Shopee Dashboard](https://img.shields.io/badge/Shopee-Ecommerce-orange) ![Python](https://img.shields.io/badge/Python-3.8+-blue) ![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red) ![DuckDB](https://img.shields.io/badge/DuckDB-0.9+-green)

---

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Data Pipeline](#data-pipeline)
- [Dashboard Calculations](#dashboard-calculations)
- [Master Tables](#master-tables)
- [Dashboard Queries](#dashboard-queries)
- [DuckDB Usage](#duckdb-usage)
- [File Structure](#file-structure)

---

## Project Overview

The Shopee Dashboard is a powerful analytics solution designed for Shopee sellers to monitor and optimize their e-commerce performance. It combines data from four key Shopee sources:

- **Shopee Orders** - Transaction data including sales, fees, and customer information
- **Shopee Ads** - Advertising campaign performance metrics
- **Shopee Live** - Live streaming commerce data
- **Shopee Video** - Video content engagement and sales

The dashboard provides real-time KPIs, trend analysis, product segmentation, ad performance tracking, and geographic insights to help sellers make data-driven decisions.

---

## Features

### Executive Dashboard
- **Hero KPIs** - Total Orders, GMV, Net Revenue, AOV, Units Sold with growth deltas (DoD, WoW, MoM)
- **Today's Performance** - Quick snapshot of current day metrics
- **Period Comparison** - Compare any two time periods (Day-over-Day, Week-over-Week, Month-over-Month)

### Trend Analysis
- **Daily Trend** - Last 90 days of sales performance with growth rates
- **Weekly Trend** - Aggregated weekly metrics
- **Monthly Trend** - Aggregated monthly metrics
- **GMV Segmentation** - Categorize days as Max (Top 20%), Middle, or Min (Bottom 20%)
- **AOV Segmentation** - Average Order Value analysis

### Product Analysis
- **Top Products** - Best-selling products by GMV
- **Product Segments** - Star, Hero, Volume, and Core product categorization
- **Product Risk Analysis** - Identify over-reliance on hero products and underperforming core products
- **GMV Contribution** - Percentage of total revenue per product

### Ads Performance
- **Ads Summary** - Overall campaign performance (Impressions, Clicks, ROAS, ACOS)
- **Top Campaigns** - Best performing campaigns by ROAS
- **Bleeding Campaigns** - Underperforming campaigns requiring attention
- **Campaign Health** - Classification: Excellent, Good, Break-even, Bleeding

### Geographic Analysis
- **Top Provinces** - Sales distribution by province
- **Province Distribution** - GMV share percentage per region

### Content Commerce
- **Live Streaming** - Sales, viewers, PCU (Peak Concurrent Users), GPM (Gross Merchandise Value per Minute)
- **Video Content** - Engagement metrics, sales, likes, shares, comments

---

## Installation & Setup

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Step 1: Clone or Download the Project

```bash
cd c:/Projects/Shopee-dashboard
```

### Step 2: Install Dependencies

Install all required Python packages:

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install pandas>=2.0.0
pip install numpy>=1.24.0
pip install openpyxl>=3.1.0
pip install duckdb>=0.9.0
pip install streamlit>=1.28.0
pip install plotly>=5.18.0
```

### Step 3: Prepare Data Folders

Ensure your Shopee data files are placed in the following folders:

| Folder | File Format | Description |
|--------|-------------|-------------|
| `Shopee orders/` | `.xlsx` | Order export files (e.g., `Order.all.20240101_20240131.xlsx`) |
| `Shopee Ad/` | `.csv` | Ads performance reports |
| `Shopee Live/` | `.csv` | Live streaming overview reports |
| `Shopee Video/` | `.csv` | Video engagement reports |

---

## Usage

### Running the Data Pipeline

Process all source data and create master tables:

**Option 1: Using Python**
```bash
python data_pipeline.py
```

**Option 2: Using Batch File (Windows)**
```bash
run_pipeline.bat
```

The pipeline will:
1. Load data from all four source folders
2. Clean and transform the data
3. Create master tables (Daily Sales, Products, Ads, Geographic)
4. Export to CSV files in `processed_data/`
5. Create a DuckDB database for fast queries

### Running the Dashboard

Launch the interactive Streamlit dashboard:

**Option 1: Using Python**
```bash
streamlit run dashboard.py
```

**Option 2: Using Batch File (Windows)**
```bash
run_dashboard.bat
```

**Option 3: Access via Tailscale (for remote access)**
```bash
run_dashboard_tailscale.bat
```

The dashboard will open in your default browser at `http://localhost:8501`

---

## Data Sources

### 1. Shopee Orders

**File Format:** Excel (.xlsx)

**Key Columns:**
- `หมายเลขคำสั่งซื้อ` - Order ID
- `สถานะการสั่งซื้อ` - Order Status
- `วันที่ทำการสั่งซื้อ` - Order Date
- `ชื่อสินค้า` - Product Name
- `ราคาขายสุทธิ` - Net Sales
- `ค่าคอมมิชชั่น` - Commission
- `Transaction Fee` - Transaction Fee
- `ค่าบริการ` - Service Fee
- `ส่วนลดจาก Shopee` - Shopee Discount
- `โค้ดส่วนลดชำระโดยผู้ขาย` - Seller Discount
- `จังหวัด` - Province

**Status Filter:** Only includes orders with status:
- `สำเร็จแล้ว` (Completed)
- `ที่ต้องจัดส่ง` (To Ship)
- `กำลังจัดส่ง` (Shipping)
- `รอการชำระเงิน` (Pending Payment)

### 2. Shopee Ads

**File Format:** CSV

**Key Metrics:**
- `การมองเห็น` - Impressions
- `จำนวนคลิก` - Clicks
- `อัตราการคลิก (CTR)` - Click-Through Rate
- `การสั่งซื้อ` - Orders
- `ยอดขาย` - Sales
- `ค่าโฆษณา` - Ad Cost
- `ยอดขาย/รายจ่าย (ROAS)` - Return on Ad Spend
- `ACOS` - Advertising Cost of Sales

### 3. Shopee Live

**File Format:** CSV (complex multi-row structure)

**Key Metrics:**
- `ยอดขาย(คำสั่งซื้อที่ยืนยันแล้ว)` - Confirmed Sales
- `คำสั่งซื้อ(คำสั่งซื้อที่ยืนยันแล้ว)` - Confirmed Orders
- `จำนวน Live ทั้งหมด` - Total Live Sessions
- `ระยะเวลา Live ทั้งหมด` - Total Live Duration
- `ผู้ชมทั้งหมด` - Total Viewers
- `PCU` - Peak Concurrent Users
- `GPM(คำสั่งซื้อที่ยืนยันแล้ว)` - Gross Merchandise Value per Minute

### 4. Shopee Video

**File Format:** CSV (complex multi-row structure)

**Key Metrics:**
- `ยอดขาย(คำสั่งซื้อที่ยืนยันแล้ว)` - Video Sales Confirmed
- `คำสั่งซื้อ(คำสั่งซื้อที่ยืนยันแล้ว)` - Video Orders Confirmed
- `การเข้าชม` - Total Views
- `ผู้ชมทั้งหมด` - Total Viewers
- `GPM(คำสั่งซื้อที่ยืนยันแล้ว)` - Video GPM
- `วิดีโอที่มีสินค้า` - Videos With Products
- `ถูกใจ` - Total Likes
- `แชร์ทั้งหมด` - Total Shares
- `คอมเมนต์ทั้งหมด` - Total Comments

---

## Data Pipeline

### Pipeline Architecture

```
┌─────────────────┐
│  Source Folders │
│  - Orders       │
│  - Ads          │
│  - Live         │
│  - Video        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Loading   │
│  - Parse files  │
│  - Extract dates│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Cleaning  │
│  - Filter status│
│  - Parse values │
│  - Calculate    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Master Tables  │
│  - Daily Sales  │
│  - Products     │
│  - Ads          │
│  - Geographic   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Export & Store │
│  - CSV Files    │
│  - DuckDB       │
└─────────────────┘
```

### Utility Parsers

#### `parse_thai_currency(value)`
Converts Thai currency strings to float:
- Removes `฿` symbol and commas
- Handles numeric values directly
- Returns `0.0` for invalid values

```python
parse_thai_currency("฿1,234.56")  # Returns: 1234.56
parse_thai_currency("1,234.56")    # Returns: 1234.56
```

#### `parse_percentage(value)`
Converts percentage strings to decimal:
- Removes `%` symbol
- Divides by 100 if value > 1
- Returns `0.0` for invalid values

```python
parse_percentage("15.5%")   # Returns: 0.155
parse_percentage("0.155")   # Returns: 0.155
```

#### `parse_time_duration(value)`
Parses Thai time duration strings:
- Extracts hours (`ชั่วโมง`), minutes (`นาที`), seconds (`วินาที`)
- Returns total seconds

```python
parse_time_duration("4ชั่วโมง9นาที28วินาที")  # Returns: 14968
```

### Orders Cleaning Process

1. **Column Renaming** - Maps Thai column names to English
2. **Status Filtering** - Keeps only valid order statuses
3. **Product Filtering** - Excludes non-skincare/supplement products using keywords:
   - `iphone`, `ipad`, `apple`, `samsung`, `phone`, `case`, `cable`
   - `charger`, `headphone`, `earphone`, `laptop`, `computer`
4. **Date Parsing** - Converts order dates to datetime objects
5. **Numeric Conversion** - Parses currency values for all monetary fields
6. **Fee Calculations** - Computes derived metrics

---

## Dashboard Calculations

### Revenue Calculations

| Metric | Formula | Description |
|--------|---------|-------------|
| **Total_Fees** | `Commission + Transaction_Fee + Service_Fee` | Sum of all platform fees |
| **True_Net_Revenue** | `Net_Sales - Total_Fees` | Revenue after all fees |
| **Total_Discount** | `Shopee_Discount + Seller_Discount` | Combined discounts |
| **GMV** | `Net_Sales` (aggregated) | Gross Merchandise Value |
| **AOV** | `GMV / Orders` | Average Order Value |

### Growth Rates

Growth rates are calculated using lag functions to compare against previous periods:

| Growth Type | Lag Period | Formula |
|-------------|------------|---------|
| **DoD** (Day-over-Day) | 1 day | `(Current - Previous) / Previous` |
| **WoW** (Week-over-Week) | 7 days | `(Current - Previous) / Previous` |
| **MoM** (Month-over-Month) | 30 days | `(Current - Previous) / Previous` |
| **QoQ** (Quarter-over-Quarter) | 90 days | `(Current - Previous) / Previous` |

### Product Segmentation

Products are categorized based on their GMV and Quantity relative to medians:

| Segment | GMV | Quantity | Description |
|---------|-----|----------|-------------|
| **Star** | > Median | > Median | High GMV, High Volume |
| **Hero** | > Median | ≤ Median | High GMV, Lower Volume |
| **Volume** | ≤ Median | > Median | Lower GMV, High Volume |
| **Core** | ≤ Median | ≤ Median | Average performance |

### GMV Segmentation

Daily performance is categorized based on GMV distribution:

| Segment | Condition | Description |
|---------|-----------|-------------|
| **Max (Top 20%)** | GMV ≥ 80th percentile | High performing days |
| **Middle** | 20th < GMV < 80th percentile | Average performing days |
| **Min (Bottom 20%)** | GMV ≤ 20th percentile | Low performing days |

### AOV Segmentation

Average Order Value is categorized relative to the mean:

| Segment | Condition | Description |
|---------|-----------|-------------|
| **Max (>20% Avg)** | AOV > Mean × 1.2 | High value orders |
| **Middle** | 0.8 × Mean ≤ AOV ≤ 1.2 × Mean | Average value orders |
| **Min (<-20% Avg)** | AOV < Mean × 0.8 | Low value orders |

### Ad Health Scoring

Campaigns are classified based on ROAS and performance:

| Health Status | ROAS | Orders/Clicks | Description |
|---------------|------|---------------|-------------|
| **🚀 Excellent** | ≥ 5 | - | Highly profitable |
| **✅ Good** | 3 - 5 | - | Profitable |
| **⚠️ Break-even** | 1 - 3 | - | Recovering ad costs |
| **🔴 Bleeding** | < 1 | 0 orders, >100 clicks | Losing money |
| **📊 Needs Monitoring** | < 1 | - | Underperforming |

### Product Risk Flags

Products are flagged for potential business risks:

| Risk Status | Condition | Action Required |
|-------------|-----------|-----------------|
| **⚠️ RISK: Over-reliance** | Hero product with >60% GMV contribution | Diversify product portfolio |
| **⚠️ URGENT: Need to push** | Core product with <25% GMV contribution | Promote core products |
| **✅ Healthy** | No risk conditions | Maintain current strategy |

---

## Master Tables

### Master_Daily_Sales.csv

Daily aggregated sales data with growth metrics and segmentations.

| Column | Type | Description |
|--------|------|-------------|
| Date | Date | Order date |
| Platform | String | Platform name (Shopee) |
| Orders | Integer | Number of orders |
| GMV | Float | Gross Merchandise Value |
| Net_Revenue | Float | Revenue after fees |
| Units_Sold | Integer | Total quantity sold |
| Total_Fees | Float | Sum of all fees |
| Total_Discount | Float | Total discounts |
| Commission | Float | Commission amount |
| Transaction_Fee | Float | Transaction fee |
| Service_Fee | Float | Service fee |
| AOV | Float | Average Order Value |
| DoD_GMV_Growth | Float | Day-over-Day growth |
| WoW_GMV_Growth | Float | Week-over-Week growth |
| MoM_GMV_Growth | Float | Month-over-Month growth |
| QoQ_GMV_Growth | Float | Quarter-over-Quarter growth |
| GMV_Segment | String | GMV performance segment |
| AOV_Segment | String | AOV performance segment |

### Master_Product_Sales.csv

Product-level aggregated data with segmentation and risk analysis.

| Column | Type | Description |
|--------|------|-------------|
| Product_Name | String | Product name |
| SKU | String | SKU reference |
| Platform | String | Platform name |
| Orders | Integer | Number of orders |
| Total_GMV | Float | Total GMV |
| Net_Revenue | Float | Net revenue |
| Total_Qty | Integer | Total quantity sold |
| Total_Discount | Float | Total discounts |
| Avg_Price | Float | Average selling price |
| Product_Segment | String | Product category (Star/Hero/Volume/Core) |
| GMV_Contribution_% | Float | Percentage of total GMV |
| Risk_Status | String | Risk assessment |

### Master_Ads_Performance.csv

Campaign-level advertising performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| Ad_Name | String | Campaign/ad name |
| Ad_Type | String | Ad type |
| Status | String | Campaign status |
| Platform | String | Platform name |
| Impressions | Integer | Number of impressions |
| Clicks | Integer | Number of clicks |
| Orders | Integer | Number of orders |
| Direct_Orders | Integer | Direct orders |
| Products_Sold | Integer | Products sold |
| Sales | Float | Total sales |
| Direct_Sales | Float | Direct sales |
| Ad_Cost | Float | Ad spend |
| CTR | Float | Click-through rate |
| Conversion_Rate | Float | Conversion rate |
| ROAS | Float | Return on ad spend |
| ACOS | Float | Advertising cost of sales |
| Campaign_Health | String | Health classification |

### Master_Geographic.csv

Sales distribution by province.

| Column | Type | Description |
|--------|------|-------------|
| Province | String | Province name |
| Platform | String | Platform name |
| Orders | Integer | Number of orders |
| GMV | Float | Total GMV |
| Units_Sold | Integer | Total units sold |

---

## Dashboard Queries

The dashboard uses pre-built SQL queries stored in [`dashboard_queries.py`](dashboard_queries.py). These can be used directly with DuckDB or connected to BI tools like Looker Studio.

### Hero KPI Queries

#### HERO_KPIS
```sql
-- Executive Summary KPIs
SELECT
    SUM(Orders) as Total_Orders,
    SUM(GMV) as Total_GMV,
    SUM(Net_Revenue) as Total_Net_Revenue,
    AVG(AOV) as Average_AOV,
    SUM(Units_Sold) as Total_Units,
    SUM(Total_Fees) as Total_Fees,
    SUM(Commission) as Total_Commission
FROM daily_sales
WHERE Date >= CURRENT_DATE - INTERVAL '30 days'
```

#### TODAY_KPIS
```sql
-- Today's Performance
SELECT
    Date,
    GMV,
    Orders,
    AOV,
    GMV_Segment,
    DoD_GMV_Growth * 100 as DoD_Growth_Pct,
    WoW_GMV_Growth * 100 as WoW_Growth_Pct
FROM daily_sales
WHERE Date = (SELECT MAX(Date) FROM daily_sales)
```

### Trend Analysis Queries

#### DAILY_TREND
```sql
-- Daily Sales Trend (Last 90 days)
SELECT
    Date,
    GMV,
    Orders,
    AOV,
    Net_Revenue,
    GMV_Segment,
    LAG(GMV) OVER (ORDER BY Date) as Previous_Day_GMV,
    LAG(GMV, 7) OVER (ORDER BY Date) as Previous_Week_GMV,
    LAG(GMV, 30) OVER (ORDER BY Date) as Previous_Month_GMV
FROM daily_sales
WHERE Date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY Date
```

#### WEEKLY_TREND
```sql
-- Weekly Aggregation
SELECT
    DATE_TRUNC('week', Date) as Week_Start,
    SUM(GMV) as Weekly_GMV,
    SUM(Orders) as Weekly_Orders,
    AVG(AOV) as Weekly_AOV,
    SUM(Net_Revenue) as Weekly_Net_Revenue
FROM daily_sales
GROUP BY DATE_TRUNC('week', Date)
ORDER BY Week_Start DESC
```

#### MONTHLY_TREND
```sql
-- Monthly Aggregation
SELECT
    DATE_TRUNC('month', Date) as Month_Start,
    SUM(GMV) as Monthly_GMV,
    SUM(Orders) as Monthly_Orders,
    AVG(AOV) as Monthly_AOV,
    SUM(Net_Revenue) as Monthly_Net_Revenue,
    SUM(Total_Fees) as Monthly_Fees
FROM daily_sales
GROUP BY DATE_TRUNC('month', Date)
ORDER BY Month_Start DESC
```

### Product Analysis Queries

#### TOP_PRODUCTS
```sql
-- Top 20 Products by GMV
SELECT
    Product_Name,
    Product_Segment,
    Total_GMV,
    Total_Qty,
    Avg_Price,
    GMV_Contribution_%,
    Risk_Status
FROM products
ORDER BY Total_GMV DESC
LIMIT 20
```

#### PRODUCT_SEGMENT_SUMMARY
```sql
-- Product Segment Summary
SELECT
    Product_Segment,
    COUNT(*) as Product_Count,
    SUM(Total_GMV) as Segment_GMV,
    SUM(Total_Qty) as Segment_Units,
    AVG(Avg_Price) as Avg_Price,
    SUM(GMV_Contribution_%) as Total_Contribution
FROM products
GROUP BY Product_Segment
ORDER BY Segment_GMV DESC
```

#### PRODUCT_RISK_ANALYSIS
```sql
-- Products with Risk Flags
SELECT
    Product_Name,
    Product_Segment,
    Total_GMV,
    GMV_Contribution_%,
    Risk_Status
FROM products
WHERE Risk_Status != '✅ Healthy'
ORDER BY Total_GMV DESC
```

### Ads Performance Queries

#### ADS_SUMMARY
```sql
-- Overall Ads Performance
SELECT
    SUM(Impressions) as Total_Impressions,
    SUM(Clicks) as Total_Clicks,
    SUM(Orders) as Total_Orders,
    SUM(Sales) as Total_Sales,
    SUM(Ad_Cost) as Total_Ad_Spend,
    SUM(Sales) / NULLIF(SUM(Ad_Cost), 0) as Blended_ROAS,
    SUM(Ad_Cost) / NULLIF(SUM(Sales), 0) * 100 as ACOS_Pct,
    SUM(Clicks) / NULLIF(SUM(Impressions), 0) * 100 as CTR_Pct
FROM ads_performance
```

#### TOP_CAMPAIGNS
```sql
-- Top Performing Campaigns
SELECT
    Ad_Name,
    Ad_Type,
    Status,
    Impressions,
    Clicks,
    Orders,
    Sales,
    Ad_Cost,
    ROAS,
    ACOS,
    Campaign_Health
FROM ads_performance
WHERE Status = 'กำลังดำเนินการ'
ORDER BY ROAS DESC
LIMIT 15
```

#### BLEEDING_CAMPAIGNS
```sql
-- Campaigns Losing Money
SELECT
    Ad_Name,
    Ad_Type,
    Impressions,
    Clicks,
    Orders,
    Sales,
    Ad_Cost,
    ROAS,
    Campaign_Health
FROM ads_performance
WHERE (ROAS < 1 OR (Orders = 0 AND Clicks > 100))
ORDER BY Ad_Cost DESC
```

### Geographic Analysis Queries

#### TOP_PROVINCES
```sql
-- Top Provinces by GMV
SELECT
    Province,
    Orders,
    GMV,
    Units_Sold,
    GMV / NULLIF(Orders, 0) as Avg_Order_Value
FROM geographic
ORDER BY GMV DESC
LIMIT 20
```

#### PROVINCE_DISTRIBUTION
```sql
-- Province Sales Distribution
SELECT
    Province,
    Orders,
    GMV,
    Units_Sold,
    GMV * 100.0 / (SELECT SUM(GMV) FROM geographic) as GMV_Share_Pct
FROM geographic
WHERE GMV > 0
ORDER BY GMV DESC
```

### Segmentation Queries

#### GMV_SEGMENT_ANALYSIS
```sql
-- GMV Segment Distribution
SELECT
    GMV_Segment,
    COUNT(*) as Day_Count,
    SUM(GMV) as Segment_GMV,
    AVG(GMV) as Avg_Daily_GMV,
    AVG(Orders) as Avg_Daily_Orders,
    MIN(Date) as First_Date,
    MAX(Date) as Last_Date
FROM daily_sales
GROUP BY GMV_Segment
ORDER BY Segment_GMV DESC
```

#### AOV_SEGMENT_ANALYSIS
```sql
-- AOV Segment Distribution
SELECT
    AOV_Segment,
    COUNT(*) as Day_Count,
    AVG(AOV) as Avg_AOV,
    AVG(GMV) as Avg_GMV,
    AVG(Orders) as Avg_Orders
FROM daily_sales
GROUP BY AOV_Segment
```

### Comparison Queries

#### PERIOD_COMPARISON
```sql
-- Compare Current Period vs Previous Period
WITH current_period AS (
    SELECT
        SUM(GMV) as GMV,
        SUM(Orders) as Orders,
        AVG(AOV) as AOV,
        SUM(Net_Revenue) as Net_Revenue
    FROM daily_sales
    WHERE Date >= CURRENT_DATE - INTERVAL '7 days'
),
previous_period AS (
    SELECT
        SUM(GMV) as GMV,
        SUM(Orders) as Orders,
        AVG(AOV) as AOV,
        SUM(Net_Revenue) as Net_Revenue
    FROM daily_sales
    WHERE Date >= CURRENT_DATE - INTERVAL '14 days'
      AND Date < CURRENT_DATE - INTERVAL '7 days'
)
SELECT
    c.GMV as Current_GMV,
    p.GMV as Previous_GMV,
    (c.GMV - p.GMV) / NULLIF(p.GMV, 0) * 100 as WoW_Growth_Pct,
    c.Orders as Current_Orders,
    p.Orders as Previous_Orders,
    (c.Orders - p.Orders) / NULLIF(p.Orders, 0) * 100 as Orders_Growth_Pct,
    c.AOV as Current_AOV,
    p.AOV as Previous_AOV,
    (c.AOV - p.AOV) / NULLIF(p.AOV, 0) * 100 as AOV_Growth_Pct
FROM current_period c, previous_period p
```

### Content Commerce Queries

#### CONTENT_PERFORMANCE
```sql
-- Combined Content Commerce View
SELECT
    Report_Date,
    'Live' as Content_Type,
    Sales_Confirmed as Sales,
    Orders_Confirmed as Orders,
    Total_Viewers as Viewers,
    Peak_Concurrent_Users as PCU,
    GPM_Confirmed as GPM
FROM combined_live

UNION ALL

SELECT
    Report_Date,
    'Video' as Content_Type,
    Video_Sales_Confirmed as Sales,
    Video_Orders_Confirmed as Orders,
    Total_Viewers as Viewers,
    NULL as PCU,
    Video_GPM_Confirmed as GPM
FROM combined_video

ORDER BY Report_Date DESC
```

---

## DuckDB Usage

### What is DuckDB?

DuckDB is an in-process SQL OLAP database management system designed for fast analytical queries. It provides:

- **Zero Configuration** - No server setup required
- **Fast Performance** - Columnar storage optimized for analytics
- **SQL Compatibility** - Full SQL support with window functions
- **Python Integration** - Seamless pandas and Python integration
- **Portable** - Single file database

### Database Schema

The pipeline creates the following tables in [`processed_data/shopee_dashboard.duckdb`](processed_data/shopee_dashboard.duckdb):

| Table | Description |
|-------|-------------|
| `daily_sales` | Daily aggregated sales data |
| `products` | Product-level metrics |
| `ads_performance` | Campaign performance data |
| `geographic` | Sales by province |
| `orders_raw` | Raw order data (key columns) |
| `daily_geographic` | Daily geographic breakdown |
| `combined_live` | Live streaming data |
| `combined_video` | Video engagement data |

### Pre-built Views

The database includes pre-built views for common queries:

| View | Purpose |
|------|---------|
| `kpi_summary` | Executive KPIs |
| `top_products` | Top 20 products |
| `daily_trend` | Daily sales trend |

### Querying the Database

Using Python:

```python
import duckdb
from pathlib import Path

# Connect to database
conn = duckdb.connect("processed_data/shopee_dashboard.duckdb", read_only=True)

# Run query
result = conn.execute("SELECT * FROM daily_sales ORDER BY Date DESC LIMIT 10").fetchdf()

# Close connection
conn.close()
```

Using [`dashboard_queries.py`](dashboard_queries.py):

```python
from dashboard_queries import run_query

# Run named query
df = run_query('hero_kpis')
print(df)
```

### Connecting to BI Tools

DuckDB can be connected to BI tools like Looker Studio, Tableau, or Power BI using:

1. **DuckDB Extension** - Native DuckDB connector
2. **ODBC Driver** - Generic database connection
3. **Export to CSV** - Use master CSV files directly

---

## File Structure

```
Shopee-dashboard/
├── data_pipeline.py              # Main data processing pipeline
├── dashboard.py                  # Streamlit dashboard application
├── dashboard_queries.py          # SQL query definitions
├── auto_monitor.py               # Auto-monitoring script
├── requirements.txt              # Python dependencies
│
├── run_pipeline.bat              # Windows batch file for pipeline
├── run_dashboard.bat             # Windows batch file for dashboard
├── run_dashboard_tailscale.bat   # Windows batch file for remote access
│
├── Shopee orders/                # Order data source folder
│   ├── Order.all.20240101_20240131.xlsx
│   ├── Order.all.20240201_20240229.xlsx
│   └── ...
│
├── Shopee Ad/                    # Ads data source folder
│   ├── ข้อมูล-Shopee-Ads-01_01_2026-31_01_2026.csv
│   ├── ข้อมูล-Shopee-Ads-01_02_2026-20_02_2026.csv
│   └── ...
│
├── Shopee Live/                  # Live streaming data source folder
│   ├── overview-v2_1d_2026-02-19_*.csv
│   ├── overview-v2_1m_2026-01-31_*.csv
│   └── ...
│
├── Shopee Video/                 # Video data source folder
│   ├── export-sc_0_1m_2026-01-31_*.csv
│   ├── export-sc_0_1m_2026-02-28_*.csv
│   └── ...
│
├── processed_data/               # Output folder
│   ├── shopee_dashboard.duckdb   # DuckDB database
│   │
│   ├── Master_Daily_Sales.csv    # Daily sales master table
│   ├── Master_Product_Sales.csv  # Product master table
│   ├── Master_Ads_Performance.csv # Ads master table
│   ├── Master_Geographic.csv     # Geographic master table
│   │
│   ├── Combined_Orders.csv       # Combined raw orders
│   ├── Combined_Ads.csv          # Combined raw ads
│   ├── Combined_Live.csv         # Combined raw live data
│   └── Combined_Video.csv        # Combined raw video data
│
├── .streamlit/                   # Streamlit configuration
├── .claude/                      # Claude AI configuration
├── CLAUDE.md                     # Claude AI documentation
└── README.md                     # This file
```

---

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is for personal and commercial use.

## Support

For questions or issues, please refer to the project documentation or create an issue in the repository.

---

**Last Updated:** 2026-02-20
