"""
Shopee Dashboard SQL Queries
=============================
Pre-built SQL queries for DuckDB database
Use these in Looker Studio or any BI tool connected to the DuckDB database
"""

import duckdb
from pathlib import Path

# Database path
DB_PATH = Path(r"C:\Projects\Shopee-dashboard\processed_data\shopee_dashboard.duckdb")


def get_connection():
    """Get DuckDB connection"""
    return duckdb.connect(str(DB_PATH), read_only=True)


# ==========================================
# HERO KPI QUERIES
# ==========================================

HERO_KPIS = """
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
"""

TODAY_KPIS = """
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
"""

# ==========================================
# TREND ANALYSIS QUERIES
# ==========================================

DAILY_TREND = """
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
"""

WEEKLY_TREND = """
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
"""

MONTHLY_TREND = """
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
"""

# ==========================================
# PRODUCT ANALYSIS QUERIES
# ==========================================

TOP_PRODUCTS = """
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
"""

PRODUCT_SEGMENT_SUMMARY = """
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
"""

PRODUCT_RISK_ANALYSIS = """
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
"""

# ==========================================
# ADS PERFORMANCE QUERIES
# ==========================================

ADS_SUMMARY = """
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
"""

TOP_CAMPAIGNS = """
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
"""

BLEEDING_CAMPAIGNS = """
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
"""

# ==========================================
# GEOGRAPHIC ANALYSIS QUERIES
# ==========================================

TOP_PROVINCES = """
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
"""

PROVINCE_DISTRIBUTION = """
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
"""

# ==========================================
# SEGMENTATION QUERIES
# ==========================================

GMV_SEGMENT_ANALYSIS = """
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
"""

AOV_SEGMENT_ANALYSIS = """
-- AOV Segment Distribution
SELECT
    AOV_Segment,
    COUNT(*) as Day_Count,
    AVG(AOV) as Avg_AOV,
    AVG(GMV) as Avg_GMV,
    AVG(Orders) as Avg_Orders
FROM daily_sales
GROUP BY AOV_Segment
"""

# ==========================================
# COMPARISON QUERIES (D/D, W/W, M/M)
# ==========================================

PERIOD_COMPARISON = """
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
"""

# ==========================================
# CONTENT COMMERCE QUERIES (LIVE & VIDEO)
# ==========================================

CONTENT_PERFORMANCE = """
-- Combined Content Commerce View
-- Note: Requires live and video tables to be loaded
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
"""

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def run_query(query_name):
    """Run a named query and return DataFrame"""
    queries = {
        'hero_kpis': HERO_KPIS,
        'today_kpis': TODAY_KPIS,
        'daily_trend': DAILY_TREND,
        'weekly_trend': WEEKLY_TREND,
        'monthly_trend': MONTHLY_TREND,
        'top_products': TOP_PRODUCTS,
        'product_segments': PRODUCT_SEGMENT_SUMMARY,
        'product_risks': PRODUCT_RISK_ANALYSIS,
        'ads_summary': ADS_SUMMARY,
        'top_campaigns': TOP_CAMPAIGNS,
        'bleeding_campaigns': BLEEDING_CAMPAIGNS,
        'top_provinces': TOP_PROVINCES,
        'province_distribution': PROVINCE_DISTRIBUTION,
        'gmv_segments': GMV_SEGMENT_ANALYSIS,
        'aov_segments': AOV_SEGMENT_ANALYSIS,
        'period_comparison': PERIOD_COMPARISON,
    }

    if query_name not in queries:
        print(f"Query '{query_name}' not found. Available: {list(queries.keys())}")
        return None

    try:
        conn = get_connection()
        result = conn.execute(queries[query_name]).fetchdf()
        conn.close()
        return result
    except Exception as e:
        print(f"Error running query: {e}")
        return None


def list_tables():
    """List all tables in the database"""
    conn = get_connection()
    tables = conn.execute("SHOW TABLES").fetchdf()
    conn.close()
    return tables


def get_table_schema(table_name):
    """Get schema for a specific table"""
    conn = get_connection()
    schema = conn.execute(f"DESCRIBE {table_name}").fetchdf()
    conn.close()
    return schema


# ==========================================
# EXAMPLE USAGE
# ==========================================
if __name__ == "__main__":
    print("=" * 60)
    print("SHOPEE DASHBOARD SQL QUERIES")
    print("=" * 60)

    # List available tables
    print("\n📋 Available Tables:")
    print(list_tables())

    # Run example queries
    print("\n📊 Hero KPIs:")
    print(run_query('hero_kpis'))

    print("\n📈 Daily Trend (Last 5 days):")
    trend = run_query('daily_trend')
    if trend is not None:
        print(trend.head())

    print("\n🏆 Top 10 Products:")
    print(run_query('top_products').head(10))

    print("\n📢 Ads Summary:")
    print(run_query('ads_summary'))

    print("\n🗺️ Top Provinces:")
    print(run_query('top_provinces').head(10))
