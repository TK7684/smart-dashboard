"""
Shared utilities for Shopee Dashboard
"""
import duckdb
import pandas as pd
import re
from pathlib import Path

# Database connection
DB_PATH = Path(__file__).parent / "processed_data" / "shopee_dashboard.duckdb"
conn = duckdb.connect(str(DB_PATH), read_only=True)

def safe_float(value):
    """Safely convert value to float, handling Thai currency format"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # Remove currency symbols, commas, and whitespace
    cleaned = re.sub(r'[฿,\s]', '', str(value))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def format_currency(value):
    """Format number as Thai Baht currency"""
    try:
        num = safe_float(value)
        if num >= 1000000:
            return f"฿{num/1000000:.2f}M"
        elif num >= 1000:
            return f"฿{num/1000:.1f}K"
        else:
            return f"฿{num:,.0f}"
    except:
        return "฿0"

def format_number(value):
    """Format large numbers with K/M suffix"""
    try:
        num = safe_float(value)
        if num >= 1000000:
            return f"{num/1000000:.2f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        else:
            return f"{num:,.0f}"
    except:
        return "0"

def format_percent(value):
    """Format as percentage"""
    try:
        num = safe_float(value)
        return f"{num*100:+.1f}%"
    except:
        return "-%"

def metric_with_tooltip(label, value, delta=None, help_text=""):
    """Display metric with tooltip"""
    if delta:
        st.metric(label, value, delta=delta, help=help_text)
    else:
        st.metric(label, value, help=help_text)

# Data loaders with caching handled in individual pages
def load_daily_sales():
    return conn.execute("SELECT * FROM daily_sales ORDER BY Date").fetchdf()

def load_products():
    return conn.execute("SELECT * FROM products ORDER BY Total_GMV DESC").fetchdf()

def load_orders_raw():
    try:
        return conn.execute("SELECT * FROM orders_raw").fetchdf()
    except:
        return pd.DataFrame()

def load_ads():
    return conn.execute("SELECT * FROM ads_performance ORDER BY ROAS DESC").fetchdf()

def load_geographic():
    return conn.execute("SELECT * FROM geographic ORDER BY GMV DESC").fetchdf()

def load_daily_geographic():
    try:
        return conn.execute("SELECT * FROM daily_geographic").fetchdf()
    except:
        return pd.DataFrame()

def load_live_data():
    try:
        return conn.execute("SELECT * FROM combined_live").fetchdf()
    except:
        return pd.DataFrame()

def load_video_data():
    try:
        return conn.execute("SELECT * FROM combined_video").fetchdf()
    except:
        return pd.DataFrame()

def load_tiktok_live():
    """Load TikTok Live session data"""
    try:
        return conn.execute("SELECT * FROM tiktok_live").fetchdf()
    except:
        return pd.DataFrame()

def load_tiktok_video():
    """Load TikTok Video data"""
    try:
        return conn.execute("SELECT * FROM tiktok_video").fetchdf()
    except:
        return pd.DataFrame()

# Common CSS styles
COMMON_STYLES = """
<style>
    /* Main background */
    .stApp {
        background-color: #fafafa;
    }

    /* Header styling */
    .main-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 0.3rem;
    }

    /* Metric cards */
    div[data-testid="metric-container"] {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 12px 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }

    /* Metric labels */
    div[data-testid="metric-container"] > label {
        font-size: 0.85rem;
        color: #666;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #333;
        border-bottom: 2px solid #ee4d2d;
        padding-bottom: 0.3rem;
        margin-top: 1rem;
        margin-bottom: 0.8rem;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #fff;
        border-right: 1px solid #e0e0e0;
    }

    /* Buttons */
    .stButton button {
        background-color: #ee4d2d;
        color: white;
        border: none;
        border-radius: 6px;
    }

    .stButton button:hover {
        background-color: #d73211;
    }

    /* Dataframes */
    .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
    }

    /* Expander */
    .streamlit-expanderHeader {
        background-color: #f5f5f5;
        border-radius: 6px;
    }

    /* Positive delta */
    [data-testid="stMetricDelta"] > div {
        color: #28a745;
    }

    /* Negative delta */
    [data-testid="stMetricDelta"] > div:nth-child(2) {
        color: #dc3545;
    }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Platform selector styling */
    .platform-selector {
        background: white;
        border-radius: 8px;
        padding: 10px;
        border: 1px solid #e0e0e0;
    }
</style>
"""
