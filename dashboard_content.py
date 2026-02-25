"""
Content Commerce Analytics Dashboard
=====================================
TikTok + Shopee Video/Live Analytics Dashboard
Streamlit dashboard with DuckDB for fast SQL queries

Run: streamlit run dashboard_content.py --server.port 8502
"""

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Content Commerce Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Light theme CSS with TikTok/Shopee colors
st.markdown("""
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

    /* Metric values */
    div[data-testid="metric-container"] > div {
        font-size: 1.4rem;
        font-weight: 600;
        color: #1a1a1a;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #333;
        border-bottom: 2px solid #000000;
        padding-bottom: 0.3rem;
        margin-top: 1rem;
        margin-bottom: 0.8rem;
    }

    /* Platform colors */
    .tiktok-color { color: #FF0050; }
    .shopee-color { color: #EE4D2D; }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #fff;
        border-right: 1px solid #e0e0e0;
    }

    /* Buttons */
    .stButton button {
        background-color: #000000;
        color: white;
        border: none;
        border-radius: 6px;
    }

    .stButton button:hover {
        background-color: #333333;
    }

    /* Dataframes */
    .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        background-color: #f0f0f0;
        border-radius: 6px 6px 0 0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #000000;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Database path
DB_PATH = Path(__file__).parent / "processed_data" / "shopee_dashboard.duckdb"

# ==========================================
# DATABASE CONNECTION
# ==========================================
@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)

conn = get_connection()

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def safe_float(value, default=0.0):
    """Safely convert value to float"""
    if pd.isna(value):
        return default
    try:
        return float(str(value).replace('฿', '').replace(',', '').replace('"', '').strip())
    except:
        return default

def format_currency(value):
    """Format as Thai Baht currency"""
    try:
        return f"฿{safe_float(value):,.0f}"
    except:
        return "฿0"

def format_number(value):
    """Format as number with commas"""
    try:
        return f"{safe_float(value):,.0f}"
    except:
        return "0"

def format_percent(value):
    """Format as percentage"""
    try:
        v = safe_float(value)
        return f"{v*100:.1f}%" if not pd.isna(v) else "-"
    except:
        return "-"

def format_duration(seconds):
    """Format seconds as human-readable duration"""
    seconds = safe_float(seconds)
    if seconds >= 3600:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"
    elif seconds >= 60:
        mins = int(seconds // 60)
        return f"{mins}m"
    else:
        return f"{int(seconds)}s"

# ==========================================
# DATA LOADING FUNCTIONS
# ==========================================
@st.cache_data(ttl=300)
def load_tiktok_live():
    try:
        return conn.execute("SELECT * FROM tiktok_live").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_tiktok_video():
    try:
        return conn.execute("SELECT * FROM tiktok_video").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_shopee_live():
    try:
        return conn.execute("SELECT * FROM combined_live").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_shopee_video():
    try:
        return conn.execute("SELECT * FROM combined_video").fetchdf()
    except:
        return pd.DataFrame()

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 🎬 Content Commerce")
    st.markdown("**TikTok + Shopee Analytics**")
    st.markdown("---")

    # Load data for date range
    tiktok_live = load_tiktok_live()
    tiktok_video = load_tiktok_video()
    shopee_live = load_shopee_live()
    shopee_video = load_shopee_video()

    # Determine date range from all data sources
    all_dates = []
    for df in [tiktok_live, tiktok_video]:
        if not df.empty and 'Report_Date' in df.columns:
            dates = pd.to_datetime(df['Report_Date'], errors='coerce').dropna()
            all_dates.extend(dates.tolist())

    if all_dates:
        min_date = min(all_dates).date()
        max_date = max(all_dates).date()
    else:
        min_date = datetime.now().date() - timedelta(days=30)
        max_date = datetime.now().date()

    # Quick time range selection
    st.markdown("#### ⚡ Quick Select")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("This Week", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=7), max_date)
        if st.button("This Month", use_container_width=True):
            st.session_state.date_range = (datetime(max_date.year, max_date.month, 1).date(), max_date)

    with col2:
        if st.button("Last 7 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=7), max_date)
        if st.button("Last 30 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=30), max_date)

    st.markdown("---")

    # Custom date range
    st.markdown("#### 📅 Custom Range")

    default_start = st.session_state.get('date_range', (max_date - timedelta(days=30), max_date))[0]
    default_end = st.session_state.get('date_range', (max_date - timedelta(days=30), max_date))[1]

    date_range = st.date_input(
        "Select period",
        value=(default_start, default_end),
        min_value=min_date,
        max_value=max_date,
        label_visibility="collapsed"
    )

    st.markdown("---")

    # Platform filters
    st.markdown("#### 🔍 Platforms")
    show_tiktok = st.checkbox("TikTok", value=True)
    show_shopee = st.checkbox("Shopee Content", value=True)

    st.markdown("---")

    # Info
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if all_dates:
        st.markdown(f"**Data Range:** {min_date} to {max_date}")

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# FILTER DATA BY DATE RANGE
# ==========================================
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

def filter_by_date(df, date_col='Report_Date'):
    """Filter dataframe by selected date range"""
    if df.empty or date_col not in df.columns:
        return df
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    mask = (df[date_col] >= pd.to_datetime(start_date)) & (df[date_col] <= pd.to_datetime(end_date))
    return df[mask].copy()

# Filter all data by date
tiktok_live_filtered = filter_by_date(tiktok_live) if show_tiktok else pd.DataFrame()
tiktok_video_filtered = filter_by_date(tiktok_video) if show_tiktok else pd.DataFrame()
shopee_live_filtered = filter_by_date(shopee_live) if show_shopee else pd.DataFrame()
shopee_video_filtered = filter_by_date(shopee_video) if show_shopee else pd.DataFrame()

# ==========================================
# HEADER
# ==========================================
st.markdown('<p class="main-header">Content Commerce Analytics</p>', unsafe_allow_html=True)
st.markdown(f"**Period:** {start_date} to {end_date} | TikTok vs Shopee Content Performance")

# ==========================================
# SECTION 1: PLATFORM OVERVIEW
# ==========================================
st.markdown('<p class="section-header">📊 Platform Overview</p>', unsafe_allow_html=True)

# Calculate platform totals
def calc_platform_metrics(live_df, video_df, platform_name):
    """Calculate aggregate metrics for a platform"""
    metrics = {
        'platform': platform_name,
        'live_gmv': live_df['GMV'].apply(safe_float).sum() if not live_df.empty and 'GMV' in live_df.columns else 0,
        'live_orders': live_df['Orders'].apply(safe_float).sum() if not live_df.empty and 'Orders' in live_df.columns else 0,
        'live_viewers': live_df['Viewers'].apply(safe_float).sum() if not live_df.empty and 'Viewers' in live_df.columns else 0,
        'live_sessions': len(live_df),
        'video_gmv': video_df['GMV'].apply(safe_float).sum() if not video_df.empty and 'GMV' in video_df.columns else 0,
        'video_orders': video_df['Orders'].apply(safe_float).sum() if not video_df.empty and 'Orders' in video_df.columns else 0,
        'video_views': video_df['Views'].apply(safe_float).sum() if not video_df.empty and 'Views' in video_df.columns else 0,
        'video_count': len(video_df),
    }
    metrics['total_gmv'] = metrics['live_gmv'] + metrics['video_gmv']
    metrics['total_orders'] = metrics['live_orders'] + metrics['video_orders']
    return metrics

tiktok_metrics = calc_platform_metrics(tiktok_live_filtered, tiktok_video_filtered, 'TikTok')
shopee_metrics = calc_platform_metrics(shopee_live_filtered, shopee_video_filtered, 'Shopee')

# Display platform comparison
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎵 TikTok")
    if tiktok_metrics['total_gmv'] > 0 or tiktok_metrics['live_sessions'] > 0:
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total GMV", format_currency(tiktok_metrics['total_gmv']))
        with m2:
            st.metric("Total Orders", format_number(tiktok_metrics['total_orders']))
        with m3:
            st.metric("Live Sessions", format_number(tiktok_metrics['live_sessions']))

        m4, m5, m6 = st.columns(3)
        with m4:
            st.metric("Live GMV", format_currency(tiktok_metrics['live_gmv']))
        with m5:
            st.metric("Video GMV", format_currency(tiktok_metrics['video_gmv']))
        with m6:
            st.metric("Videos", format_number(tiktok_metrics['video_count']))
    else:
        st.info("No TikTok data for selected period")

with col2:
    st.markdown("### 🛒 Shopee Content")
    if shopee_metrics['total_gmv'] > 0 or shopee_metrics['live_sessions'] > 0:
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total GMV", format_currency(shopee_metrics['total_gmv']))
        with m2:
            st.metric("Total Orders", format_number(shopee_metrics['total_orders']))
        with m3:
            st.metric("Live Sessions", format_number(shopee_metrics['live_sessions']))

        m4, m5, m6 = st.columns(3)
        with m4:
            st.metric("Live GMV", format_currency(shopee_metrics['live_gmv']))
        with m5:
            st.metric("Video GMV", format_currency(shopee_metrics['video_gmv']))
        with m6:
            st.metric("Videos", format_number(shopee_metrics['video_count']))
    else:
        st.info("No Shopee content data for selected period")

st.markdown("---")

# ==========================================
# SECTION 2: LIVE STREAMING ANALYSIS
# ==========================================
st.markdown('<p class="section-header">🎬 Live Streaming Analysis</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎵 TikTok Live")

    if not tiktok_live_filtered.empty:
        # Summary metrics
        total_gmv = tiktok_live_filtered['GMV'].apply(safe_float).sum()
        total_orders = tiktok_live_filtered['Orders'].apply(safe_float).sum()
        total_viewers = tiktok_live_filtered['Viewers'].apply(safe_float).sum()
        total_likes = tiktok_live_filtered['Likes'].apply(safe_float).sum() if 'Likes' in tiktok_live_filtered.columns else 0

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("GMV", format_currency(total_gmv))
        with m2:
            st.metric("Orders", format_number(total_orders))
        with m3:
            st.metric("Viewers", format_number(total_viewers))
        with m4:
            st.metric("Likes", format_number(total_likes))

        # Top performing sessions
        if 'GMV' in tiktok_live_filtered.columns:
            top_live = tiktok_live_filtered.nlargest(10, 'GMV')[['Creator', 'GMV', 'Orders', 'Viewers', 'Duration_Minutes']].copy() if 'Creator' in tiktok_live_filtered.columns else tiktok_live_filtered.nlargest(10, 'GMV')
            if 'Duration_Minutes' in top_live.columns:
                top_live['Duration_Minutes'] = top_live['Duration_Minutes'].apply(lambda x: f"{safe_float(x):.0f}m")
            top_live['GMV'] = top_live['GMV'].apply(format_currency)
            top_live['Orders'] = top_live['Orders'].apply(format_number)
            top_live['Viewers'] = top_live['Viewers'].apply(format_number)

            st.markdown("**Top 10 Sessions by GMV**")
            st.dataframe(top_live, use_container_width=True, hide_index=True)
    else:
        st.info("No TikTok Live data for selected period")

with col2:
    st.markdown("### 🛒 Shopee Live")

    if not shopee_live_filtered.empty:
        # Find sales/orders columns
        sales_col = [c for c in shopee_live_filtered.columns if 'Sales_Confirmed' in c or 'Sales' in c]
        orders_col = [c for c in shopee_live_filtered.columns if 'Orders_Confirmed' in c or 'Orders' in c]
        viewers_col = [c for c in shopee_live_filtered.columns if 'Viewers' in c]

        total_gmv = shopee_live_filtered[sales_col[0]].apply(safe_float).sum() if sales_col else 0
        total_orders = shopee_live_filtered[orders_col[0]].apply(safe_float).sum() if orders_col else 0
        total_viewers = shopee_live_filtered[viewers_col[0]].apply(safe_float).sum() if viewers_col else 0

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("GMV", format_currency(total_gmv))
        with m2:
            st.metric("Orders", format_number(total_orders))
        with m3:
            st.metric("Viewers", format_number(total_viewers))

        # Show available data
        st.markdown("**Session Data**")
        display_cols = [c for c in shopee_live_filtered.columns if c not in ['File_Source', 'Report_Period']]
        st.dataframe(shopee_live_filtered[display_cols].head(10), use_container_width=True, hide_index=True)
    else:
        st.info("No Shopee Live data for selected period")

st.markdown("---")

# ==========================================
# SECTION 3: VIDEO PERFORMANCE
# ==========================================
st.markdown('<p class="section-header">🎥 Video Performance</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎵 TikTok Video")

    if not tiktok_video_filtered.empty:
        # Summary metrics
        total_gmv = tiktok_video_filtered['GMV'].apply(safe_float).sum()
        total_orders = tiktok_video_filtered['Orders'].apply(safe_float).sum()
        total_views = tiktok_video_filtered['Views'].apply(safe_float).sum()
        total_likes = tiktok_video_filtered['Likes'].apply(safe_float).sum() if 'Likes' in tiktok_video_filtered.columns else 0
        avg_engagement = tiktok_video_filtered['Engagement_Rate'].mean() if 'Engagement_Rate' in tiktok_video_filtered.columns else 0

        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.metric("GMV", format_currency(total_gmv))
        with m2:
            st.metric("Orders", format_number(total_orders))
        with m3:
            st.metric("Views", format_number(total_views))
        with m4:
            st.metric("Likes", format_number(total_likes))
        with m5:
            st.metric("Avg Engagement", format_percent(avg_engagement))

        # Top performing videos
        if 'GMV' in tiktok_video_filtered.columns:
            top_videos = tiktok_video_filtered.nlargest(10, 'GMV').copy()

            # Select display columns
            display_cols = ['Creator', 'Video_Title', 'GMV', 'Orders', 'Views', 'Engagement_Rate']
            available_cols = [c for c in display_cols if c in top_videos.columns]
            top_videos_display = top_videos[available_cols].copy()

            # Format columns
            if 'GMV' in top_videos_display.columns:
                top_videos_display['GMV'] = top_videos_display['GMV'].apply(format_currency)
            if 'Orders' in top_videos_display.columns:
                top_videos_display['Orders'] = top_videos_display['Orders'].apply(format_number)
            if 'Views' in top_videos_display.columns:
                top_videos_display['Views'] = top_videos_display['Views'].apply(format_number)
            if 'Engagement_Rate' in top_videos_display.columns:
                top_videos_display['Engagement_Rate'] = top_videos_display['Engagement_Rate'].apply(format_percent)
            if 'Video_Title' in top_videos_display.columns:
                top_videos_display['Video_Title'] = top_videos_display['Video_Title'].apply(lambda x: str(x)[:50] + '...' if len(str(x)) > 50 else x)

            st.markdown("**Top 10 Videos by GMV**")
            st.dataframe(top_videos_display, use_container_width=True, hide_index=True)

        # Engagement distribution
        if 'Engagement_Rate' in tiktok_video_filtered.columns and total_views > 0:
            st.markdown("**Engagement Rate Distribution**")
            fig = px.histogram(tiktok_video_filtered, x='Engagement_Rate', nbins=20,
                             title="Video Engagement Rate Distribution",
                             labels={'Engagement_Rate': 'Engagement Rate'})
            fig.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No TikTok Video data for selected period")

with col2:
    st.markdown("### 🛒 Shopee Video")

    if not shopee_video_filtered.empty:
        # Find columns
        sales_col = [c for c in shopee_video_filtered.columns if 'Sales_Confirmed' in c or 'Sales' in c]
        orders_col = [c for c in shopee_video_filtered.columns if 'Orders_Confirmed' in c or 'Orders' in c]
        views_col = [c for c in shopee_video_filtered.columns if 'Views' in c or 'เข้าชม' in c]

        total_gmv = shopee_video_filtered[sales_col[0]].apply(safe_float).sum() if sales_col else 0
        total_orders = shopee_video_filtered[orders_col[0]].apply(safe_float).sum() if orders_col else 0
        total_views = shopee_video_filtered[views_col[0]].apply(safe_float).sum() if views_col else 0

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("GMV", format_currency(total_gmv))
        with m2:
            st.metric("Orders", format_number(total_orders))
        with m3:
            st.metric("Views", format_number(total_views))

        # Show available data
        st.markdown("**Video Data**")
        display_cols = [c for c in shopee_video_filtered.columns if c not in ['File_Source', 'Report_Period']]
        st.dataframe(shopee_video_filtered[display_cols].head(10), use_container_width=True, hide_index=True)
    else:
        st.info("No Shopee Video data for selected period")

st.markdown("---")

# ==========================================
# SECTION 4: PLATFORM COMPARISON CHARTS
# ==========================================
st.markdown('<p class="section-header">📈 Platform Comparison</p>', unsafe_allow_html=True)

# Create comparison charts
col1, col2 = st.columns(2)

with col1:
    # GMV by Platform
    gmv_data = {
        'Platform': [],
        'Type': [],
        'GMV': []
    }

    if show_tiktok:
        gmv_data['Platform'].extend(['TikTok', 'TikTok'])
        gmv_data['Type'].extend(['Live', 'Video'])
        gmv_data['GMV'].extend([tiktok_metrics['live_gmv'], tiktok_metrics['video_gmv']])

    if show_shopee:
        gmv_data['Platform'].extend(['Shopee', 'Shopee'])
        gmv_data['Type'].extend(['Live', 'Video'])
        gmv_data['GMV'].extend([shopee_metrics['live_gmv'], shopee_metrics['video_gmv']])

    if gmv_data['GMV'] and sum(gmv_data['GMV']) > 0:
        fig = px.bar(gmv_data, x='Platform', y='GMV', color='Type',
                    title="GMV by Platform and Content Type",
                    barmode='group',
                    color_discrete_map={'Live': '#FF0050', 'Video': '#00F2EA'})
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No GMV data for comparison")

with col2:
    # Orders by Platform
    orders_data = {
        'Platform': [],
        'Type': [],
        'Orders': []
    }

    if show_tiktok:
        orders_data['Platform'].extend(['TikTok', 'TikTok'])
        orders_data['Type'].extend(['Live', 'Video'])
        orders_data['Orders'].extend([tiktok_metrics['live_orders'], tiktok_metrics['video_orders']])

    if show_shopee:
        orders_data['Platform'].extend(['Shopee', 'Shopee'])
        orders_data['Type'].extend(['Live', 'Video'])
        orders_data['Orders'].extend([shopee_metrics['live_orders'], shopee_metrics['video_orders']])

    if orders_data['Orders'] and sum(orders_data['Orders']) > 0:
        fig = px.bar(orders_data, x='Platform', y='Orders', color='Type',
                    title="Orders by Platform and Content Type",
                    barmode='group',
                    color_discrete_map={'Live': '#FF0050', 'Video': '#00F2EA'})
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No orders data for comparison")

st.markdown("---")

# ==========================================
# SECTION 5: TOP PERFORMERS
# ==========================================
st.markdown('<p class="section-header">🏆 Top Performers</p>', unsafe_allow_html=True)

tab1, tab2 = st.tabs(["TikTok Creators", "Content Performance"])

with tab1:
    if not tiktok_live_filtered.empty or not tiktok_video_filtered.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Top Live Creators**")
            if not tiktok_live_filtered.empty and 'Creator' in tiktok_live_filtered.columns:
                creator_stats = tiktok_live_filtered.groupby('Creator').agg({
                    'GMV': 'sum',
                    'Orders': 'sum',
                    'Viewers': 'sum',
                    'Likes': 'sum'
                }).reset_index() if 'GMV' in tiktok_live_filtered.columns else pd.DataFrame()

                if not creator_stats.empty:
                    creator_stats = creator_stats.nlargest(10, 'GMV')
                    creator_stats['GMV'] = creator_stats['GMV'].apply(format_currency)
                    creator_stats['Orders'] = creator_stats['Orders'].apply(format_number)
                    creator_stats['Viewers'] = creator_stats['Viewers'].apply(format_number)
                    st.dataframe(creator_stats, use_container_width=True, hide_index=True)
            else:
                st.info("No creator data available")

        with col2:
            st.markdown("**Top Video Creators**")
            if not tiktok_video_filtered.empty and 'Creator' in tiktok_video_filtered.columns:
                video_creator_stats = tiktok_video_filtered.groupby('Creator').agg({
                    'GMV': 'sum',
                    'Orders': 'sum',
                    'Views': 'sum',
                    'Engagement_Rate': 'mean'
                }).reset_index() if 'GMV' in tiktok_video_filtered.columns else pd.DataFrame()

                if not video_creator_stats.empty:
                    video_creator_stats = video_creator_stats.nlargest(10, 'GMV')
                    video_creator_stats['GMV'] = video_creator_stats['GMV'].apply(format_currency)
                    video_creator_stats['Orders'] = video_creator_stats['Orders'].apply(format_number)
                    video_creator_stats['Views'] = video_creator_stats['Views'].apply(format_number)
                    video_creator_stats['Engagement_Rate'] = video_creator_stats['Engagement_Rate'].apply(format_percent)
                    st.dataframe(video_creator_stats, use_container_width=True, hide_index=True)
            else:
                st.info("No creator data available")
    else:
        st.info("No TikTok data for selected period")

with tab2:
    if not tiktok_video_filtered.empty:
        st.markdown("**Top Videos by Engagement**")
        if 'Engagement_Rate' in tiktok_video_filtered.columns:
            top_engagement = tiktok_video_filtered.nlargest(15, 'Engagement_Rate').copy()

            display_cols = ['Creator', 'Video_Title', 'Views', 'Likes', 'Comments', 'Engagement_Rate']
            available_cols = [c for c in display_cols if c in top_engagement.columns]
            top_engagement_display = top_engagement[available_cols].copy()

            # Format
            for col in ['Views', 'Likes', 'Comments']:
                if col in top_engagement_display.columns:
                    top_engagement_display[col] = top_engagement_display[col].apply(format_number)
            if 'Engagement_Rate' in top_engagement_display.columns:
                top_engagement_display['Engagement_Rate'] = top_engagement_display['Engagement_Rate'].apply(format_percent)
            if 'Video_Title' in top_engagement_display.columns:
                top_engagement_display['Video_Title'] = top_engagement_display['Video_Title'].apply(lambda x: str(x)[:60] + '...' if len(str(x)) > 60 else x)

            st.dataframe(top_engagement_display, use_container_width=True, hide_index=True)
        else:
            st.info("No engagement data available")
    else:
        st.info("No TikTok video data for selected period")

st.markdown("---")

# ==========================================
# SECTION 6: DAILY TRENDS
# ==========================================
st.markdown('<p class="section-header">📅 Daily Trends</p>', unsafe_allow_html=True)

# Aggregate daily data
def get_daily_trends(df, date_col='Report_Date', value_cols=['GMV', 'Orders', 'Views']):
    if df.empty or date_col not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    agg_dict = {}
    for col in value_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'

    if not agg_dict:
        return pd.DataFrame()

    daily = df.groupby(date_col).agg(agg_dict).reset_index()
    return daily

tiktok_daily_live = get_daily_trends(tiktok_live_filtered)
tiktok_daily_video = get_daily_trends(tiktok_video_filtered)

if not tiktok_daily_live.empty or not tiktok_daily_video.empty:
    fig = go.Figure()

    if not tiktok_daily_live.empty and 'GMV' in tiktok_daily_live.columns:
        fig.add_trace(go.Scatter(
            x=tiktok_daily_live['Report_Date'],
            y=tiktok_daily_live['GMV'],
            name='TikTok Live GMV',
            line=dict(color='#FF0050', width=2)
        ))

    if not tiktok_daily_video.empty and 'GMV' in tiktok_daily_video.columns:
        fig.add_trace(go.Scatter(
            x=tiktok_daily_video['Report_Date'],
            y=tiktok_daily_video['GMV'],
            name='TikTok Video GMV',
            line=dict(color='#00F2EA', width=2)
        ))

    fig.update_layout(
        title="Daily GMV Trends",
        xaxis_title="Date",
        yaxis_title="GMV (฿)",
        hovermode='x unified',
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", y=1.1)
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No trend data available for selected period")

# ==========================================
# FOOTER
# ==========================================
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.85rem;">
        <p>Content Commerce Dashboard v1.0 | TikTok + Shopee Analytics</p>
        <p>Powered by DuckDB & Streamlit</p>
    </div>
    """, unsafe_allow_html=True)
