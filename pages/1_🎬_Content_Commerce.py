"""
Content Commerce Analysis Page
Live Streaming & Video Performance with Platform Filtering
Supports: Shopee (monthly aggregated) and TikTok (session-level)
With Gemini AI-powered insights
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path
import google.generativeai as genai

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from utils import (
    safe_float, format_currency, format_number, load_live_data, load_video_data,
    load_tiktok_live, load_tiktok_video, COMMON_STYLES, load_daily_sales
)

# ==========================================
# GEMINI AI CONFIGURATION
# ==========================================
import os
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.0-flash') if GEMINI_API_KEY else None

def get_gemini_insights(prompt, max_retries=2):
    """Get AI insights from Gemini with error handling"""
    if not gemini_model:
        return "⚠️ AI analysis unavailable: GEMINI_API_KEY not configured. Set the environment variable to enable AI insights."
    try:
        response = gemini_model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.7,
                max_output_tokens=500,
            )
        )
        return response.text
    except Exception as e:
        return f"⚠️ AI analysis temporarily unavailable: {str(e)[:100]}"

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="Content Commerce - Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply common styles
st.markdown(COMMON_STYLES, unsafe_allow_html=True)

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 🎬 Content Commerce")
    st.markdown("---")

    # Load data for date range
    daily_df = load_daily_sales()
    min_date = pd.to_datetime(daily_df['Date']).min().date()
    max_date = pd.to_datetime(daily_df['Date']).max().date()

    # Quick time range selection
    st.markdown("#### ⚡ Quick Select")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Today", use_container_width=True):
            st.session_state.date_range = (max_date, max_date)
        if st.button("This Week", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=7), max_date)
        if st.button("Q1", use_container_width=True):
            q1_end = min(datetime(max_date.year, 3, 31).date(), max_date)
            st.session_state.date_range = (datetime(max_date.year, 1, 1).date(), q1_end)
        if st.button("Q3", use_container_width=True):
            q3_start = max(datetime(max_date.year, 7, 1).date(), min_date)
            q3_end = min(datetime(max_date.year, 9, 30).date(), max_date)
            if q3_start <= q3_end:
                st.session_state.date_range = (q3_start, q3_end)

    with col2:
        if st.button("This Month", use_container_width=True):
            st.session_state.date_range = (datetime(max_date.year, max_date.month, 1).date(), max_date)
        if st.button("This Year", use_container_width=True):
            st.session_state.date_range = (datetime(max_date.year, 1, 1).date(), max_date)
        if st.button("Q2", use_container_width=True):
            q2_end = min(datetime(max_date.year, 6, 30).date(), max_date)
            st.session_state.date_range = (datetime(max_date.year, 4, 1).date(), q2_end)
        if st.button("Q4", use_container_width=True):
            q4_start = max(datetime(max_date.year, 10, 1).date(), min_date)
            q4_end = max_date
            if q4_start <= q4_end:
                st.session_state.date_range = (q4_start, q4_end)

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

    # Platform selection
    st.markdown("#### 🏪 Platform Selection")
    platform = st.selectbox(
        "Select Platform",
        options=["All", "Shopee", "TikTok"],
        index=0,
        label_visibility="collapsed"
    )

    st.markdown("---")

    # Info
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.markdown(f"**Data Range:** {min_date} to {max_date}")

    st.markdown("---")

    # Navigation hint
    st.markdown("📌 **Navigation**")
    st.page_link("dashboard.py", label="📊 Main Dashboard", icon="📊")
    st.page_link("pages/1_🎬_Content_Commerce.py", label="🎬 Content Commerce", icon="🎬")

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# PROCESS DATE RANGE
# ==========================================
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

# ==========================================
# HEADER
# ==========================================
st.markdown('<p class="main-header">🎬 Content Commerce Analysis</p>', unsafe_allow_html=True)
st.markdown(f"**Period:** {start_date} to {end_date} | **Platform:** {platform}")
st.caption("Live Streaming & Video performance analysis with Shopee (monthly) and TikTok (session-level) data")

# ==========================================
# LOAD AND FILTER DATA
# ==========================================
# Load Shopee data
shopee_live_raw = load_live_data()
shopee_video_raw = load_video_data()

# Load TikTok data
tiktok_live_raw = load_tiktok_live()
tiktok_video_raw = load_tiktok_video()

# Filter Shopee by date range
shopee_live = pd.DataFrame()
shopee_video = pd.DataFrame()

if not shopee_live_raw.empty and 'Report_Date' in shopee_live_raw.columns:
    shopee_live_raw['Report_Date'] = pd.to_datetime(shopee_live_raw['Report_Date'])
    mask = (shopee_live_raw['Report_Date'] >= pd.to_datetime(start_date)) & (shopee_live_raw['Report_Date'] <= pd.to_datetime(end_date))
    shopee_live = shopee_live_raw[mask].copy()

if not shopee_video_raw.empty and 'Report_Date' in shopee_video_raw.columns:
    shopee_video_raw['Report_Date'] = pd.to_datetime(shopee_video_raw['Report_Date'])
    mask = (shopee_video_raw['Report_Date'] >= pd.to_datetime(start_date)) & (shopee_video_raw['Report_Date'] <= pd.to_datetime(end_date))
    shopee_video = shopee_video_raw[mask].copy()

# Filter TikTok by date range
tiktok_live = pd.DataFrame()
tiktok_video = pd.DataFrame()

if not tiktok_live_raw.empty and 'Start_Time' in tiktok_live_raw.columns:
    tiktok_live_raw['Start_DateTime'] = pd.to_datetime(tiktok_live_raw['Start_Time'], errors='coerce')
    mask = (tiktok_live_raw['Start_DateTime'] >= pd.to_datetime(start_date)) & (tiktok_live_raw['Start_DateTime'] <= pd.to_datetime(end_date))
    tiktok_live = tiktok_live_raw[mask].copy()

if not tiktok_video_raw.empty and 'Post_Time' in tiktok_video_raw.columns:
    tiktok_video_raw['Post_DateTime'] = pd.to_datetime(tiktok_video_raw['Post_Time'], errors='coerce')
    mask = (tiktok_video_raw['Post_DateTime'] >= pd.to_datetime(start_date)) & (tiktok_video_raw['Post_DateTime'] <= pd.to_datetime(end_date))
    tiktok_video = tiktok_video_raw[mask].copy()

# Calculate metrics based on platform selection
def calc_shopee_live_metrics(df):
    if df.empty:
        return {'sales': 0, 'orders': 0, 'viewers': 0, 'duration': 0, 'peak': 0, 'followers': 0, 'likes': 0, 'shares': 0, 'comments': 0}
    return {
        'sales': df['Sales_Confirmed'].apply(safe_float).sum() if 'Sales_Confirmed' in df.columns else 0,
        'orders': df['Orders_Confirmed'].apply(safe_float).sum() if 'Orders_Confirmed' in df.columns else 0,
        'viewers': df['Total_Viewers'].apply(safe_float).sum() if 'Total_Viewers' in df.columns else 0,
        'duration': df['Live_Duration_Hours'].apply(safe_float).sum() if 'Live_Duration_Hours' in df.columns else 0,
        'peak': df['Peak_Concurrent_Users'].apply(safe_float).max() if 'Peak_Concurrent_Users' in df.columns else 0,
        'followers': df['การมีส่วนร่วม_ผู้ติดตามใหม่จาก Live'].apply(safe_float).sum() if 'การมีส่วนร่วม_ผู้ติดตามใหม่จาก Live' in df.columns else 0,
        'likes': df['การมีส่วนร่วม_การถูกใจทั้งหมด'].apply(safe_float).sum() if 'การมีส่วนร่วม_การถูกใจทั้งหมด' in df.columns else 0,
        'shares': df['การมีส่วนร่วม_แชร์ทั้งหมด'].apply(safe_float).sum() if 'การมีส่วนร่วม_แชร์ทั้งหมด' in df.columns else 0,
        'comments': df['การมีส่วนร่วม_คอมเมนต์ทั้งหมด'].apply(safe_float).sum() if 'การมีส่วนร่วม_คอมเมนต์ทั้งหมด' in df.columns else 0,
    }

def calc_shopee_video_metrics(df):
    if df.empty:
        return {'sales': 0, 'orders': 0, 'views': 0, 'videos_with_products': 0, 'revenue_videos': 0, 'followers': 0, 'likes': 0, 'shares': 0, 'comments': 0}
    return {
        'sales': df['Video_Sales_Confirmed'].apply(safe_float).sum() if 'Video_Sales_Confirmed' in df.columns else 0,
        'orders': df['Video_Orders_Confirmed'].apply(safe_float).sum() if 'Video_Orders_Confirmed' in df.columns else 0,
        'views': df['Total_Views'].apply(safe_float).sum() if 'Total_Views' in df.columns else 0,
        'videos_with_products': df['Videos_With_Products'].apply(safe_float).sum() if 'Videos_With_Products' in df.columns else 0,
        'revenue_videos': df['Revenue_Generating_Videos'].apply(safe_float).sum() if 'Revenue_Generating_Videos' in df.columns else 0,
        'followers': df['New_Followers'].apply(safe_float).sum() if 'New_Followers' in df.columns else 0,
        'likes': df['Total_Likes'].apply(safe_float).sum() if 'Total_Likes' in df.columns else 0,
        'shares': df['Total_Shares'].apply(safe_float).sum() if 'Total_Shares' in df.columns else 0,
        'comments': df['Total_Comments'].apply(safe_float).sum() if 'Total_Comments' in df.columns else 0,
    }

def calc_tiktok_live_metrics(df):
    if df.empty:
        return {'sales': 0, 'orders': 0, 'viewers': 0, 'views': 0, 'duration': 0, 'followers': 0, 'likes': 0, 'shares': 0, 'comments': 0, 'sessions': 0}
    return {
        'sales': df['GMV'].apply(safe_float).sum() if 'GMV' in df.columns else 0,
        'orders': df['Orders'].apply(safe_float).sum() if 'Orders' in df.columns else 0,
        'viewers': df['Viewers'].apply(safe_float).sum() if 'Viewers' in df.columns else 0,
        'views': df['Views'].apply(safe_float).sum() if 'Views' in df.columns else 0,
        'duration': df['Duration_Hours'].apply(safe_float).sum() if 'Duration_Hours' in df.columns else 0,
        'followers': df['New_Followers'].apply(safe_float).sum() if 'New_Followers' in df.columns else 0,
        'likes': df['Likes'].apply(safe_float).sum() if 'Likes' in df.columns else 0,
        'shares': df['Shares'].apply(safe_float).sum() if 'Shares' in df.columns else 0,
        'comments': df['Comments'].apply(safe_float).sum() if 'Comments' in df.columns else 0,
        'sessions': len(df),
    }

def calc_tiktok_video_metrics(df):
    if df.empty:
        return {'sales': 0, 'orders': 0, 'views': 0, 'followers': 0, 'likes': 0, 'shares': 0, 'comments': 0, 'videos': 0}
    return {
        'sales': df['GMV'].apply(safe_float).sum() if 'GMV' in df.columns else 0,
        'orders': df['Orders'].apply(safe_float).sum() if 'Orders' in df.columns else 0,
        'views': df['Views'].apply(safe_float).sum() if 'Views' in df.columns else 0,
        'followers': df['New_Followers'].apply(safe_float).sum() if 'New_Followers' in df.columns else 0,
        'likes': df['Likes'].apply(safe_float).sum() if 'Likes' in df.columns else 0,
        'shares': df['Shares'].apply(safe_float).sum() if 'Shares' in df.columns else 0,
        'comments': df['Comments'].apply(safe_float).sum() if 'Comments' in df.columns else 0,
        'videos': len(df),
    }

# Calculate metrics
shopee_live_m = calc_shopee_live_metrics(shopee_live)
shopee_video_m = calc_shopee_video_metrics(shopee_video)
tiktok_live_m = calc_tiktok_live_metrics(tiktok_live)
tiktok_video_m = calc_tiktok_video_metrics(tiktok_video)

# Aggregate based on platform
if platform == "Shopee":
    live_m = shopee_live_m
    video_m = shopee_video_m
    live_df = shopee_live
    video_df = shopee_video
elif platform == "TikTok":
    live_m = tiktok_live_m
    video_m = tiktok_video_m
    live_df = tiktok_live
    video_df = tiktok_video
else:  # All
    live_m = {
        'sales': shopee_live_m['sales'] + tiktok_live_m['sales'],
        'orders': shopee_live_m['orders'] + tiktok_live_m['orders'],
        'viewers': shopee_live_m['viewers'] + tiktok_live_m['viewers'],
        'duration': shopee_live_m['duration'] + tiktok_live_m['duration'],
        'followers': shopee_live_m['followers'] + tiktok_live_m['followers'],
        'likes': shopee_live_m['likes'] + tiktok_live_m['likes'],
        'shares': shopee_live_m['shares'] + tiktok_live_m['shares'],
        'comments': shopee_live_m['comments'] + tiktok_live_m['comments'],
        'views': tiktok_live_m.get('views', 0),
        'sessions': tiktok_live_m.get('sessions', 0),
        'peak': shopee_live_m.get('peak', 0),
    }
    video_m = {
        'sales': shopee_video_m['sales'] + tiktok_video_m['sales'],
        'orders': shopee_video_m['orders'] + tiktok_video_m['orders'],
        'views': shopee_video_m['views'] + tiktok_video_m['views'],
        'followers': shopee_video_m['followers'] + tiktok_video_m['followers'],
        'likes': shopee_video_m['likes'] + tiktok_video_m['likes'],
        'shares': shopee_video_m['shares'] + tiktok_video_m['shares'],
        'comments': shopee_video_m['comments'] + tiktok_video_m['comments'],
        'videos_with_products': shopee_video_m.get('videos_with_products', 0),
        'revenue_videos': shopee_video_m.get('revenue_videos', 0),
        'videos': tiktok_video_m.get('videos', 0),
    }

# ==========================================
# KPIs SUMMARY
# ==========================================
total_sales = live_m['sales'] + video_m['sales']
total_orders = live_m['orders'] + video_m['orders']
total_views = live_m.get('views', live_m.get('viewers', 0)) + video_m['views']
total_followers = live_m['followers'] + video_m['followers']

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Total Content Sales", format_currency(total_sales))
with k2:
    st.metric("Total Orders", format_number(total_orders))
with k3:
    st.metric("Live Hours", f"{live_m['duration']:.1f}h")
with k4:
    st.metric("Total Views", format_number(total_views))
with k5:
    st.metric("New Followers", format_number(total_followers))

st.markdown("---")

# ==========================================
# PLATFORM COMPARISON (if All selected)
# ==========================================
if platform == "All":
    st.markdown("**📊 Platform Comparison**")
    comp1, comp2, comp3, comp4 = st.columns(4)
    with comp1:
        st.metric("Shopee Live Sales", format_currency(shopee_live_m['sales']))
    with comp2:
        st.metric("TikTok Live Sales", format_currency(tiktok_live_m['sales']))
    with comp3:
        st.metric("Shopee Video Sales", format_currency(shopee_video_m['sales']))
    with comp4:
        st.metric("TikTok Video Sales", format_currency(tiktok_video_m['sales']))
    st.markdown("---")

# ==========================================
# LIVE STREAMING SECTION
# ==========================================
st.markdown('<p class="section-header">📺 Live Streaming</p>', unsafe_allow_html=True)

col_live1, col_live2 = st.columns(2)

with col_live1:
    st.markdown("**Metrics Summary**")
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Sales", format_currency(live_m['sales']))
    with m2:
        st.metric("Orders", format_number(live_m['orders']))
    with m3:
        st.metric("Viewers", format_number(live_m.get('viewers', live_m.get('views', 0))))

    m4, m5, m6 = st.columns(3)
    with m4:
        st.metric("Live Hours", f"{live_m['duration']:.1f}h")
    with m5:
        st.metric("New Followers", format_number(live_m['followers']))
    with m6:
        conv = (live_m['orders'] / max(live_m.get('viewers', 1), 1) * 100) if live_m.get('viewers', 0) > 0 else 0
        st.metric("Conversion", f"{conv:.2f}%")

with col_live2:
    st.markdown("**Engagement**")
    if live_m['likes'] + live_m['shares'] + live_m['comments'] > 0:
        fig_eng = go.Figure(data=[go.Pie(
            labels=['Likes', 'Shares', 'Comments'],
            values=[live_m['likes'], live_m['shares'], live_m['comments']],
            hole=0.4,
            marker_colors=['#ee4d2d', '#0066cc', '#28a745']
        )])
        fig_eng.update_layout(
            title="Live Engagement Distribution",
            height=250,
            margin=dict(l=0, r=0, t=40, b=0)
        )
        st.plotly_chart(fig_eng, use_container_width=True)
    else:
        st.info("No engagement data available")

# TikTok Live Sessions Table (if TikTok data available)
if platform in ["TikTok", "All"] and not tiktok_live.empty:
    st.markdown("**🎬 TikTok Live Sessions (Top 10 by GMV)**")

    # Deduplicate by session (Start_Time + Creator combination)
    agg_dict = {
        'GMV': 'sum',
        'Orders': 'sum',
        'Viewers': 'sum',
        'Views': 'sum',
        'Likes': 'sum',
        'New_Followers': 'sum',
        'Duration': 'first',
        'Creator': 'first',
        'Nickname': 'first',
    }
    available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_live.columns}

    if 'Start_Time' in tiktok_live.columns and 'Creator' in tiktok_live.columns:
        deduped_live = tiktok_live.groupby(['Start_Time', 'Creator'], as_index=False).agg(available_agg)
    else:
        deduped_live = tiktok_live

    display_cols = ['Start_Time', 'Creator', 'Duration', 'GMV', 'Orders', 'Viewers', 'Views', 'Likes', 'New_Followers']
    available_cols = [c for c in display_cols if c in deduped_live.columns]
    # Convert GMV to numeric for proper sorting
    if 'GMV' in deduped_live.columns:
        deduped_live['GMV_Numeric'] = deduped_live['GMV'].apply(safe_float)
        top_sessions = deduped_live.nlargest(10, 'GMV_Numeric')[available_cols]
    else:
        top_sessions = deduped_live.head(10)[available_cols]
    st.dataframe(top_sessions, use_container_width=True, hide_index=True)
    st.caption(f"Showing top 10 of {len(deduped_live)} TikTok live sessions in selected period")

st.markdown("---")

# ==========================================
# VIDEO SECTION
# ==========================================
st.markdown('<p class="section-header">🎥 Video Performance</p>', unsafe_allow_html=True)

col_vid1, col_vid2 = st.columns(2)

with col_vid1:
    st.markdown("**Metrics Summary**")
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Sales", format_currency(video_m['sales']))
    with m2:
        st.metric("Orders", format_number(video_m['orders']))
    with m3:
        st.metric("Views", format_number(video_m['views']))

    m4, m5, m6 = st.columns(3)
    with m4:
        st.metric("Likes", format_number(video_m['likes']))
    with m5:
        st.metric("Shares", format_number(video_m['shares']))
    with m6:
        conv = (video_m['orders'] / max(video_m['views'], 1) * 100) if video_m['views'] > 0 else 0
        st.metric("Conversion", f"{conv:.2f}%")

with col_vid2:
    st.markdown("**Engagement**")
    if video_m['likes'] + video_m['shares'] + video_m['comments'] > 0:
        fig_eng = go.Figure(data=[go.Pie(
            labels=['Likes', 'Shares', 'Comments'],
            values=[video_m['likes'], video_m['shares'], video_m['comments']],
            hole=0.4,
            marker_colors=['#ee4d2d', '#0066cc', '#28a745']
        )])
        fig_eng.update_layout(
            title="Video Engagement Distribution",
            height=250,
            margin=dict(l=0, r=0, t=40, b=0)
        )
        st.plotly_chart(fig_eng, use_container_width=True)
    else:
        st.info("No engagement data available")

# TikTok Videos Table (if TikTok data available)
if platform in ["TikTok", "All"] and not tiktok_video.empty:
    st.markdown("**🎬 TikTok Videos (Top 10 by Views)**")

    # Deduplicate by Video_ID first
    agg_dict = {
        'Views': 'sum',
        'Likes': 'sum',
        'GMV': 'sum',
        'Orders': 'sum',
        'Creator': 'first',
        'Video_Title': 'first',
        'Post_Time': 'first',
    }
    available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_video.columns}

    if 'Video_ID' in tiktok_video.columns:
        deduped = tiktok_video.groupby('Video_ID', as_index=False).agg(available_agg)
    else:
        deduped = tiktok_video

    display_cols = ['Video_ID', 'Post_Time', 'Creator', 'Video_Title', 'Views', 'Likes', 'GMV', 'Orders']
    available_cols = [c for c in display_cols if c in deduped.columns]
    top_videos = deduped.nlargest(10, 'Views')[available_cols]
    # Truncate video title for display
    if 'Video_Title' in top_videos.columns:
        top_videos['Video_Title'] = top_videos['Video_Title'].apply(lambda x: str(x)[:50] + '...' if len(str(x)) > 50 else x)
    # Convert Video_ID to string to prevent scientific notation
    if 'Video_ID' in top_videos.columns:
        top_videos['Video_ID'] = top_videos['Video_ID'].apply(lambda x: str(int(safe_float(x))) if pd.notna(x) else 'N/A')
    st.dataframe(top_videos, use_container_width=True, hide_index=True)
    unique_count = tiktok_video['Video_ID'].nunique() if 'Video_ID' in tiktok_video.columns else len(tiktok_video)
    st.caption(f"Showing top 10 of {unique_count} unique TikTok videos in selected period")

st.markdown("---")

# ==========================================
# AD BOOST SCORING MODEL
# ==========================================
def calculate_ad_boost_score(df, content_type='video'):
    """
    Calculate Ad Boost Score using weighted metrics for marketing optimization.

    Video Scoring (no GMV data available):
    - Views (reach potential): 25%
    - Engagement Rate (audience quality): 30%
    - Conversion Rate (sales potential): 25%
    - Like Rate (content quality): 20%

    Live Scoring:
    - GMV (revenue): 35%
    - Engagement Rate: 25%
    - Conversion Rate: 25%
    - Viewer Growth Potential: 15%
    """
    import numpy as np

    if df.empty:
        return df

    df = df.copy()

    if content_type == 'video':
        # Normalize each metric to 0-100 scale
        views = df['Views'].apply(safe_float)
        eng_rate = df['Engagement_Rate'].apply(safe_float) if 'Engagement_Rate' in df.columns else pd.Series([0] * len(df))
        conv_rate = df['Conversion_Rate'].apply(safe_float) if 'Conversion_Rate' in df.columns else pd.Series([0] * len(df))
        likes = df['Likes'].apply(safe_float) if 'Likes' in df.columns else pd.Series([0] * len(df))

        # Normalize to 0-100
        views_norm = ((views - views.min()) / (views.max() - views.min() + 1)) * 100
        eng_norm = ((eng_rate - eng_rate.min()) / (eng_rate.max() - eng_rate.min() + 1)) * 100
        conv_norm = ((conv_rate - conv_rate.min()) / (conv_rate.max() - conv_rate.min() + 1)) * 100

        # Like rate (likes/views)
        like_rate = (likes / (views + 1)) * 100
        like_norm = ((like_rate - like_rate.min()) / (like_rate.max() - like_rate.min() + 1)) * 100

        # Weighted score
        df['Ad_Boost_Score'] = (
            views_norm * 0.25 +
            eng_norm * 0.30 +
            conv_norm * 0.25 +
            like_norm * 0.20
        )

    else:  # live
        gmv = df['GMV'].apply(safe_float) if 'GMV' in df.columns else pd.Series([0] * len(df))
        eng_rate = df['Engagement_Rate'].apply(safe_float) if 'Engagement_Rate' in df.columns else pd.Series([0] * len(df))
        viewers = df['Viewers'].apply(safe_float) if 'Viewers' in df.columns else pd.Series([0] * len(df))
        orders = df['Orders'].apply(safe_float) if 'Orders' in df.columns else pd.Series([0] * len(df))

        # Normalize
        gmv_norm = ((gmv - gmv.min()) / (gmv.max() - gmv.min() + 1)) * 100
        eng_norm = ((eng_rate - eng_rate.min()) / (eng_rate.max() - eng_rate.min() + 1)) * 100

        # Conversion rate (orders/viewers)
        conv_rate = (orders / (viewers + 1)) * 100
        conv_norm = ((conv_rate - conv_rate.min()) / (conv_rate.max() - conv_rate.min() + 1)) * 100

        # Viewer growth (using new followers if available)
        followers = df['New_Followers'].apply(safe_float) if 'New_Followers' in df.columns else pd.Series([0] * len(df))
        growth_rate = (followers / (viewers + 1)) * 100
        growth_norm = ((growth_rate - growth_rate.min()) / (growth_rate.max() - growth_rate.min() + 1)) * 100

        # Weighted score
        df['Ad_Boost_Score'] = (
            gmv_norm * 0.35 +
            eng_norm * 0.25 +
            conv_norm * 0.25 +
            growth_norm * 0.15
        )

    return df

# ==========================================
# BEST PERFORMING PERIODS (For Ad Boost)
# ==========================================
st.markdown("## 🏆 Best Performing Content (For Ad Boost Targeting)")
st.caption("Ranked using weighted scoring: Views (25%) + Engagement (30%) + Conversion (25%) + Quality (20%)")

# Create tabs for different views
if platform in ["TikTok", "All"] and not tiktok_video.empty:
    tab1, tab2, tab3 = st.tabs(["🎥 Top Videos for Ad Boost", "📺 Top Live Sessions", "📊 Scoring Methodology"])

    with tab1:
        st.markdown("**Top 10 TikTok Videos - Optimized for Ad Boost (AI Scored)**")

        # First, aggregate by Video_ID to deduplicate
        # Group by Video_ID and sum/aggregate metrics
        agg_dict = {
            'Views': 'sum',
            'Likes': 'sum',
            'Comments': 'sum',
            'Shares': 'sum',
            'Engagement_Rate': 'mean',
            'Conversion_Rate': 'mean',
            'Creator': 'first',
            'Nickname': 'first',
            'Video_Title': 'first',
        }
        # Only include columns that exist
        available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_video.columns}

        if 'Video_ID' in tiktok_video.columns:
            deduped_videos = tiktok_video.groupby('Video_ID', as_index=False).agg(available_agg)
        else:
            deduped_videos = tiktok_video

        # Calculate scores on deduplicated data
        scored_videos = calculate_ad_boost_score(deduped_videos, 'video')
        top_vids = scored_videos.nlargest(10, 'Ad_Boost_Score')

        # Create clean dataframe for display
        display_data = []
        for i, (_, row) in enumerate(top_vids.iterrows(), 1):
            # Convert Video_ID to full string (prevent scientific notation)
            video_id = str(int(safe_float(row.get('Video_ID', 0)))) if pd.notna(row.get('Video_ID')) else 'N/A'
            display_data.append({
                'Rank': i,
                'Score': f"{safe_float(row.get('Ad_Boost_Score', 0)):.1f}",
                'Video ID': video_id,
                'Username': str(row.get('Creator', 'Unknown'))[:20],
                'Title': str(row.get('Video_Title', 'Unknown'))[:35] + ('...' if len(str(row.get('Video_Title', ''))) > 35 else ''),
                'Views': format_number(safe_float(row.get('Views', 0))),
                'Eng Rate': f"{safe_float(row.get('Engagement_Rate', 0)):.1f}%",
                'Conv Rate': f"{safe_float(row.get('Conversion_Rate', 0)):.2f}%",
                'Likes': format_number(safe_float(row.get('Likes', 0))),
            })

        if display_data:
            df_display = pd.DataFrame(display_data)
            st.dataframe(df_display, use_container_width=True, hide_index=True)

            # Quick copy section
            st.markdown("---")
            st.markdown("**📋 Copy Video IDs for TikTok Ads Manager:**")
            video_ids = [d['Video ID'] for d in display_data[:5]]
            st.code(', '.join(video_ids), language='text')
            st.caption("Top 5 Video IDs - Paste into TikTok Ads Manager > Boost Post")

    with tab2:
        st.markdown("**Top 10 TikTok Live Sessions - Optimized for Ad Retargeting**")

        # First, aggregate by session to deduplicate
        agg_dict = {
            'GMV': 'sum',
            'Orders': 'sum',
            'Viewers': 'sum',
            'Views': 'sum',
            'Likes': 'sum',
            'New_Followers': 'sum',
            'Engagement_Rate': 'mean',
            'Duration': 'first',
            'Creator': 'first',
            'Nickname': 'first',
            'Start_Time': 'first',
        }
        available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_live.columns}

        if 'Start_Time' in tiktok_live.columns and 'Creator' in tiktok_live.columns:
            deduped_live = tiktok_live.groupby(['Start_Time', 'Creator'], as_index=False).agg(available_agg)
        else:
            deduped_live = tiktok_live

        # Calculate scores on deduplicated data
        scored_live = calculate_ad_boost_score(deduped_live, 'live')
        top_live = scored_live.nlargest(10, 'Ad_Boost_Score')

        display_data = []
        for i, (_, row) in enumerate(top_live.iterrows(), 1):
            display_data.append({
                'Rank': i,
                'Score': f"{safe_float(row.get('Ad_Boost_Score', 0)):.1f}",
                'Username': str(row.get('Nickname', row.get('Creator', 'Unknown')))[:20],
                'Start': str(row.get('Start_Time', 'N/A'))[:16],
                'Duration': str(row.get('Duration', 'N/A')),
                'GMV': format_currency(safe_float(row.get('GMV', 0))),
                'Orders': int(safe_float(row.get('Orders', 0))),
                'Viewers': format_number(safe_float(row.get('Viewers', 0))),
                'Eng Rate': f"{safe_float(row.get('Engagement_Rate', 0)):.1f}%",
            })

        if display_data:
            df_display = pd.DataFrame(display_data)
            st.dataframe(df_display, use_container_width=True, hide_index=True)

    with tab3:
        st.markdown("### 📊 Ad Boost Scoring Methodology")
        st.markdown("""
        **Video Scoring Model** (No GMV data available for TikTok videos)

        | Factor | Weight | Why It Matters |
        |--------|--------|----------------|
        | Views (Reach) | 25% | Indicates content virality & audience reach |
        | Engagement Rate | 30% | Shows audience quality & interaction |
        | Conversion Rate | 25% | Predicts sales potential when boosted |
        | Like Rate | 20% | Content quality indicator |

        **Live Session Scoring Model**

        | Factor | Weight | Why It Matters |
        |--------|--------|----------------|
        | GMV | 35% | Direct revenue impact |
        | Engagement Rate | 25% | Audience quality for retargeting |
        | Conversion Rate | 25% | Sales efficiency |
        | Follower Growth | 15% | Long-term audience building |

        **Why This Works Better Than Simple GMV Sorting:**
        - High GMV doesn't always mean best ad performance
        - Engagement indicates audience quality for pixel firing
        - Conversion rate predicts ROAS when boosted
        - Balanced approach prevents over-indexing on one metric
        """)

elif platform == "Shopee":
    st.info("**Shopee provides monthly aggregated data** - Use periods with highest sales as reference for campaign timing")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📺 Top Live Periods")
        if not shopee_live.empty:
            shopee_live_sorted = shopee_live.copy()
            if 'Sales_Confirmed' in shopee_live_sorted.columns:
                shopee_live_sorted['Sales_Numeric'] = shopee_live_sorted['Sales_Confirmed'].apply(safe_float)
                top_periods = shopee_live_sorted.nlargest(5, 'Sales_Numeric')

                display_data = []
                for i, (_, row) in enumerate(top_periods.iterrows(), 1):
                    display_data.append({
                        'Rank': i,
                        'Period': str(row.get('Report_Period', 'N/A')),
                        'Sales': format_currency(safe_float(row.get('Sales_Confirmed', 0))),
                        'Orders': int(safe_float(row.get('Orders_Confirmed', 0))),
                    })

                if display_data:
                    st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)

    with col2:
        st.markdown("### 🎥 Top Video Periods")
        if not shopee_video.empty:
            shopee_video_sorted = shopee_video.copy()
            if 'Video_Sales_Confirmed' in shopee_video_sorted.columns:
                shopee_video_sorted['Sales_Numeric'] = shopee_video_sorted['Video_Sales_Confirmed'].apply(safe_float)
                top_periods = shopee_video_sorted.nlargest(5, 'Sales_Numeric')

                display_data = []
                for i, (_, row) in enumerate(top_periods.iterrows(), 1):
                    display_data.append({
                        'Rank': i,
                        'Period': str(row.get('Report_Period', 'N/A')),
                        'Sales': format_currency(safe_float(row.get('Video_Sales_Confirmed', 0))),
                        'Orders': int(safe_float(row.get('Video_Orders_Confirmed', 0))),
                    })

                if display_data:
                    st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)

else:
    st.info("No content data available for the selected platform and period")

# ==========================================
# AI-POWERED INSIGHTS
# ==========================================
st.markdown("---")
st.markdown("## 🤖 AI-Powered Content Insights")
st.caption("Powered by Gemini AI - Analyzes your top content and provides actionable recommendations")

if platform in ["TikTok", "All"] and not tiktok_video.empty:
    # Create tabs for different AI insights
    ai_tab1, ai_tab2, ai_tab3 = st.tabs(["📊 Video Analysis", "📺 Live Session Analysis", "💡 Marketing Recommendations"])

    with ai_tab1:
        st.markdown("### 🎥 Top Video Performance Analysis")

        # Prepare video data summary for AI
        if 'Video_ID' in tiktok_video.columns:
            agg_dict = {
                'Views': 'sum',
                'Likes': 'sum',
                'Comments': 'sum',
                'Shares': 'sum',
                'Engagement_Rate': 'mean',
                'Conversion_Rate': 'mean',
                'Creator': 'first',
                'Video_Title': 'first',
            }
            available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_video.columns}
            deduped_videos = tiktok_video.groupby('Video_ID', as_index=False).agg(available_agg)
            scored_videos = calculate_ad_boost_score(deduped_videos, 'video')
            top_5 = scored_videos.nlargest(5, 'Ad_Boost_Score')
        else:
            top_5 = tiktok_video.head(5)

        # Display top 5 summary
        for i, row in top_5.iterrows():
            with st.container():
                col1, col2 = st.columns([3, 1])
                with col1:
                    video_id = str(int(safe_float(row.get('Video_ID', 0)))) if pd.notna(row.get('Video_ID')) else 'N/A'
                    st.markdown(f"**#{top_5.index.get_loc(i)+1}** | Video ID: `{video_id}`")
                    st.markdown(f"📹 **Title:** {str(row.get('Video_Title', 'N/A'))[:60]}...")
                    st.markdown(f"👤 **Creator:** {row.get('Creator', 'Unknown')}")
                with col2:
                    st.metric("Views", format_number(safe_float(row.get('Views', 0))))
                    st.metric("Score", f"{safe_float(row.get('Ad_Boost_Score', 0)):.1f}")
                st.markdown("---")

        # Generate AI insights
        if st.button("🔍 Generate AI Video Insights", key="video_insights_btn"):
            with st.spinner("🤖 AI is analyzing your top videos..."):
                # Prepare data summary for AI
                video_summary = f"""
                Top 5 TikTok Videos Performance Data:
                {chr(10).join([f"- Video {i+1}: Views={format_number(safe_float(r.get('Views',0)))}, Likes={format_number(safe_float(r.get('Likes',0)))}, Engagement={safe_float(r.get('Engagement_Rate',0)):.1f}%, Score={safe_float(r.get('Ad_Boost_Score',0)):.1f}" for i, r in top_5.iterrows()])}

                Average Engagement Rate: {top_5['Engagement_Rate'].apply(safe_float).mean():.2f}%
                Average Views: {format_number(top_5['Views'].apply(safe_float).mean())}
                Total Views (Top 5): {format_number(top_5['Views'].apply(safe_float).sum())}
                """

                prompt = f"""You are a TikTok marketing expert. Analyze this video performance data and provide actionable insights in Thai language with English technical terms:

                {video_summary}

                Please provide:
                1. **📊 สรุปภาพรวม (Overview)**: Brief summary of performance
                2. **🎯 จุดแข็ง (Strengths)**: What's working well (2-3 points)
                3. **⚠️ โอกาสในการปรับปรุง (Improvement Opportunities)**: Areas to improve (2-3 points)
                4. **💡 คำแนะนำสำหรับ Ad Boost (Ad Boost Recommendations)**: Specific advice for boosting these videos

                Keep it concise and actionable. Use bullet points.
                """

                insights = get_gemini_insights(prompt)
                st.markdown(insights)

    with ai_tab2:
        st.markdown("### 📺 Live Session Performance Analysis")

        if not tiktok_live.empty:
            # Prepare live data summary
            agg_dict = {
                'GMV': 'sum',
                'Orders': 'sum',
                'Viewers': 'sum',
                'Views': 'sum',
                'Likes': 'sum',
                'New_Followers': 'sum',
                'Engagement_Rate': 'mean',
                'Duration': 'first',
                'Creator': 'first',
                'Start_Time': 'first',
            }
            available_agg = {k: v for k, v in agg_dict.items() if k in tiktok_live.columns}
            if 'Start_Time' in tiktok_live.columns and 'Creator' in tiktok_live.columns:
                deduped_live = tiktok_live.groupby(['Start_Time', 'Creator'], as_index=False).agg(available_agg)
            else:
                deduped_live = tiktok_live

            scored_live = calculate_ad_boost_score(deduped_live, 'live')
            top_5_live = scored_live.nlargest(5, 'Ad_Boost_Score')

            # Display top 5 summary
            for i, row in top_5_live.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**#{top_5_live.index.get_loc(i)+1}** | {row.get('Start_Time', 'N/A')}")
                        st.markdown(f"👤 **Creator:** {row.get('Creator', 'Unknown')}")
                        st.markdown(f"⏱️ **Duration:** {row.get('Duration', 'N/A')}")
                    with col2:
                        st.metric("GMV", format_currency(safe_float(row.get('GMV', 0))))
                        st.metric("Orders", int(safe_float(row.get('Orders', 0))))
                    st.markdown("---")

            if st.button("🔍 Generate AI Live Session Insights", key="live_insights_btn"):
                with st.spinner("🤖 AI is analyzing your live sessions..."):
                    live_summary = f"""
                    Top 5 TikTok Live Sessions Performance Data:
                    {chr(10).join([f"- Session {i+1}: GMV={format_currency(safe_float(r.get('GMV',0)))}, Orders={int(safe_float(r.get('Orders',0)))}, Viewers={format_number(safe_float(r.get('Viewers',0)))}, Engagement={safe_float(r.get('Engagement_Rate',0)):.1f}%" for i, r in top_5_live.iterrows()])}

                    Total GMV (Top 5): {format_currency(top_5_live['GMV'].apply(safe_float).sum())}
                    Average Orders: {top_5_live['Orders'].apply(safe_float).mean():.0f}
                    Total Viewers: {format_number(top_5_live['Viewers'].apply(safe_float).sum())}
                    """

                    prompt = f"""You are a TikTok Live commerce expert. Analyze this live session performance data and provide actionable insights in Thai language with English technical terms:

                    {live_summary}

                    Please provide:
                    1. **📊 สรุปยอดขาย (Sales Overview)**: Brief summary of live performance
                    2. **🏆 ปัจจัยความสำเร็จ (Success Factors)**: What drives sales in these sessions
                    3. **📈 กลยุทธ์การขาย (Sales Strategies)**: Recommended strategies for future sessions
                    4. **🎯 คำแนะนำสำหรับ Retargeting Ads**: Specific advice for ad retargeting

                    Keep it concise and actionable. Use bullet points.
                    """

                    insights = get_gemini_insights(prompt)
                    st.markdown(insights)
        else:
            st.info("No TikTok live session data available for analysis")

    with ai_tab3:
        st.markdown("### 💡 AI Marketing Recommendations")

        if st.button("🎯 Generate Comprehensive Marketing Strategy", key="marketing_btn"):
            with st.spinner("🤖 AI is generating marketing recommendations..."):
                # Aggregate overall metrics
                total_video_views = tiktok_video['Views'].apply(safe_float).sum() if 'Views' in tiktok_video.columns else 0
                total_live_gmv = tiktok_live['GMV'].apply(safe_float).sum() if 'GMV' in tiktok_live.columns else 0
                avg_engagement = tiktok_video['Engagement_Rate'].apply(safe_float).mean() if 'Engagement_Rate' in tiktok_video.columns else 0

                overall_summary = f"""
                Content Commerce Performance Summary ({start_date} to {end_date}):

                VIDEO METRICS:
                - Total Video Views: {format_number(total_video_views)}
                - Average Engagement Rate: {avg_engagement:.2f}%
                - Total Videos: {tiktok_video['Video_ID'].nunique() if 'Video_ID' in tiktok_video.columns else len(tiktok_video)}

                LIVE STREAMING METRICS:
                - Total Live GMV: {format_currency(total_live_gmv)}
                - Total Live Sessions: {len(tiktok_live) if not tiktok_live.empty else 0}
                - Total Orders: {tiktok_live['Orders'].apply(safe_float).sum() if 'Orders' in tiktok_live.columns else 0}

                PLATFORM: TikTok
                """

                prompt = f"""You are an e-commerce marketing strategist specializing in TikTok Shop. Based on this performance data, provide a comprehensive marketing strategy in Thai language with English technical terms:

                {overall_summary}

                Please provide a detailed strategy with:
                1. **🎯 กลยุทธ์การใช้ Ad Boost (Ad Boost Strategy)**
                   - Budget allocation recommendations
                   - Which content types to prioritize
                   - Timing recommendations

                2. **📱 Content Strategy**
                   - Types of content to create more
                   - Optimal posting schedule
                   - Content format recommendations

                3. **🛒 Conversion Optimization**
                   - How to improve conversion rates
                   - Product showcasing tips
                   - Call-to-action recommendations

                4. **📊 KPIs ที่ควรติดตาม (KPIs to Track)**
                   - Key metrics to monitor
                   - Benchmarks to set

                5. **💰 การกระจายงบประมาณ (Budget Allocation)**
                   - Recommended split between video ads and live promotion
                   - ROAS targets

                Keep recommendations specific and actionable. Use bullet points and sub-headers.
                """

                recommendations = get_gemini_insights(prompt)
                st.markdown(recommendations)

elif platform == "Shopee":
    st.info("AI insights are optimized for TikTok data with session-level metrics. Switch to TikTok or All platforms for detailed AI analysis.")

# ==========================================
# DATA TABLES (Expandable)
# ==========================================
st.markdown("---")
st.markdown("**📋 Detailed Data Tables**")

if platform in ["Shopee", "All"]:
    with st.expander("📺 Shopee Live Data", expanded=False):
        if not shopee_live.empty:
            st.dataframe(shopee_live, use_container_width=True, hide_index=True)
        else:
            st.info("No Shopee live data for selected period")

    with st.expander("🎥 Shopee Video Data", expanded=False):
        if not shopee_video.empty:
            st.dataframe(shopee_video, use_container_width=True, hide_index=True)
        else:
            st.info("No Shopee video data for selected period")

if platform in ["TikTok", "All"]:
    with st.expander("🎬 TikTok Live Sessions", expanded=False):
        if not tiktok_live.empty:
            st.dataframe(tiktok_live, use_container_width=True, hide_index=True)
        else:
            st.info("No TikTok live data for selected period")

    with st.expander("🎥 TikTok Videos", expanded=False):
        if not tiktok_video.empty:
            st.dataframe(tiktok_video, use_container_width=True, hide_index=True)
        else:
            st.info("No TikTok video data for selected period")
