"""
Direct Sales Page - Line & Facebook Manual Sales
Track offline/direct sales that don't go through e-commerce platforms
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from utils import (
    safe_float, format_currency, format_number, load_line_orders,
    load_daily_sales, COMMON_STYLES
)

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="Direct Sales - Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply common styles
st.markdown(COMMON_STYLES, unsafe_allow_html=True)

# Platform colors
PLATFORM_COLORS = {
    'Line': '#00b900',
    'Facebook': '#1877f2',
    'Cash Sale / Line': '#00b900',
    'Line - Cod': '#00c300',
}

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 💰 Direct Sales")
    st.markdown("---")

    # Load data for date range
    daily_df = load_daily_sales()
    line_df = load_line_orders()

    if not daily_df.empty:
        min_date = pd.to_datetime(daily_df['Date']).min().date()
        max_date = pd.to_datetime(daily_df['Date']).max().date()
    else:
        min_date = datetime.now().date() - timedelta(days=30)
        max_date = datetime.now().date()

    # Quick time range
    st.markdown("#### ⚡ Quick Select")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Last 7 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=7), max_date)
            st.rerun()
        if st.button("Last 30 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=30), max_date)
            st.rerun()

    with col2:
        if st.button("Last 90 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=90), max_date)
            st.rerun()
        if st.button("Last 12 Months", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=365), max_date)
            st.rerun()

    with col3:
        if st.button("This Year", use_container_width=True):
            st.session_state.date_range = (datetime(max_date.year, 1, 1).date(), max_date)
            st.rerun()
        if st.button("All Time", use_container_width=True, type="primary"):
            st.session_state.date_range = (min_date, max_date)
            st.rerun()

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
st.markdown('<p class="main-header">💰 Direct Sales - Line & Facebook</p>', unsafe_allow_html=True)
st.markdown(f"**Period:** {start_date} to {end_date}")
st.caption("Track manual sales from Line, Facebook, and other direct channels")

# ==========================================
# LOAD AND FILTER DATA
# ==========================================
line_df = load_line_orders()

if not line_df.empty and 'Order_Date' in line_df.columns:
    line_df['Order_Date'] = pd.to_datetime(line_df['Order_Date'])
    mask = (line_df['Order_Date'] >= pd.to_datetime(start_date)) & (line_df['Order_Date'] <= pd.to_datetime(end_date))
    filtered_line = line_df[mask].copy()
else:
    filtered_line = pd.DataFrame()

# ==========================================
# KPI SECTION
# ==========================================
st.markdown('<p class="section-header">📈 Direct Sales Summary</p>', unsafe_allow_html=True)

if not filtered_line.empty:
    total_gmv = filtered_line['Net_Sales'].sum()
    total_with_vat = filtered_line['Total_Amount'].sum()
    total_vat = filtered_line['VAT'].sum()
    total_transactions = len(filtered_line)
    avg_transaction = total_gmv / total_transactions if total_transactions > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("💰 Total Sales (ex VAT)", format_currency(total_gmv))
    with col2:
        st.metric("💵 Total with VAT", format_currency(total_with_vat))
    with col3:
        st.metric("📦 Transactions", format_number(total_transactions))
    with col4:
        st.metric("📊 Avg Transaction", format_currency(avg_transaction))

    st.markdown("---")

    # ==========================================
    # SALES BY CHANNEL
    # ==========================================
    st.markdown('<p class="section-header">📊 Sales by Channel</p>', unsafe_allow_html=True)

    if 'Customer_Type' in filtered_line.columns:
        channel_summary = filtered_line.groupby('Customer_Type').agg({
            'Net_Sales': 'sum',
            'VAT': 'sum',
            'Total_Amount': 'sum',
            'Order_ID': 'count'
        }).reset_index()
        channel_summary.columns = ['Channel', 'Net_Sales', 'VAT', 'Total', 'Transactions']
        channel_summary = channel_summary.sort_values('Net_Sales', ascending=False)

        col1, col2 = st.columns([2, 1])

        with col1:
            # Bar chart
            fig = go.Figure(data=[
                go.Bar(
                    x=channel_summary['Channel'],
                    y=channel_summary['Net_Sales'],
                    marker_color=[PLATFORM_COLORS.get(c, '#666') for c in channel_summary['Channel']],
                    text=[format_currency(v) for v in channel_summary['Net_Sales']],
                    textposition='outside'
                )
            ])
            fig.update_layout(
                title="Sales by Channel",
                xaxis_title="",
                yaxis_title="GMV (฿)",
                height=350,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Donut chart
            fig = go.Figure(data=[go.Pie(
                labels=channel_summary['Channel'],
                values=channel_summary['Net_Sales'],
                hole=0.5,
                marker_colors=[PLATFORM_COLORS.get(c, '#666') for c in channel_summary['Channel']],
                textinfo='percent',
                texttemplate='%{percent:.1%}'
            )])
            fig.update_layout(
                title="Share by Channel",
                height=350,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

    # ==========================================
    # DAILY TREND
    # ==========================================
    st.markdown('<p class="section-header">📈 Daily Sales Trend</p>', unsafe_allow_html=True)

    daily_trend = filtered_line.groupby(filtered_line['Order_Date'].dt.date).agg({
        'Net_Sales': 'sum',
        'Total_Amount': 'sum',
        'Order_ID': 'count'
    }).reset_index()
    daily_trend.columns = ['Date', 'Net_Sales', 'Total', 'Transactions']
    daily_trend = daily_trend.sort_values('Date')

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily_trend['Date'],
        y=daily_trend['Net_Sales'],
        name='Daily Sales',
        marker_color='#00b900',
        text=[format_currency(v) if v > 5000 else '' for v in daily_trend['Net_Sales']],
        textposition='outside'
    ))

    # Add trend line
    if len(daily_trend) > 1:
        fig.add_trace(go.Scatter(
            x=daily_trend['Date'],
            y=daily_trend['Net_Sales'].rolling(window=min(7, len(daily_trend)), min_periods=1).mean(),
            name='7-Day Avg',
            line=dict(color='#ee4d2d', width=2, dash='dot')
        ))

    fig.update_layout(
        title="Daily Direct Sales",
        xaxis_title="Date",
        yaxis_title="GMV (฿)",
        hovermode='x unified',
        height=400,
        showlegend=True,
        legend=dict(orientation="h", y=1.1)
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ==========================================
    # COMPARISON WITH PLATFORM SALES
    # ==========================================
    st.markdown('<p class="section-header">🔄 Platform Comparison</p>', unsafe_allow_html=True)

    # Load all daily sales for comparison
    daily_df = load_daily_sales()

    if not daily_df.empty:
        daily_df['Date'] = pd.to_datetime(daily_df['Date'])
        mask = (daily_df['Date'] >= pd.to_datetime(start_date)) & (daily_df['Date'] <= pd.to_datetime(end_date))
        platform_daily = daily_df[mask].copy()

        # Aggregate by platform
        platform_summary = platform_daily.groupby('Platform').agg({
            'GMV': 'sum',
            'Orders': 'sum'
        }).reset_index()

        # Add Line data
        line_row = pd.DataFrame([{
            'Platform': 'Line/Direct',
            'GMV': total_gmv,
            'Orders': total_transactions
        }])

        comparison_df = pd.concat([platform_summary, line_row], ignore_index=True)
        comparison_df = comparison_df.sort_values('GMV', ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            # Comparison bar chart
            fig = go.Figure(data=[
                go.Bar(
                    x=comparison_df['Platform'],
                    y=comparison_df['GMV'],
                    marker_color=['#ee4d2d' if p == 'Shopee' else '#000000' if p == 'TikTok' else '#00b900' for p in comparison_df['Platform']],
                    text=[format_currency(v) for v in comparison_df['GMV']],
                    textposition='outside'
                )
            ])
            fig.update_layout(
                title="GMV by Platform",
                xaxis_title="",
                yaxis_title="GMV (฿)",
                height=350,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Share pie chart
            fig = go.Figure(data=[go.Pie(
                labels=comparison_df['Platform'],
                values=comparison_df['GMV'],
                hole=0.4,
                marker_colors=['#ee4d2d' if p == 'Shopee' else '#000000' if p == 'TikTok' else '#00b900' for p in comparison_df['Platform']],
                textinfo='percent+label',
                texttemplate='%{label}<br>%{percent:.1%}'
            )])
            fig.update_layout(
                title="GMV Share",
                height=350,
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)

        # Calculate share
        total_all = comparison_df['GMV'].sum()
        line_share = (total_gmv / total_all * 100) if total_all > 0 else 0

        st.info(f"💡 Direct sales (Line/Facebook) represent **{line_share:.1f}%** of total GMV in this period.")

    st.markdown("---")

    # ==========================================
    # TRANSACTION DETAILS
    # ==========================================
    st.markdown('<p class="section-header">📋 Recent Transactions</p>', unsafe_allow_html=True)

    display_df = filtered_line.sort_values('Order_Date', ascending=False).head(50).copy()
    display_df['Order_Date'] = display_df['Order_Date'].dt.strftime('%Y-%m-%d')
    display_df['Net_Sales'] = display_df['Net_Sales'].apply(lambda x: f"฿{x:,.2f}")
    display_df['Total_Amount'] = display_df['Total_Amount'].apply(lambda x: f"฿{x:,.0f}")

    display_cols = ['Order_ID', 'Order_Date', 'Customer_Type', 'Net_Sales', 'Total_Amount']
    available_cols = [c for c in display_cols if c in display_df.columns]

    st.dataframe(
        display_df[available_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            'Order_ID': 'Document ID',
            'Order_Date': 'Date',
            'Customer_Type': 'Channel',
            'Net_Sales': st.column_config.TextColumn('Net Sales'),
            'Total_Amount': st.column_config.TextColumn('Total (inc VAT)')
        }
    )

else:
    st.info("No direct sales data available for the selected period.")
    st.markdown("**To add direct sales data:**")
    st.markdown("1. Add sales records to `Facebook and Line/ยอดขาย2024-2026.xlsx`")
    st.markdown("2. Run `python data_pipeline.py` to update the database")
    st.markdown("3. Refresh this page")

# ==========================================
# FOOTER
# ==========================================
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
