"""
Customer Intelligence Dashboard
===============================
RFM Segmentation, Customer Analytics, and Market Basket Analysis
"""

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime, timedelta

# Database path
DB_PATH = Path(__file__).parent.parent / "processed_data" / "shopee_dashboard.duckdb"

# ==========================================
# CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Customer Intelligence",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Segment colors
SEGMENT_COLORS = {
    'Champions': '#1f77b4',
    'Loyal Customers': '#2ca02c',
    'Potential Loyalist': '#17becf',
    'New Customers': '#9467bd',
    'Promising': '#c5b0d5',
    'Need Attention': '#ffbb78',
    'About to Sleep': '#ff9896',
    'At Risk': '#ff7f0e',
    'Cant Lose Them': '#d62728',
    'Hibernating': '#c7c7c7',
    'Lost': '#7f7f7f',
}

# Segment strategies
SEGMENT_STRATEGIES = {
    'Champions': 'Reward with early access, exclusive offers. Ask for reviews and referrals.',
    'Loyal Customers': 'Upsell higher-value products. Engage with loyalty program.',
    'Potential Loyalist': 'Recommend products, offer membership benefits.',
    'New Customers': 'Onboarding series, provide excellent support.',
    'Promising': 'Create brand awareness, offer first-purchase incentives.',
    'Need Attention': 'Reactivate with limited-time offers, highlight new products.',
    'About to Sleep': 'Re-engage with personalized recommendations.',
    'At Risk': 'Send aggressive win-back campaigns, conduct surveys.',
    'Cant Lose Them': 'Personal outreach, premium support, understand their needs.',
    'Hibernating': 'Offer steep discounts, highlight value proposition.',
    'Lost': 'Attempt reactivation, but focus budget elsewhere.',
}

# ==========================================
# DATABASE CONNECTION
# ==========================================
@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)

conn = get_connection()

# ==========================================
# DATA LOADING
# ==========================================
@st.cache_data(ttl=300)
def load_rfm_data():
    try:
        return conn.execute("SELECT * FROM customer_rfm").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_segment_summary():
    try:
        return conn.execute("SELECT * FROM segment_summary").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_orders():
    try:
        return conn.execute("""
            SELECT Order_ID, Order_Date, Product_Name, Net_Sales, Quantity, Platform
            FROM orders_raw
            UNION ALL
            SELECT Order_ID, Order_Date, Product_Name, Net_Sales, Quantity, Platform
            FROM tiktok_orders
        """).fetchdf()
    except:
        return pd.DataFrame()

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def format_currency(value):
    try:
        return f"฿{float(value):,.0f}"
    except:
        return "฿0"

def format_number(value):
    try:
        return f"{float(value):,.0f}"
    except:
        return "0"

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 👥 Customer Intelligence")
    st.markdown("---")
    st.markdown("**RFM Analysis** segments customers based on:")
    st.markdown("- **R**ecency: Days since last purchase")
    st.markdown("- **F**requency: Number of orders")
    st.markdown("- **M**onetary: Total spend")
    st.markdown("---")
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# LOAD DATA
# ==========================================
rfm_df = load_rfm_data()
segment_df = load_segment_summary()
orders_df = load_orders()

# ==========================================
# HEADER
# ==========================================
st.markdown('<p style="font-size:1.5rem; font-weight:600; color:#1a1a1a;">👥 Customer Intelligence</p>', unsafe_allow_html=True)
st.caption("RFM Segmentation, Customer Analytics & Market Basket Analysis")
st.markdown("---")

if rfm_df.empty:
    st.warning("⚠️ No customer RFM data available. Run the data pipeline to generate customer segmentation.")
    st.info("Run: `python data_pipeline.py --incremental`")
    st.stop()

# ==========================================
# SECTION 1: RFM OVERVIEW
# ==========================================
st.markdown('<p style="font-size:1.1rem; font-weight:600; color:#333; border-bottom:2px solid #ee4d2d; padding-bottom:0.3rem;">📊 Customer Segment Overview</p>', unsafe_allow_html=True)

# Key metrics
total_customers = len(rfm_df)
total_revenue = rfm_df['Monetary'].sum()
avg_frequency = rfm_df['Frequency'].mean()
avg_recency = rfm_df['Recency'].mean()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Customers", format_number(total_customers))
with col2:
    st.metric("Total Revenue", format_currency(total_revenue))
with col3:
    st.metric("Avg Orders/Customer", f"{avg_frequency:.1f}")
with col4:
    st.metric("Avg Days Since Purchase", f"{avg_recency:.0f}")

st.markdown("---")

# ==========================================
# SECTION 2: SEGMENT DISTRIBUTION
# ==========================================
col_left, col_right = st.columns([2, 1])

with col_left:
    # Segment distribution bar chart
    segment_counts = rfm_df.groupby('Segment').agg({
        'Customer_ID': 'count',
        'Monetary': 'sum'
    }).reset_index()
    segment_counts.columns = ['Segment', 'Customers', 'Revenue']
    segment_counts = segment_counts.sort_values('Revenue', ascending=True)

    fig = px.bar(
        segment_counts,
        x='Customers',
        y='Segment',
        orientation='h',
        color='Segment',
        color_discrete_map=SEGMENT_COLORS,
        hover_data={'Revenue': ':,.0f'}
    )
    fig.update_layout(
        title="Customers by Segment",
        height=400,
        showlegend=False,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    # Revenue pie chart
    fig = px.pie(
        segment_counts,
        values='Revenue',
        names='Segment',
        color='Segment',
        color_discrete_map=SEGMENT_COLORS,
        hole=0.4
    )
    fig.update_layout(
        title="Revenue by Segment",
        height=400,
        showlegend=False,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    fig.update_traces(textinfo='percent', texttemplate='%{percent:.1%}')
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ==========================================
# SECTION 3: SEGMENT DETAILS TABLE
# ==========================================
st.markdown('<p style="font-size:1.1rem; font-weight:600; color:#333; border-bottom:2px solid #ee4d2d; padding-bottom:0.3rem;">📋 Segment Details & Strategies</p>', unsafe_allow_html=True)

if not segment_df.empty:
    # Display segment summary
    display_df = segment_df.copy()
    display_df['Avg_Monetary'] = display_df['Avg_Monetary'].apply(format_currency)
    display_df['Total_Monetary'] = display_df['Total_Monetary'].apply(format_currency)
    display_df['Pct_Customers'] = display_df['Pct_Customers'].apply(lambda x: f"{x:.1f}%")
    display_df['Pct_Revenue'] = display_df['Pct_Revenue'].apply(lambda x: f"{x:.1f}%")
    display_df['Strategy'] = display_df['Segment'].map(SEGMENT_STRATEGIES)

    display_df = display_df[['Segment', 'Customers', 'Pct_Customers', 'Avg_Monetary',
                             'Total_Monetary', 'Pct_Revenue', 'Strategy']]

    st.dataframe(display_df, use_container_width=True, hide_index=True)

st.markdown("---")

# ==========================================
# SECTION 4: RFM SCATTER PLOT
# ==========================================
st.markdown('<p style="font-size:1.1rem; font-weight:600; color:#333; border-bottom:2px solid #ee4d2d; padding-bottom:0.3rem;">📈 RFM Score Distribution</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Recency vs Frequency
    fig = px.scatter(
        rfm_df.sample(min(1000, len(rfm_df))),  # Sample for performance
        x='Recency',
        y='Frequency',
        color='Segment',
        size='Monetary',
        color_discrete_map=SEGMENT_COLORS,
        hover_data=['Customer_ID', 'Monetary'],
        title="Recency vs Frequency (size = Monetary)"
    )
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # RFM Score distribution
    rfm_df['RFM_Score_Num'] = rfm_df['R_Score'] + rfm_df['F_Score'] + rfm_df['M_Score']
    fig = px.histogram(
        rfm_df,
        x='RFM_Score_Num',
        color='Segment',
        color_discrete_map=SEGMENT_COLORS,
        nbins=15,
        title="RFM Score Distribution (R+F+M)"
    )
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ==========================================
# SECTION 5: TOP CUSTOMERS
# ==========================================
st.markdown('<p style="font-size:1.1rem; font-weight:600; color:#333; border-bottom:2px solid #ee4d2d; padding-bottom:0.3rem;">🏆 Top Customers by Segment</p>', unsafe_allow_html=True)

# Segment selector
selected_segment = st.selectbox(
    "Select Segment",
    options=['All'] + list(rfm_df['Segment'].unique()),
    index=0
)

if selected_segment == 'All':
    top_customers = rfm_df.nlargest(20, 'Monetary')
else:
    top_customers = rfm_df[rfm_df['Segment'] == selected_segment].nlargest(20, 'Monetary')

# Display top customers
display_cols = ['Customer_ID', 'Recency', 'Frequency', 'Monetary', 'R_Score', 'F_Score', 'M_Score', 'Segment']
top_display = top_customers[display_cols].copy()
top_display['Monetary'] = top_display['Monetary'].apply(format_currency)
top_display.columns = ['Customer', 'Days Since Purchase', 'Orders', 'Total Spend', 'R', 'F', 'M', 'Segment']

st.dataframe(top_display, use_container_width=True, hide_index=True)

st.markdown("---")

# ==========================================
# SECTION 6: MARKET BASKET ANALYSIS (SIMPLIFIED)
# ==========================================
st.markdown('<p style="font-size:1.1rem; font-weight:600; color:#333; border-bottom:2px solid #ee4d2d; padding-bottom:0.3rem;">🛒 Product Affinity (Top Product Pairs)</p>', unsafe_allow_html=True)

if not orders_df.empty:
    # Simple product co-occurrence analysis
    # Find products frequently bought together
    order_products = orders_df.groupby('Order_ID')['Product_Name'].apply(list).reset_index()

    # Count product pairs
    from collections import Counter
    pair_counts = Counter()

    for products in order_products['Product_Name']:
        if len(products) > 1:
            # Get unique products in this order
            unique_products = list(set(products))
            # Count all pairs
            for i in range(len(unique_products)):
                for j in range(i+1, len(unique_products)):
                    pair = tuple(sorted([unique_products[i][:30], unique_products[j][:30]]))
                    pair_counts[pair] += 1

    # Get top 15 pairs
    top_pairs = pair_counts.most_common(15)

    if top_pairs:
        pairs_df = pd.DataFrame([
            {'Product A': p[0][0], 'Product B': p[0][1], 'Bought Together': p[1]}
            for p in top_pairs
        ])
        st.dataframe(pairs_df, use_container_width=True, hide_index=True)
        st.caption("Products frequently purchased together (based on order co-occurrence)")
    else:
        st.info("Not enough multi-product orders to analyze product affinities.")
else:
    st.info("Order data not available for market basket analysis.")

# ==========================================
# FOOTER
# ==========================================
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.85rem;">
    <p>Customer Intelligence Dashboard | RFM Analysis</p>
</div>
""", unsafe_allow_html=True)
