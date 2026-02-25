"""
Shopee Executive Dashboard
===========================
Streamlit dashboard with DuckDB for fast SQL queries
Light theme, minimal style, optimized for sharing

Run: streamlit run dashboard.py --server.address 100.66.69.21
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
    page_title="Shopee Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Light theme CSS
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
    [data-testid="stMetricDelta"] > div:has-text("-") {
        color: #dc3545;
    }

    /* Quick select buttons */
    .quick-btn {
        display: inline-block;
        padding: 4px 12px;
        margin: 2px;
        border-radius: 4px;
        background: #f0f0f0;
        color: #333;
        cursor: pointer;
        font-size: 0.85rem;
    }
    .quick-btn:hover {
        background: #e0e0e0;
    }
    .quick-btn.active {
        background: #ee4d2d;
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
        return f"{v:+.1f}%" if not pd.isna(v) else "-"
    except:
        return "-"

def info_tooltip(text):
    """Display info text"""
    pass  # Removed icon - tooltips not working properly

def metric_with_tooltip(label, value, delta=None, tooltip_text=""):
    """Display a metric"""
    if delta:
        st.metric(label=label, value=value, delta=delta)
    else:
        st.metric(label=label, value=value)

# ==========================================
# DATA LOADING FUNCTIONS
# ==========================================
@st.cache_data(ttl=300)
def load_daily_sales():
    return conn.execute("SELECT * FROM daily_sales ORDER BY Date").fetchdf()

@st.cache_data(ttl=300)
def load_products():
    return conn.execute("SELECT * FROM products ORDER BY Total_GMV DESC").fetchdf()

@st.cache_data(ttl=300)
def load_orders_raw():
    """Load raw orders for time-filtered product analysis"""
    try:
        return conn.execute("SELECT * FROM orders_raw").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_ads():
    return conn.execute("SELECT * FROM ads_performance ORDER BY ROAS DESC").fetchdf()

@st.cache_data(ttl=300)
def load_geographic():
    return conn.execute("SELECT * FROM geographic ORDER BY GMV DESC").fetchdf()

@st.cache_data(ttl=300)
def load_daily_geographic():
    """Load daily geographic data for time-filtered analysis"""
    try:
        return conn.execute("SELECT * FROM daily_geographic").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_live_data():
    try:
        return conn.execute("SELECT * FROM combined_live").fetchdf()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_video_data():
    try:
        return conn.execute("SELECT * FROM combined_video").fetchdf()
    except:
        return pd.DataFrame()

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 📊 Shopee Dashboard")
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
            q4_end = max_date  # Always cap at max_date
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

    # Filters
    st.markdown("#### 🔍 Filters")
    show_ads = st.checkbox("Show Ads Data", value=False)
    show_geo = st.checkbox("Show Geographic", value=True)

    st.markdown("---")

    # Info
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.markdown(f"**Data Range:** {min_date} to {max_date}")

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    # Navigation
    st.markdown("#### 📌 Pages")
    st.page_link("dashboard.py", label="📊 Main Dashboard", icon="📊")
    st.page_link("pages/1_🎬_Content_Commerce.py", label="🎬 Content Commerce", icon="🎬")

# ==========================================
# FILTER DATA BY DATE RANGE
# ==========================================
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

daily_df['Date'] = pd.to_datetime(daily_df['Date'])
mask = (daily_df['Date'] >= pd.to_datetime(start_date)) & (daily_df['Date'] <= pd.to_datetime(end_date))
filtered_daily = daily_df[mask].copy()

# Calculate previous period for comparison
period_days = (end_date - start_date).days + 1
prev_start = start_date - timedelta(days=period_days)
prev_end = start_date - timedelta(days=1)

prev_mask = (daily_df['Date'] >= pd.to_datetime(prev_start)) & (daily_df['Date'] <= pd.to_datetime(prev_end))
prev_daily = daily_df[prev_mask].copy()

# ==========================================
# HEADER
# ==========================================
st.markdown('<p class="main-header">Shopee GMV & Marketing Command Center</p>', unsafe_allow_html=True)
st.markdown(f"**Period:** {start_date} to {end_date} | Compared to: {prev_start} to {prev_end}")
st.caption(f"Dashboard v2.0 - Updated: 2026-02-24 11:30")

# ==========================================
# SECTION 1: HERO KPIs WITH COMPARISON
# ==========================================
st.markdown('<p class="section-header">📈 Key Performance Indicators</p>', unsafe_allow_html=True)

# Current period metrics
current_gmv = filtered_daily['GMV'].sum()
current_orders = filtered_daily['Orders'].sum()
current_aov = filtered_daily['AOV'].mean() if len(filtered_daily) > 0 else 0
current_revenue = filtered_daily['Net_Revenue'].sum()
current_units = filtered_daily['Units_Sold'].sum()

# Previous period metrics
prev_gmv = prev_daily['GMV'].sum() if len(prev_daily) > 0 else 1
prev_orders = prev_daily['Orders'].sum() if len(prev_daily) > 0 else 1
prev_aov = prev_daily['AOV'].mean() if len(prev_daily) > 0 else 1
prev_revenue = prev_daily['Net_Revenue'].sum() if len(prev_daily) > 0 else 1

# Calculate changes
gmv_change = ((current_gmv - prev_gmv) / prev_gmv * 100) if prev_gmv > 0 else 0
orders_change = ((current_orders - prev_orders) / prev_orders * 100) if prev_orders > 0 else 0
aov_change = ((current_aov - prev_aov) / prev_aov * 100) if prev_aov > 0 else 0
revenue_change = ((current_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    metric_with_tooltip(
        "💰 Total GMV",
        format_currency(current_gmv),
        f"{gmv_change:+.1f}% vs prev",
        "GMV (มูลค่าสินค้ารวม): ผลรวมยอดขายสุทธิจากคำสั่งซื้อทั้งหมดในช่วงเวลาที่เลือก ไม่รวมคำสั่งซื้อที่ยกเลิก"
    )

with col2:
    metric_with_tooltip(
        "📦 Total Orders",
        format_number(current_orders),
        f"{orders_change:+.1f}% vs prev",
        "จำนวนคำสั่งซื้อที่ยืนยันแล้วในช่วงเวลาที่เลือก ไม่รวมคำสั่งซื้อที่ยกเลิกหรือส่งคืน"
    )

with col3:
    metric_with_tooltip(
        "💵 Average AOV",
        format_currency(current_aov),
        f"{aov_change:+.1f}% vs prev",
        "AOV (มูลค่าเฉลี่ยต่อคำสั่งซื้อ): GMV ÷ จำนวนคำสั่งซื้อ แสดงขนาดตะกร้าซื้อเฉลี่ย"
    )

with col4:
    if show_ads:
        ads_df = load_ads()
        total_ad_spend = ads_df['Ad_Cost'].sum()
        total_ad_sales = ads_df['Sales'].sum()
        blended_roas = total_ad_sales / total_ad_spend if total_ad_spend > 0 else 0
        metric_with_tooltip(
            "📊 Blended ROAS",
            f"{blended_roas:.2f}x",
            f"Spend: {format_currency(total_ad_spend)}",
            "ROAS (ผลตอบแทนจากการลงโฆษณา): ยอดขายจากโฆษณา ÷ ค่าโฆษณาทั้งหมด ยิ่งสูงยิ่งดี ข้อมูลเป็นแบบรวมระดับแคมเปญ"
        )
    else:
        metric_with_tooltip(
            "📊 Blended ROAS",
            "N/A",
            None,
            "ROAS (ผลตอบแทนจากการลงโฆษณา): ยอดขายจากโฆษณา ÷ ค่าโฆษณาทั้งหมด"
        )

with col5:
    margin = (current_revenue / current_gmv * 100) if current_gmv > 0 else 0
    metric_with_tooltip(
        "🎯 Net Margin",
        f"{margin:.1f}%",
        f"{revenue_change:+.1f}% vs prev",
        "อัตรากำไรสุทธิ: รายได้สุทธิ ÷ GMV × 100 แสดงอัตรากำไรหลังหักค่าธรรมเนียมแพลตฟอร์มและส่วนลด"
    )

st.markdown("---")

# ==========================================
# SECTION 2: SALES TRENDS
# ==========================================
st.markdown('<p class="section-header">📊 Sales Trends</p>', unsafe_allow_html=True)
st.caption("แนวโน้มยอดขายรายวันและการกระจายสินค้าตามช่วงเวลาที่เลือก")

# Load products for segment distribution (time-filtered)
orders_raw_df = load_orders_raw()
if not orders_raw_df.empty and 'Order_Date' in orders_raw_df.columns:
    orders_raw_df['Order_Date'] = pd.to_datetime(orders_raw_df['Order_Date'])
    orders_mask = (orders_raw_df['Order_Date'] >= pd.to_datetime(start_date)) & (orders_raw_df['Order_Date'] <= pd.to_datetime(end_date))
    filtered_orders = orders_raw_df[orders_mask]

    if len(filtered_orders) > 0:
        products_df = filtered_orders.groupby(['Product_Name', 'SKU']).agg({
            'Net_Sales': 'sum',
            'Quantity': 'sum',
            'Order_ID': 'nunique'
        }).reset_index()
        products_df.columns = ['Product_Name', 'SKU', 'Total_GMV', 'Total_Qty', 'Orders']

        median_gmv = products_df['Total_GMV'].median()
        median_qty = products_df['Total_Qty'].median()

        def assign_segment(row):
            if row['Total_GMV'] >= median_gmv and row['Total_Qty'] >= median_qty:
                return 'Star (High GMV, High Volume)'
            elif row['Total_GMV'] >= median_gmv:
                return 'Hero (High GMV)'
            elif row['Total_Qty'] >= median_qty:
                return 'Volume (High Volume, Low GMV)'
            else:
                return 'Core (Average)'

        products_df['Product_Segment'] = products_df.apply(assign_segment, axis=1)
        products_df = products_df.sort_values('Total_GMV', ascending=False)
    else:
        products_df = pd.DataFrame()
else:
    products_df = pd.DataFrame()

col_left, col_right = st.columns([2, 1])

with col_left:
    # Daily GMV trend with previous period comparison
    st.caption("เส้นทึบ = ช่วงปัจจุบัน, เส้นประ = ช่วงก่อนหน้า")
    fig = go.Figure()

    # Current period
    fig.add_trace(go.Scatter(
        x=filtered_daily['Date'],
        y=filtered_daily['GMV'],
        name="Current Period",
        line=dict(color='#ee4d2d', width=2),
        fill='tozeroy',
        fillcolor='rgba(238, 77, 45, 0.1)'
    ))

    # Previous period (shifted to align)
    if len(prev_daily) > 0:
        prev_daily_shifted = prev_daily.copy()
        prev_daily_shifted['Date'] = prev_daily_shifted['Date'] + timedelta(days=period_days)

        fig.add_trace(go.Scatter(
            x=prev_daily_shifted['Date'],
            y=prev_daily_shifted['GMV'],
            name="Previous Period",
            line=dict(color='#999', width=2, dash='dot'),
        ))

    fig.update_layout(
        title="Daily GMV: Current vs Previous Period",
        hovermode='x unified',
        height=350,
        showlegend=True,
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=0, r=0, t=40, b=0)
    )

    st.plotly_chart(fig, use_container_width=True)

with col_right:
    # Products by Segment Distribution
    segment_colors = {
        'Star (High GMV, High Volume)': '#28a745',
        'Hero (High GMV)': '#007bff',
        'Volume (High Volume, Low GMV)': '#fd7e14',
        'Core (Average)': '#6c757d'
    }

    if len(products_df) > 0:
        # Show top 8 products by GMV
        top_products = products_df.nlargest(8, 'Total_GMV').copy()
        top_products['Short_Name'] = top_products['Product_Name'].apply(lambda x: x[:25] + '...' if len(str(x)) > 25 else x)

        st.caption("Top 8 สินค้าตาม GMV")
        fig = go.Figure(data=[go.Pie(
            labels=top_products['Short_Name'],
            values=top_products['Total_GMV'],
            hole=0.5,
            textinfo='percent',
            texttemplate='%{percent:.1%}',
            hovertemplate='<b>%{label}</b><br>GMV: ฿%{value:,.0f}<extra></extra>'
        )])

        fig.update_layout(
            title="Top Products by GMV",
            height=350,
            margin=dict(l=0, r=0, t=40, b=0),
            showlegend=True
        )

        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ==========================================
# SECTION 3: PRODUCT PERFORMANCE (Time-Filtered)
# ==========================================
st.markdown('<p class="section-header">🏷️ Product Performance</p>', unsafe_allow_html=True)
st.caption("ประสิทธิภาพสินค้าตาม GMV และปริมาณที่ขาย จัดกลุ่มตามค่ามัธยฐาน")

# products_df is already loaded in Section 2 (Sales Trends)
# Just need to add Avg_Price for display
if len(products_df) > 0:
    products_df['Avg_Price'] = products_df['Total_GMV'] / products_df['Total_Qty']
    products_df['Avg_Price'] = products_df['Avg_Price'].fillna(0)

col1, col2 = st.columns([2, 1])

with col1:
    # Product scatter matrix with distinct colors
    segment_colors = {
        'Star (High GMV, High Volume)': '#28a745',
        'Hero (High GMV)': '#007bff',
        'Volume (High Volume, Low GMV)': '#fd7e14',
        'Core (Average)': '#6c757d'
    }

    if len(products_df) > 0:
        st.caption("X=จำนวน, Y=GMV, เส้นประ=ค่ามัธยฐาน")
        fig = px.scatter(
            products_df.head(30),
            x='Total_Qty',
            y='Total_GMV',
            color='Product_Segment',
            size='Total_GMV',
            hover_name='Product_Name',
            hover_data={'SKU': True} if 'SKU' in products_df.columns else {},
            title=f"Product Matrix: Volume vs Revenue ({start_date} to {end_date})",
            color_discrete_map=segment_colors
        )

        # Use actual median from filtered data
        plot_median_qty = products_df['Total_Qty'].median()
        plot_median_gmv = products_df['Total_GMV'].median()

        fig.add_hline(y=plot_median_gmv, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=plot_median_qty, line_dash="dash", line_color="gray", opacity=0.5)

        fig.update_layout(
            height=380,
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product data for selected period")

with col2:
    # Segment summary with colors
    if len(products_df) > 0:
        segment_summary = products_df.groupby('Product_Segment').agg({
            'Product_Name': 'count',
            'Total_GMV': 'sum',
            'Total_Qty': 'sum'
        }).reset_index()
        segment_summary.columns = ['Segment', 'Products', 'GMV', 'Units']

        st.markdown("*Segment Summary*", help="จัดกลุ่มตามค่ามัธยฐานของ GMV และปริมาณ")
        for _, row in segment_summary.iterrows():
            color = segment_colors.get(row['Segment'], '#666')
            st.markdown(f"<span style='color:{color}; font-weight:bold;'>●</span> **{row['Segment']}**: {row['Products']} products, {format_currency(row['GMV'])}", unsafe_allow_html=True)

        # Risk alerts - show products with declining sales (lowest GMV in period)
        st.markdown("*Lowest Performers*", help="3 อันดับสินค้าที่มียอดขายต่ำสุด")
        low_performers = products_df.nsmallest(3, 'Total_GMV')
        for _, row in low_performers.iterrows():
            if 'SKU' in products_df.columns and pd.notna(row.get('SKU')):
                product_display = f"{row['Product_Name'][:40]}... (SKU: {row['SKU']})"
            else:
                product_display = row['Product_Name'][:45] + "..." if len(row['Product_Name']) > 45 else row['Product_Name']
            st.caption(f"📉 {product_display}: {format_currency(row['Total_GMV'])}")
    else:
        st.info("No data for selected period")

st.markdown("---")

# ==========================================
# SECTION 4: ADS PERFORMANCE (Campaign-Level Aggregated)
# ==========================================
if show_ads:
    st.markdown('<p class="section-header">📢 Ads Performance</p>', unsafe_allow_html=True)
    st.caption("ข้อมูลโฆษณาระดับแคมเปญ ไม่กรองตามวันที่")

    ads_df = load_ads()

    # Note about ads data
    st.caption("📌 Ads data is campaign-level aggregated from source files (not filtered by date). ROAS = Sales / Ad Spend.")

    total_spend = ads_df['Ad_Cost'].sum()
    total_sales = ads_df['Sales'].sum()

    # Calculate weighted average ROAS (weighted by spend)
    total_roas = total_sales / total_spend if total_spend > 0 else 0
    avg_acos = (total_spend / total_sales * 100) if total_sales > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        metric_with_tooltip("💰 Total Ad Spend", format_currency(total_spend), None, "ผลรวมค่าโฆษณาจากทุกแคมเปญ ข้อมูลเป็นแบบรวมระดับแคมเปญ ไม่กรองตามวัน")
    with col2:
        metric_with_tooltip("💵 Total Ad Sales", format_currency(total_sales), None, "ยอดขายที่มาจากโฆษณา อาจรวมการขายแบบออร์แกนิคบางส่วน ข้อมูลระดับแคมเปญ")
    with col3:
        metric_with_tooltip("📈 Blended ROAS", f"{total_roas:.2f}x", None, "ยอดขายจากโฆษณา ÷ ค่าโฆษณา ค่าเฉลี่ยถ่วงน้ำหนักจากทุกแคมเปญ")
    with col4:
        metric_with_tooltip("🎯 Blended ACOS", f"{avg_acos:.1f}%", None, "ค่าโฆษณา ÷ ยอดขายจากโฆษณา × 100 ยิ่งต่ำยิ่งมีประสิทธิภาพ เป้าหมาย: <30%")

    # Campaign Health Breakdown
    st.markdown("### 🏥 Campaign Health Breakdown")

    # Define health categories with explanations
    health_info = {
        '🚀 Excellent (ROAS ≥ 5)': {
            'condition': lambda x: x['ROAS'] >= 5,
            'color': '#28a745',
            'desc': 'Highly profitable - ROAS of 5x or more'
        },
        '✅ Good (ROAS 3-5)': {
            'condition': lambda x: (x['ROAS'] >= 3) & (x['ROAS'] < 5),
            'color': '#17a2b8',
            'desc': 'Profitable - ROAS between 3x and 5x'
        },
        '⚠️ Break-even (ROAS 1-3)': {
            'condition': lambda x: (x['ROAS'] >= 1) & (x['ROAS'] < 3),
            'color': '#ffc107',
            'desc': 'Low profit margin - ROAS between 1x and 3x'
        },
        '🔴 Bleeding (No sales)': {
            'condition': lambda x: (x['ROAS'] < 1) & (x['Orders'] == 0) & (x['Clicks'] > 50),
            'color': '#dc3545',
            'desc': 'Spending with NO conversions - URGENT review needed'
        },
        '📊 Needs Monitoring': {
            'condition': lambda x: (x['ROAS'] < 1) & (x['Orders'] > 0),
            'color': '#6c757d',
            'desc': 'Losing money but has some sales'
        }
    }

    # Create tabs for each health category
    health_tabs = st.tabs([
        f"🚀 Excellent ({len(ads_df[ads_df['ROAS'] >= 5])})",
        f"✅ Good ({len(ads_df[(ads_df['ROAS'] >= 3) & (ads_df['ROAS'] < 5)])})",
        f"⚠️ Break-even ({len(ads_df[(ads_df['ROAS'] >= 1) & (ads_df['ROAS'] < 3)])})",
        f"🔴 Bleeding ({len(ads_df[(ads_df['ROAS'] < 1) & (ads_df['Orders'] == 0) & (ads_df['Clicks'] > 50)])})",
        f"📊 Needs Review ({len(ads_df[(ads_df['ROAS'] < 1) & (ads_df['Orders'] > 0)])})"
    ])

    # Excellent Campaigns
    with health_tabs[0]:
        excellent = ads_df[ads_df['ROAS'] >= 5][['Ad_Name', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales', 'Ad_Cost', 'ROAS']].copy()
        excellent.columns = ['Campaign', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales (฿)', 'Spend (฿)', 'ROAS']
        excellent['Sales (฿)'] = excellent['Sales (฿)'].apply(format_currency)
        excellent['Spend (฿)'] = excellent['Spend (฿)'].apply(format_currency)
        excellent['ROAS'] = excellent['ROAS'].apply(lambda x: f"{x:.2f}x")
        st.dataframe(excellent, use_container_width=True, hide_index=True)
        st.success("✅ These campaigns are performing excellently! Keep running them.")

    # Good Campaigns
    with health_tabs[1]:
        good = ads_df[(ads_df['ROAS'] >= 3) & (ads_df['ROAS'] < 5)][['Ad_Name', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales', 'Ad_Cost', 'ROAS']].copy()
        good.columns = ['Campaign', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales (฿)', 'Spend (฿)', 'ROAS']
        good['Sales (฿)'] = good['Sales (฿)'].apply(format_currency)
        good['Spend (฿)'] = good['Spend (฿)'].apply(format_currency)
        good['ROAS'] = good['ROAS'].apply(lambda x: f"{x:.2f}x")
        st.dataframe(good, use_container_width=True, hide_index=True)
        st.info("👍 Good performance. Consider increasing budget for top performers.")

    # Break-even Campaigns
    with health_tabs[2]:
        breakeven = ads_df[(ads_df['ROAS'] >= 1) & (ads_df['ROAS'] < 3)][['Ad_Name', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales', 'Ad_Cost', 'ROAS']].copy()
        breakeven.columns = ['Campaign', 'Status', 'Impressions', 'Clicks', 'Orders', 'Sales (฿)', 'Spend (฿)', 'ROAS']
        breakeven['Sales (฿)'] = breakeven['Sales (฿)'].apply(format_currency)
        breakeven['Spend (฿)'] = breakeven['Spend (฿)'].apply(format_currency)
        breakeven['ROAS'] = breakeven['ROAS'].apply(lambda x: f"{x:.2f}x")
        st.dataframe(breakeven, use_container_width=True, hide_index=True)
        st.warning("⚠️ Low profit margin. Review targeting and keywords to improve ROAS.")

    # Bleeding Campaigns (CRITICAL)
    with health_tabs[3]:
        bleeding = ads_df[(ads_df['ROAS'] < 1) & (ads_df['Orders'] == 0) & (ads_df['Clicks'] > 50)][['Ad_Name', 'Status', 'Impressions', 'Clicks', 'Ad_Cost', 'CTR']].copy()
        bleeding.columns = ['Campaign', 'Status', 'Impressions', 'Clicks', 'Money Wasted (฿)', 'CTR']
        bleeding['Money Wasted (฿)'] = bleeding['Money Wasted (฿)'].apply(format_currency)
        bleeding['CTR'] = bleeding['CTR'].apply(lambda x: f"{x*100:.2f}%")
        if len(bleeding) > 0:
            st.dataframe(bleeding, use_container_width=True, hide_index=True)
            total_wasted = ads_df[(ads_df['ROAS'] < 1) & (ads_df['Orders'] == 0) & (ads_df['Clicks'] > 50)]['Ad_Cost'].sum()
            st.error(f"🚨 CRITICAL: {len(bleeding)} campaigns bleeding ฿{total_wasted:,.0f} with ZERO sales! PAUSE THESE IMMEDIATELY!")
        else:
            st.success("✅ No bleeding campaigns detected!")

    # Needs Review
    with health_tabs[4]:
        review = ads_df[(ads_df['ROAS'] < 1) & (ads_df['Orders'] > 0)][['Ad_Name', 'Status', 'Orders', 'Sales', 'Ad_Cost', 'ROAS']].copy()
        review.columns = ['Campaign', 'Status', 'Orders', 'Sales (฿)', 'Spend (฿)', 'ROAS']
        review['Sales (฿)'] = review['Sales (฿)'].apply(format_currency)
        review['Spend (฿)'] = review['Spend (฿)'].apply(format_currency)
        review['ROAS'] = review['ROAS'].apply(lambda x: f"{x:.2f}x")
        if len(review) > 0:
            st.dataframe(review, use_container_width=True, hide_index=True)
            st.warning("📊 These campaigns have sales but are losing money. Optimize or pause.")
        else:
            st.success("✅ All campaigns are profitable!")

    st.markdown("---")

# ==========================================
# SECTION 5: GEOGRAPHIC (Time-Filtered)
# ==========================================
if show_geo:
    st.markdown('<p class="section-header">🗺️ Geographic Distribution</p>', unsafe_allow_html=True)
    st.caption("การกระจายยอดขายตามจังหวัด กรองข้อมูลตามช่วงเวลาที่เลือก")

    # Use daily_geographic for time-filtered analysis
    daily_geo_df = load_daily_geographic()

    if not daily_geo_df.empty and 'Date' in daily_geo_df.columns:
        # Filter by selected date range
        daily_geo_df['Date'] = pd.to_datetime(daily_geo_df['Date'])
        geo_mask = (daily_geo_df['Date'] >= pd.to_datetime(start_date)) & (daily_geo_df['Date'] <= pd.to_datetime(end_date))
        filtered_geo = daily_geo_df[geo_mask]

        # Aggregate by province (Units_Sold is the correct column name)
        agg_dict = {'GMV': 'sum', 'Orders': 'sum'}
        if 'Units_Sold' in filtered_geo.columns:
            agg_dict['Units_Sold'] = 'sum'
        geo_df = filtered_geo.groupby('Province').agg(agg_dict).reset_index()
        geo_df = geo_df.sort_values('GMV', ascending=False)
    else:
        # Fallback to static geographic data
        geo_df = load_geographic()

    if len(geo_df) > 0:
        col1, col2 = st.columns([2, 1])

        with col1:
            top_geo = geo_df.head(15)

            st.caption("15 อันดับจังหวัดที่มี GMV สูงสุด")
            fig = px.bar(
                top_geo,
                x='GMV',
                y='Province',
                orientation='h',
                title=f"Top 15 Provinces by GMV ({start_date} to {end_date})",
                color='GMV',
                color_continuous_scale='Oranges'
            )
            fig.update_layout(
                height=380,
                yaxis={'categoryorder': 'total ascending'},
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            metric_with_tooltip("Provinces Covered", len(geo_df), None, "จำนวนจังหวัดในประเทศไทยที่มีคำสั่งซื้อในช่วงเวลาที่เลือก")
            top_share = geo_df['GMV'].iloc[0] / geo_df['GMV'].sum() * 100 if len(geo_df) > 0 else 0
            top5_share = geo_df['GMV'].head(5).sum() / geo_df['GMV'].sum() * 100 if len(geo_df) > 0 else 0
            metric_with_tooltip("Top Province Share", f"{top_share:.1f}%", None, "สัดส่วน GMV ของจังหวัดที่มียอดสูงสุด")
            metric_with_tooltip("Top 5 Share", f"{top5_share:.1f}%", None, "GMV รวมของ 5 จังหวัดอันดับแรก")

            st.markdown("**Top 5 Provinces**")
            for i, row in geo_df.head(5).iterrows():
                st.markdown(f"{i+1}. {row['Province']}: {format_currency(row['GMV'])}")
    else:
        st.info("No geographic data for selected period")

    st.markdown("---")

# ==========================================
# SECTION 6: TIME COMPARISONS
# ==========================================
st.markdown('<p class="section-header">📅 Period Comparisons</p>', unsafe_allow_html=True)
st.caption("การเปรียบเทียบ GMV ระหว่างช่วงเวลาต่างๆ")

col1, col2, col3 = st.columns(3)

with col1:
    metric_with_tooltip(
        "Day-over-Day (Last Day)",
        format_currency(filtered_daily['GMV'].iloc[-1]) if len(filtered_daily) > 0 else "฿0",
        format_percent(filtered_daily['DoD_GMV_Growth'].iloc[-1]) if len(filtered_daily) > 0 else "-",
        "การเปลี่ยนแปลง GMV จากเมื่อวานเป็นวันนี้ เปอร์เซ็นต์บวก = เติบโต"
    )

with col2:
    metric_with_tooltip(
        "Week-over-Week (Last Day)",
        format_currency(filtered_daily['GMV'].iloc[-1]) if len(filtered_daily) > 0 else "฿0",
        format_percent(filtered_daily['WoW_GMV_Growth'].iloc[-1]) if len(filtered_daily) > 0 else "-",
        "การเปลี่ยนแปลง GMV เทียบกับวันเดียวกันของสัปดาห์ก่อน เปรียบเทียบช่วง 7 วัน"
    )

with col3:
    metric_with_tooltip(
        "Month-over-Month (Last Day)",
        format_currency(filtered_daily['GMV'].iloc[-1]) if len(filtered_daily) > 0 else "฿0",
        format_percent(filtered_daily['MoM_GMV_Growth'].iloc[-1]) if len(filtered_daily) > 0 else "-",
        "การเปลี่ยนแปลง GMV เทียบกับวันเดียวกันของเดือนก่อน เปรียบเทียบช่วง ~30 วัน"
    )

st.markdown("---")

# ==========================================
# SECTION 8: RECENT PERFORMANCE
# ==========================================
st.markdown('<p class="section-header">📋 Recent Daily Performance (Last 14 Days)</p>', unsafe_allow_html=True)
st.caption("ตารางแสดงข้อมูลรายวัน 14 วันล่าสุด: วันที่, GMV, คำสั่งซื้อ, AOV, กลุ่ม, การเปลี่ยนแปลงรายวัน")

recent = filtered_daily.tail(14)[['Date', 'GMV', 'Orders', 'AOV', 'GMV_Segment', 'DoD_GMV_Growth']].copy()
recent['Date'] = pd.to_datetime(recent['Date']).dt.strftime('%Y-%m-%d')
recent['GMV'] = recent['GMV'].apply(format_currency)
recent['AOV'] = recent['AOV'].apply(format_currency)
recent['DoD_GMV_Growth'] = recent['DoD_GMV_Growth'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "-")
recent.columns = ['Date', 'GMV', 'Orders', 'AOV', 'Segment', 'DoD %']

st.dataframe(recent, use_container_width=True, hide_index=True)

# ==========================================
# FOOTER
# ==========================================
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.85rem;">
        <p>Shopee Dashboard v2.0 | Powered by DuckDB & Streamlit</p>
        <p>Share: http://100.66.69.21:8501</p>
    </div>
    """, unsafe_allow_html=True)
