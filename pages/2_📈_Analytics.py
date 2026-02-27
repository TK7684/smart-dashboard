"""
Analytics & Predictive Models Page
Regression analysis, forecasting, and performance prediction
Supports TikTok, Shopee, and Combined data views
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
    safe_float, format_currency, format_number, load_live_data, load_video_data,
    load_tiktok_live, load_tiktok_video, load_daily_sales, load_tiktok_orders,
    load_all_orders, COMMON_STYLES
)

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="Analytics & Prediction - Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply common styles
st.markdown(COMMON_STYLES, unsafe_allow_html=True)

# ==========================================
# MACHINE LEARNING IMPORTS
# ==========================================
try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def prepare_daily_gmv_for_platform(tiktok_live, tiktok_orders, shopee_daily, analysis_type, start_date, end_date):
    """Prepare daily GMV data based on selected platform"""
    daily_data = pd.DataFrame()

    if analysis_type == "TikTok Content":
        # Use TikTok Orders for GMV data (more complete), fallback to Live if not available
        if not tiktok_orders.empty and 'Order_Date' in tiktok_orders.columns:
            tiktok_orders['Date'] = pd.to_datetime(tiktok_orders['Order_Date']).dt.date
            daily_data = tiktok_orders.groupby('Date').agg({
                'Net_Sales': lambda x: x.apply(safe_float).sum(),
                'Quantity': lambda x: x.apply(safe_float).sum()
            }).reset_index()
            daily_data = daily_data.rename(columns={'Net_Sales': 'GMV', 'Quantity': 'Orders'})
            daily_data['Platform'] = 'TikTok'
        elif not tiktok_live.empty and 'Start_DateTime' in tiktok_live.columns:
            tiktok_live['Date'] = tiktok_live['Start_DateTime'].dt.date
            daily_data = tiktok_live.groupby('Date').agg({
                'GMV': lambda x: x.apply(safe_float).sum(),
                'Orders': lambda x: x.apply(safe_float).sum(),
            }).reset_index()
            daily_data['Platform'] = 'TikTok'

    elif analysis_type == "Shopee Sales":
        if not shopee_daily.empty:
            shopee_daily = shopee_daily.copy()
            shopee_daily['Date'] = pd.to_datetime(shopee_daily['Date']).dt.date
            mask = (shopee_daily['Date'] >= start_date) & (shopee_daily['Date'] <= end_date)
            daily_data = shopee_daily[mask].copy()
            daily_data = daily_data.rename(columns={'Total_GMV': 'GMV', 'Total_Orders': 'Orders'})
            daily_data = daily_data[['Date', 'GMV', 'Orders']].reset_index(drop=True)
            daily_data['Platform'] = 'Shopee'

    else:  # Combined Overview
        frames = []

        # TikTok data - use orders for GMV
        if not tiktok_orders.empty and 'Order_Date' in tiktok_orders.columns:
            tiktok_orders['Date'] = pd.to_datetime(tiktok_orders['Order_Date']).dt.date
            tiktok_daily = tiktok_orders.groupby('Date').agg({
                'Net_Sales': lambda x: x.apply(safe_float).sum(),
                'Quantity': lambda x: x.apply(safe_float).sum()
            }).reset_index()
            tiktok_daily = tiktok_daily.rename(columns={'Net_Sales': 'GMV', 'Quantity': 'Orders'})
            tiktok_daily['Platform'] = 'TikTok'
            frames.append(tiktok_daily)

        # Shopee data
        if not shopee_daily.empty:
            shopee_copy = shopee_daily.copy()
            shopee_copy['Date'] = pd.to_datetime(shopee_copy['Date']).dt.date
            mask = (shopee_copy['Date'] >= start_date) & (shopee_copy['Date'] <= end_date)
            shopee_filtered = shopee_copy[mask].copy()
            shopee_filtered = shopee_filtered.rename(columns={'Total_GMV': 'GMV', 'Total_Orders': 'Orders'})
            shopee_filtered = shopee_filtered[['Date', 'GMV', 'Orders']]
            shopee_filtered['Platform'] = 'Shopee'
            frames.append(shopee_filtered)

        if frames:
            daily_data = pd.concat(frames, ignore_index=True)

    if not daily_data.empty:
        daily_data['Date'] = pd.to_datetime(daily_data['Date'])
        daily_data = daily_data.sort_values('Date')

    return daily_data


def prepare_session_data_for_platform(tiktok_live, tiktok_orders, shopee_daily, analysis_type):
    """Prepare session/transaction level data for correlation and prediction"""
    session_data = pd.DataFrame()

    if analysis_type == "TikTok Content":
        # Use TikTok Orders for GMV data, or Live for engagement metrics
        if not tiktok_orders.empty:
            session_data = tiktok_orders.copy()
            session_data = session_data.rename(columns={'Net_Sales': 'GMV'})
            session_data['Platform'] = 'TikTok'
        elif not tiktok_live.empty:
            session_data = tiktok_live.copy()
            session_data['Platform'] = 'TikTok'

    elif analysis_type == "Shopee Sales":
        if not shopee_daily.empty:
            # Aggregate to daily level for Shopee (no session-level data)
            shopee_daily = shopee_daily.copy()
            shopee_daily['Date'] = pd.to_datetime(shopee_daily['Date'])
            session_data = shopee_daily.rename(columns={
                'Total_GMV': 'GMV',
                'Total_Orders': 'Orders',
                'Total_Quantity': 'Quantity'
            })
            session_data['Platform'] = 'Shopee'

    else:  # Combined
        frames = []

        if not tiktok_orders.empty:
            tiktok_copy = tiktok_orders.copy()
            tiktok_copy = tiktok_copy.rename(columns={'Net_Sales': 'GMV'})
            tiktok_copy['Platform'] = 'TikTok'
            frames.append(tiktok_copy)

        if not shopee_daily.empty:
            shopee_copy = shopee_daily.copy()
            shopee_copy['Date'] = pd.to_datetime(shopee_copy['Date'])
            shopee_copy = shopee_copy.rename(columns={
                'Total_GMV': 'GMV',
                'Total_Orders': 'Orders',
                'Total_Quantity': 'Quantity'
            })
            shopee_copy['Platform'] = 'Shopee'
            frames.append(shopee_copy)

        if frames:
            session_data = pd.concat(frames, ignore_index=True)

    return session_data


# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 📈 Analytics & Prediction")
    st.markdown("---")

    # Load data for date range
    daily_df = load_daily_sales()
    min_date = pd.to_datetime(daily_df['Date']).min().date()
    max_date = pd.to_datetime(daily_df['Date']).max().date()

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
    default_start = st.session_state.get('date_range', (max_date - timedelta(days=90), max_date))[0]
    default_end = st.session_state.get('date_range', (max_date - timedelta(days=90), max_date))[1]

    date_range = st.date_input(
        "Select period",
        value=(default_start, default_end),
        min_value=min_date,
        max_value=max_date,
        label_visibility="collapsed"
    )

    st.markdown("---")

    # Analysis type
    st.markdown("#### 🔧 Analysis Settings")
    analysis_type = st.selectbox(
        "Platform Focus",
        options=["TikTok Content", "Shopee Sales", "Combined Overview"],
        index=0
    )

    st.markdown("---")

    # Navigation
    st.markdown("📌 **Navigation**")
    st.page_link("dashboard.py", label="📊 Main Dashboard", icon="📊")
    st.page_link("pages/1_🎬_Content_Commerce.py", label="🎬 Content Commerce", icon="🎬")
    st.page_link("pages/2_📈_Analytics.py", label="📈 Analytics", icon="📈")

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
st.markdown('<p class="main-header">📈 Analytics & Predictive Models</p>', unsafe_allow_html=True)
st.markdown(f"**Period:** {start_date} to {end_date} | **Platform:** {analysis_type}")

if not SKLEARN_AVAILABLE:
    st.warning("⚠️ scikit-learn not installed. Some features limited. Run: `pip install scikit-learn`")

# ==========================================
# LOAD DATA
# ==========================================
# Load TikTok data
tiktok_live_raw = load_tiktok_live()
tiktok_video_raw = load_tiktok_video()
tiktok_orders_raw = load_tiktok_orders()

# Load Shopee data
shopee_live_raw = load_live_data()
shopee_video_raw = load_video_data()
daily_sales_raw = load_daily_sales()

# Filter TikTok by date
tiktok_live = pd.DataFrame()
tiktok_video = pd.DataFrame()
tiktok_orders = pd.DataFrame()

if not tiktok_live_raw.empty and 'Start_Time' in tiktok_live_raw.columns:
    tiktok_live_raw['Start_DateTime'] = pd.to_datetime(tiktok_live_raw['Start_Time'], errors='coerce')
    mask = (tiktok_live_raw['Start_DateTime'] >= pd.to_datetime(start_date)) & (tiktok_live_raw['Start_DateTime'] <= pd.to_datetime(end_date))
    tiktok_live = tiktok_live_raw[mask].copy()

if not tiktok_video_raw.empty and 'Post_Time' in tiktok_video_raw.columns:
    tiktok_video_raw['Post_DateTime'] = pd.to_datetime(tiktok_video_raw['Post_Time'], errors='coerce')
    mask = (tiktok_video_raw['Post_DateTime'] >= pd.to_datetime(start_date)) & (tiktok_video_raw['Post_DateTime'] <= pd.to_datetime(end_date))
    tiktok_video = tiktok_video_raw[mask].copy()

if not tiktok_orders_raw.empty and 'Order_Date' in tiktok_orders_raw.columns:
    tiktok_orders_raw['Order_Date'] = pd.to_datetime(tiktok_orders_raw['Order_Date'], errors='coerce')
    mask = (tiktok_orders_raw['Order_Date'] >= pd.to_datetime(start_date)) & (tiktok_orders_raw['Order_Date'] <= pd.to_datetime(end_date))
    tiktok_orders = tiktok_orders_raw[mask].copy()

# Prepare platform-specific data
daily_gmv = prepare_daily_gmv_for_platform(tiktok_live, tiktok_orders, daily_sales_raw, analysis_type, start_date, end_date)
session_data = prepare_session_data_for_platform(tiktok_live, tiktok_orders, daily_sales_raw, analysis_type)

# Platform colors
PLATFORM_COLORS = {
    'TikTok': '#000000',
    'Shopee': '#ee4d2d',
    'Combined': '#6366f1'
}

# ==========================================
# TABS
# ==========================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Correlation Analysis",
    "🔮 Sales Forecasting",
    "🎯 Performance Prediction",
    "📋 Feature Importance",
    "🌡️ Seasonality & Anomalies"
])

# ==========================================
# TAB 1: CORRELATION ANALYSIS
# ==========================================
with tab1:
    st.markdown("## 📊 Correlation Analysis")
    st.markdown(f"Understand which metrics drive GMV and engagement - **{analysis_type}**")

    if not session_data.empty:
        platform_label = analysis_type.replace(" Content", "").replace(" Sales", "").replace(" Overview", "")
        st.markdown(f"### {platform_label} - Correlation Matrix")

        # Prepare numeric columns based on platform
        if analysis_type == "TikTok Content":
            # TikTok Orders columns
            numeric_cols = ['GMV', 'Quantity', 'Original_Price', 'Gross_Sales', 'Platform_Discount',
                           'Seller_Discount', 'Total_Discount', 'Shipping_Fee', 'Weight']
        elif analysis_type == "Shopee Sales":
            numeric_cols = ['GMV', 'Orders', 'Quantity', 'Total_AOV', 'Total_Net_Revenue']
        else:  # Combined
            numeric_cols = ['GMV', 'Orders']

        available_cols = [c for c in numeric_cols if c in session_data.columns]

        if len(available_cols) >= 2:
            corr_df = session_data[available_cols].apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()

            if len(corr_df) > 5:
                corr_matrix = corr_df.corr()

                fig = go.Figure(data=go.Heatmap(
                    z=corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    colorscale='RdBu',
                    zmid=0,
                    text=[[f'{v:.2f}' for v in row] for row in corr_matrix.values],
                    texttemplate='%{text}',
                    textfont={'size': 10}
                ))
                fig.update_layout(
                    title=f"{platform_label} Metrics Correlation",
                    height=500,
                    width=700
                )
                st.plotly_chart(fig, use_container_width=True)

                # Key insights
                st.markdown("### 🔍 Key Insights")
                if 'GMV' in corr_matrix.columns:
                    gmv_corr = corr_matrix['GMV'].drop('GMV').abs().sort_values(ascending=False)
                    st.markdown(f"**Top factors correlated with GMV:**")
                    for i, (col, val) in enumerate(gmv_corr.head(5).items(), 1):
                        st.write(f"{i}. **{col}**: {val:.3f}")

                # Scatter plot
                if 'Orders' in available_cols:
                    st.markdown("### 📈 GMV vs Orders Relationship")
                    fig_scatter = go.Figure()

                    if 'Platform' in session_data.columns and analysis_type == "Combined Overview":
                        for platform in session_data['Platform'].unique():
                            platform_df = session_data[session_data['Platform'] == platform]
                            fig_scatter.add_trace(go.Scatter(
                                x=platform_df['Orders'].apply(safe_float),
                                y=platform_df['GMV'].apply(safe_float),
                                mode='markers',
                                name=platform,
                                marker=dict(color=PLATFORM_COLORS.get(platform, '#666666'), size=8),
                            ))
                    else:
                        fig_scatter.add_trace(go.Scatter(
                            x=corr_df['Orders'],
                            y=corr_df['GMV'],
                            mode='markers',
                            marker=dict(size=8, color=PLATFORM_COLORS.get(platform_label, '#ee4d2d')),
                            name='Data Points'
                        ))

                    fig_scatter.update_layout(
                        title="GMV vs Orders",
                        xaxis_title="Orders",
                        yaxis_title="GMV (฿)",
                        height=400
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
            else:
                st.info("Not enough data for correlation analysis (need >5 records)")
        else:
            st.info(f"Not enough numeric columns available for {analysis_type}")
    else:
        st.info(f"No data available for {analysis_type} in the selected period")

# ==========================================
# TAB 2: SALES FORECASTING
# ==========================================
with tab2:
    st.markdown("## 🔮 Sales Forecasting")
    st.markdown(f"Predict future GMV using time series analysis - **{analysis_type}**")

    if not daily_gmv.empty and len(daily_gmv) >= 7:
        # Aggregate by date if multiple platforms
        if 'Platform' in daily_gmv.columns and analysis_type == "Combined Overview":
            daily_agg = daily_gmv.groupby('Date').agg({
                'GMV': 'sum',
                'Orders': 'sum'
            }).reset_index()
        else:
            daily_agg = daily_gmv[['Date', 'GMV', 'Orders']].copy()

        daily_agg = daily_agg.sort_values('Date')

        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("### 📊 GMV Trend & Forecast")

            # Simple exponential smoothing forecast
            alpha = 0.3
            daily_agg['GMV_Smooth'] = daily_agg['GMV'].ewm(alpha=alpha).mean()

            # Calculate trend
            forecast_dates = None
            forecast_values = None

            if len(daily_agg) >= 14 and SKLEARN_AVAILABLE:
                X = np.arange(len(daily_agg)).reshape(-1, 1)
                y = daily_agg['GMV'].values

                model = LinearRegression()
                model.fit(X, y)
                trend = model.predict(X)
                daily_agg['Trend'] = trend

                # Forecast next 7 days
                future_X = np.arange(len(daily_agg), len(daily_agg) + 7).reshape(-1, 1)
                forecast_values = model.predict(future_X)

                last_date = daily_agg['Date'].max()
                forecast_dates = [last_date + timedelta(days=i+1) for i in range(7)]

            # Plot
            fig = go.Figure()

            # Plot by platform for combined view
            if 'Platform' in daily_gmv.columns and analysis_type == "Combined Overview":
                for platform in daily_gmv['Platform'].unique():
                    platform_data = daily_gmv[daily_gmv['Platform'] == platform].sort_values('Date')
                    fig.add_trace(go.Scatter(
                        x=platform_data['Date'],
                        y=platform_data['GMV'],
                        name=f'{platform} GMV',
                        mode='lines+markers',
                        line=dict(color=PLATFORM_COLORS.get(platform, '#666666'), width=2),
                        marker=dict(size=6)
                    ))
                # Add total line
                fig.add_trace(go.Scatter(
                    x=daily_agg['Date'],
                    y=daily_agg['GMV'],
                    name='Total GMV',
                    mode='lines',
                    line=dict(color='#6366f1', width=3, dash='dot'),
                ))
            else:
                platform_label = analysis_type.replace(" Content", "").replace(" Sales", "").replace(" Overview", "")
                fig.add_trace(go.Scatter(
                    x=daily_agg['Date'],
                    y=daily_agg['GMV'],
                    name='Actual GMV',
                    mode='lines+markers',
                    line=dict(color=PLATFORM_COLORS.get(platform_label, '#ee4d2d'), width=2),
                    marker=dict(size=6)
                ))

            # Smoothed
            fig.add_trace(go.Scatter(
                x=daily_agg['Date'],
                y=daily_agg['GMV_Smooth'],
                name='Smoothed (EWMA)',
                line=dict(color='#0066cc', width=2, dash='dot')
            ))

            # Trend and forecast
            if SKLEARN_AVAILABLE and len(daily_agg) >= 14:
                fig.add_trace(go.Scatter(
                    x=daily_agg['Date'],
                    y=daily_agg['Trend'],
                    name='Trend Line',
                    line=dict(color='#28a745', width=2, dash='dash')
                ))

                if forecast_dates is not None:
                    fig.add_trace(go.Scatter(
                        x=forecast_dates,
                        y=forecast_values,
                        name='7-Day Forecast',
                        mode='lines+markers',
                        line=dict(color='#9c27b0', width=2),
                        marker=dict(size=8, symbol='diamond')
                    ))

            fig.update_layout(
                title="Daily GMV with Trend & Forecast",
                xaxis_title="Date",
                yaxis_title="GMV (฿)",
                hovermode='x unified',
                height=450,
                legend=dict(orientation="h", y=1.1)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### 📈 Forecast Summary")

            if SKLEARN_AVAILABLE and len(daily_agg) >= 14:
                avg_gmv = daily_agg['GMV'].mean()
                last_gmv = daily_agg['GMV'].iloc[-1]
                trend_dir = "📈 Upward" if model.coef_[0] > 0 else "📉 Downward"

                st.metric("Avg Daily GMV", format_currency(avg_gmv))
                st.metric("Last Day GMV", format_currency(last_gmv))
                st.metric("Trend Direction", trend_dir)

                if forecast_dates is not None:
                    st.markdown("---")
                    st.markdown("**7-Day Forecast:**")
                    for i, (date, gmv) in enumerate(zip(forecast_dates[:7], forecast_values[:7])):
                        st.write(f"• {date.strftime('%Y-%m-%d')}: {format_currency(gmv)}")

                    total_forecast = sum(forecast_values[:7])
                    st.markdown(f"**Total 7-Day Forecast:** {format_currency(total_forecast)}")
            else:
                st.info("Need ≥14 days of data for forecasting")

        # Model accuracy
        if SKLEARN_AVAILABLE and len(daily_agg) >= 14:
            st.markdown("### 🎯 Model Accuracy")
            y_pred = model.predict(X)
            r2 = r2_score(y, y_pred)
            mae = mean_absolute_error(y, y_pred)
            rmse = np.sqrt(mean_squared_error(y, y_pred))

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("R² Score", f"{r2:.3f}", help="How well the model fits data (1.0 = perfect)")
            with col2:
                st.metric("MAE", format_currency(mae), help="Average prediction error")
            with col3:
                st.metric("RMSE", format_currency(rmse), help="Root mean square error")
    else:
        st.info(f"Need at least 7 days of data for forecasting. Current: {len(daily_gmv)} days")

# ==========================================
# TAB 3: PERFORMANCE PREDICTION
# ==========================================
with tab3:
    st.markdown("## 🎯 Performance Prediction Model")
    st.markdown(f"Predict GMV based on metrics using Random Forest - **{analysis_type}**")

    if not session_data.empty and SKLEARN_AVAILABLE:
        # Define features based on platform
        if analysis_type == "TikTok Content":
            # TikTok Orders has different columns than TikTok Live
            feature_cols = ['Quantity', 'Original_Price', 'Gross_Sales', 'Total_Discount', 'Shipping_Fee']
        elif analysis_type == "Shopee Sales":
            feature_cols = ['Orders', 'Quantity', 'Total_AOV']
        else:
            feature_cols = ['Orders']

        available_features = [c for c in feature_cols if c in session_data.columns]

        if len(available_features) >= 1 and len(session_data) >= 20:
            # Create feature matrix
            X = session_data[available_features].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
            y = session_data['GMV'].apply(safe_float)

            # Remove zero GMV for better training
            mask = y > 0
            X = X[mask]
            y = y[mask]

            if len(X) >= 15:
                # Split data
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

                # Scale features
                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train)
                X_test_scaled = scaler.transform(X_test)

                # Train Random Forest
                rf_model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
                rf_model.fit(X_train_scaled, y_train)

                # Predictions
                y_pred_train = rf_model.predict(X_train_scaled)
                y_pred_test = rf_model.predict(X_test_scaled)

                # Metrics
                train_r2 = r2_score(y_train, y_pred_train)
                test_r2 = r2_score(y_test, y_pred_test)
                test_mae = mean_absolute_error(y_test, y_pred_test)

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 🤖 Model Performance")
                    st.metric("Training R²", f"{train_r2:.3f}")
                    st.metric("Test R²", f"{test_r2:.3f}")
                    st.metric("Mean Absolute Error", format_currency(test_mae))

                    if test_r2 > 0.7:
                        st.success("✅ Good predictive power! Model explains {:.0f}% of GMV variance.".format(test_r2 * 100))
                    elif test_r2 > 0.4:
                        st.warning("⚠️ Moderate predictive power. Consider more data or features.")
                    else:
                        st.error("❌ Low predictive power. GMV may depend on external factors.")

                with col2:
                    st.markdown("### 📊 Actual vs Predicted")
                    fig = go.Figure()

                    fig.add_trace(go.Scatter(
                        x=y_test,
                        y=y_pred_test,
                        mode='markers',
                        marker=dict(size=10, color=PLATFORM_COLORS.get(
                            analysis_type.replace(" Content", "").replace(" Sales", "").replace(" Overview", ""),
                            '#ee4d2d'
                        )),
                        name='Test Data'
                    ))

                    max_val = max(y_test.max(), max(y_pred_test)) if len(y_pred_test) > 0 else 1
                    fig.add_trace(go.Scatter(
                        x=[0, max_val],
                        y=[0, max_val],
                        mode='lines',
                        line=dict(color='#28a745', dash='dash'),
                        name='Perfect Prediction'
                    ))

                    fig.update_layout(
                        title="Actual vs Predicted GMV",
                        xaxis_title="Actual GMV (฿)",
                        yaxis_title="Predicted GMV (฿)",
                        height=350
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Prediction tool
                st.markdown("---")
                st.markdown("### 🔮 GMV Predictor Tool")
                st.markdown("Enter expected metrics to predict GMV:")

                pred_cols = st.columns(len(available_features))
                user_inputs = {}
                for i, col_name in enumerate(available_features):
                    with pred_cols[i]:
                        default_val = int(X[col_name].mean())
                        user_inputs[col_name] = st.number_input(col_name, value=default_val, step=100)

                if st.button("Predict GMV", type="primary"):
                    user_features = np.array([[user_inputs[c] for c in available_features]])
                    user_scaled = scaler.transform(user_features)
                    predicted_gmv = rf_model.predict(user_scaled)[0]

                    st.metric("Predicted GMV", format_currency(predicted_gmv))
                    st.caption("Note: Prediction based on historical patterns. Actual results may vary.")
            else:
                st.info("Need at least 15 records with GMV > 0 for prediction model")
        else:
            st.info(f"Need at least 20 records with metrics for prediction model. Current: {len(session_data)} records")
    else:
        if not SKLEARN_AVAILABLE:
            st.error("scikit-learn required. Install with: `pip install scikit-learn`")
        else:
            st.info(f"No data available for {analysis_type}")

# ==========================================
# TAB 4: FEATURE IMPORTANCE
# ==========================================
with tab4:
    st.markdown("## 📋 Feature Importance Analysis")
    st.markdown(f"Which metrics have the biggest impact on GMV? - **{analysis_type}**")

    if not session_data.empty and SKLEARN_AVAILABLE:
        # Define features based on platform
        if analysis_type == "TikTok Content":
            # TikTok Orders has different columns than TikTok Live
            feature_cols = ['Quantity', 'Original_Price', 'Gross_Sales', 'Platform_Discount',
                           'Seller_Discount', 'Shipping_Fee', 'Weight']
        elif analysis_type == "Shopee Sales":
            feature_cols = ['Orders', 'Quantity', 'Total_AOV', 'Total_Net_Revenue']
        else:
            feature_cols = ['Orders']

        available_features = [c for c in feature_cols if c in session_data.columns]

        if len(available_features) >= 1:
            X = session_data[available_features].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
            y = session_data['GMV'].apply(safe_float)

            mask = y > 0
            X = X[mask]
            y = y[mask]

            if len(X) >= 15:
                # Train Random Forest for feature importance
                rf = RandomForestRegressor(n_estimators=100, random_state=42)
                rf.fit(X, y)

                # Get feature importance
                importance_df = pd.DataFrame({
                    'Feature': available_features,
                    'Importance': rf.feature_importances_
                }).sort_values('Importance', ascending=True)

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 📊 Feature Importance Chart")
                    fig = go.Figure(go.Bar(
                        x=importance_df['Importance'],
                        y=importance_df['Feature'],
                        orientation='h',
                        marker_color=PLATFORM_COLORS.get(
                            analysis_type.replace(" Content", "").replace(" Sales", "").replace(" Overview", ""),
                            '#ee4d2d'
                        )
                    ))
                    fig.update_layout(
                        title="What Drives GMV?",
                        xaxis_title="Importance Score",
                        yaxis_title="",
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("### 💡 Recommendations")

                    top_3 = importance_df.tail(3)['Feature'].tolist()
                    st.markdown(f"**Top 3 GMV Drivers:**")
                    for i, feature in enumerate(top_3, 1):
                        st.write(f"{i}. **{feature}**")

                    st.markdown("---")
                    st.markdown("**Action Items:**")

                    if 'Viewers' in top_3:
                        st.write("• Focus on increasing live viewership")
                    if 'Products_Sold' in top_3:
                        st.write("• Showcase more products during live")
                    if 'Orders' in top_3:
                        st.write("• Focus on converting viewers to buyers")
                    if 'Quantity' in top_3:
                        st.write("• Encourage larger basket sizes")
                    if 'Engagement_Rate' in top_3:
                        st.write("• Improve audience interaction")
                    if 'New_Followers' in top_3:
                        st.write("• Build follower base for long-term growth")

                # Statistical summary
                st.markdown("---")
                st.markdown("### 📈 Statistical Summary")
                st.dataframe(
                    session_data[available_features + ['GMV']].describe().round(2),
                    use_container_width=True
                )
            else:
                st.info("Need ≥15 records for feature importance analysis")
        else:
            st.info("Need at least 1 metric for feature analysis")
    else:
        if not SKLEARN_AVAILABLE:
            st.error("scikit-learn required")
        else:
            st.info(f"No data available for {analysis_type}")

# ==========================================
# TAB 5: SEASONALITY & ANOMALIES
# ==========================================
with tab5:
    st.markdown("## 🌡️ Seasonality & Anomaly Detection")
    st.markdown(f"Identify patterns and unusual sales days - **{analysis_type}**")

    if not daily_gmv.empty and len(daily_gmv) >= 30:
        # Aggregate by date if multiple platforms
        if 'Platform' in daily_gmv.columns and analysis_type == "Combined Overview":
            daily_agg = daily_gmv.groupby('Date').agg({
                'GMV': 'sum',
                'Orders': 'sum'
            }).reset_index()
        else:
            daily_agg = daily_gmv[['Date', 'GMV', 'Orders']].copy()

        daily_agg = daily_agg.sort_values('Date').reset_index(drop=True)

        # ==========================================
        # SEASONALITY ANALYSIS
        # ==========================================
        st.markdown("### 📅 Thai E-Commerce Seasonality")

        # Add day-of-week and day-of-month analysis
        daily_agg['DayOfWeek'] = pd.to_datetime(daily_agg['Date']).dt.dayofweek
        daily_agg['DayOfMonth'] = pd.to_datetime(daily_agg['Date']).dt.day
        daily_agg['Month'] = pd.to_datetime(daily_agg['Date']).dt.month
        daily_agg['WeekOfYear'] = pd.to_datetime(daily_agg['Date']).dt.isocalendar().week

        col1, col2 = st.columns(2)

        with col1:
            # Day of Week pattern
            dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            dow_avg = daily_agg.groupby('DayOfWeek')['GMV'].mean()
            dow_avg.index = dow_names

            fig = go.Figure(go.Bar(
                x=dow_avg.index,
                y=dow_avg.values,
                marker_color=['#ee4d2d' if v == dow_avg.max() else '#666666' for v in dow_avg.values],
                text=[format_currency(v) for v in dow_avg.values],
                textposition='outside'
            ))
            fig.update_layout(
                title="Average GMV by Day of Week",
                height=300,
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Day of Month pattern (Payday effect)
            # Thai paydays are typically 1st and 15th
            dom_avg = daily_agg.groupby('DayOfMonth')['GMV'].mean()

            # Highlight payday days
            colors = ['#28a745' if d in [1, 2, 15, 16, 25, 26] else '#666666' for d in dom_avg.index]

            fig = go.Figure(go.Bar(
                x=dom_avg.index,
                y=dom_avg.values,
                marker_color=colors
            ))
            fig.update_layout(
                title="Average GMV by Day of Month (Green = Payday)",
                height=300,
                showlegend=False,
                xaxis=dict(dtick=5)
            )
            st.plotly_chart(fig, use_container_width=True)

        # ==========================================
        # DOUBLE DAY ANALYSIS (Thai E-Commerce Events)
        # ==========================================
        st.markdown("---")
        st.markdown("### 🎉 Double Day Campaign Performance")

        # Thai double days: 1.1, 2.2, 3.3, 4.4, etc. + major sales
        double_days = {
            '1.1': (1, 1), '2.2': (2, 2), '3.3': (3, 3), '4.4': (4, 4),
            '5.5': (5, 5), '6.6': (6, 6), '7.7': (7, 7), '8.8': (8, 8),
            '9.9': (9, 9), '10.10': (10, 10), '11.11': (11, 11), '12.12': (12, 12),
            'Mega 9.9': (9, 9), 'Mega 10.10': (10, 10), 'Mega 11.11': (11, 11), 'Mega 12.12': (12, 12),
        }

        # Find double days in the data
        double_day_data = []
        for name, (month, day) in double_days.items():
            mask = (daily_agg['Month'] == month) & (daily_agg['DayOfMonth'] == day)
            if mask.any():
                dd_data = daily_agg[mask]
                for _, row in dd_data.iterrows():
                    double_day_data.append({
                        'Campaign': name,
                        'Date': row['Date'],
                        'GMV': row['GMV'],
                        'Orders': row['Orders']
                    })

        if double_day_data:
            dd_df = pd.DataFrame(double_day_data)

            # Compare to average
            avg_gmv = daily_agg['GMV'].mean()
            dd_df['vs_Avg'] = ((dd_df['GMV'] - avg_gmv) / avg_gmv * 100).round(1)

            # Sort by date
            dd_df = dd_df.sort_values('Date', ascending=False)

            # Display
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=dd_df['Campaign'],
                y=dd_df['GMV'],
                marker_color=['#28a745' if v > 0 else '#dc3545' for v in dd_df['vs_Avg']],
                text=[f"{v:+.0f}%" for v in dd_df['vs_Avg']],
                textposition='outside'
            ))
            fig.add_hline(y=avg_gmv, line_dash="dash", line_color="#666666",
                         annotation_text=f"Average: {format_currency(avg_gmv)}")
            fig.update_layout(
                title="Double Day GMV vs Average",
                height=350,
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

            # Table
            dd_display = dd_df.copy()
            dd_display['GMV'] = dd_display['GMV'].apply(format_currency)
            dd_display['vs_Avg'] = dd_display['vs_Avg'].apply(lambda x: f"{x:+.1f}%")
            dd_display['Date'] = pd.to_datetime(dd_display['Date']).dt.strftime('%Y-%m-%d')
            st.dataframe(dd_display[['Campaign', 'Date', 'GMV', 'vs_Avg']], use_container_width=True, hide_index=True)
        else:
            st.info("No double day campaigns found in the selected date range")

        # ==========================================
        # ANOMALY DETECTION
        # ==========================================
        st.markdown("---")
        st.markdown("### 🚨 Anomaly Detection")

        # Calculate rolling statistics
        window = 7
        daily_agg['Rolling_Mean'] = daily_agg['GMV'].rolling(window=window, min_periods=1).mean()
        daily_agg['Rolling_Std'] = daily_agg['GMV'].rolling(window=window, min_periods=1).std()

        # Z-score based anomaly detection
        daily_agg['Z_Score'] = (daily_agg['GMV'] - daily_agg['Rolling_Mean']) / daily_agg['Rolling_Std'].replace(0, 1)
        daily_agg['Is_Anomaly'] = daily_agg['Z_Score'].abs() > 2

        # Plot with anomalies highlighted
        fig = go.Figure()

        # Normal days
        normal = daily_agg[~daily_agg['Is_Anomaly']]
        fig.add_trace(go.Scatter(
            x=normal['Date'],
            y=normal['GMV'],
            mode='lines+markers',
            name='Normal',
            line=dict(color='#666666', width=1),
            marker=dict(size=4)
        ))

        # High anomalies
        high_anomaly = daily_agg[(daily_agg['Is_Anomaly']) & (daily_agg['Z_Score'] > 0)]
        if not high_anomaly.empty:
            fig.add_trace(go.Scatter(
                x=high_anomaly['Date'],
                y=high_anomaly['GMV'],
                mode='markers',
                name='Unusually High',
                marker=dict(color='#28a745', size=12, symbol='star')
            ))

        # Low anomalies
        low_anomaly = daily_agg[(daily_agg['Is_Anomaly']) & (daily_agg['Z_Score'] < 0)]
        if not low_anomaly.empty:
            fig.add_trace(go.Scatter(
                x=low_anomaly['Date'],
                y=low_anomaly['GMV'],
                mode='markers',
                name='Unusually Low',
                marker=dict(color='#dc3545', size=12, symbol='x')
            ))

        # Rolling average
        fig.add_trace(go.Scatter(
            x=daily_agg['Date'],
            y=daily_agg['Rolling_Mean'],
            mode='lines',
            name='7-Day Average',
            line=dict(color='#0066cc', width=2, dash='dot')
        ))

        fig.update_layout(
            title="GMV with Anomaly Detection (Z-Score > 2)",
            xaxis_title="Date",
            yaxis_title="GMV (฿)",
            height=400,
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Anomaly summary
        anomalies = daily_agg[daily_agg['Is_Anomaly']].copy()
        if not anomalies.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 📈 High GMV Days (Opportunities)")
                high = anomalies[anomalies['Z_Score'] > 0].sort_values('GMV', ascending=False)
                if not high.empty:
                    for _, row in high.head(5).iterrows():
                        date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
                        st.write(f"• **{date_str}**: {format_currency(row['GMV'])} (+{row['Z_Score']:.1f}σ)")
                else:
                    st.write("No unusually high days detected")

            with col2:
                st.markdown("#### 📉 Low GMV Days (Investigate)")
                low = anomalies[anomalies['Z_Score'] < 0].sort_values('GMV')
                if not low.empty:
                    for _, row in low.head(5).iterrows():
                        date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
                        st.write(f"• **{date_str}**: {format_currency(row['GMV'])} ({row['Z_Score']:.1f}σ)")
                else:
                    st.write("No unusually low days detected")

            st.markdown("---")
            st.markdown(f"**Total Anomalies:** {len(anomalies)} out of {len(daily_agg)} days ({len(anomalies)/len(daily_agg)*100:.1f}%)")
        else:
            st.success("✅ No significant anomalies detected in the selected period")

        # ==========================================
        # MONTH-OVER-MONTH COMPARISON
        # ==========================================
        st.markdown("---")
        st.markdown("### 📊 Month-over-Month Trend")

        monthly_agg = daily_agg.groupby(daily_agg['Date'].dt.to_period('M')).agg({
            'GMV': 'sum',
            'Orders': 'sum'
        }).reset_index()
        monthly_agg['Date'] = monthly_agg['Date'].astype(str)

        if len(monthly_agg) > 1:
            monthly_agg['GMV_Growth'] = monthly_agg['GMV'].pct_change() * 100

            fig = make_subplots(specs=[[{"secondary_y": True}]])

            fig.add_trace(go.Bar(
                x=monthly_agg['Date'],
                y=monthly_agg['GMV'],
                name='GMV',
                marker_color='#ee4d2d'
            ), secondary_y=False)

            fig.add_trace(go.Scatter(
                x=monthly_agg['Date'],
                y=monthly_agg['GMV_Growth'],
                name='Growth %',
                mode='lines+markers',
                line=dict(color='#28a745', width=2),
                marker=dict(size=8)
            ), secondary_y=True)

            fig.update_layout(
                title="Monthly GMV & Growth Rate",
                height=350,
                hovermode='x unified'
            )
            fig.update_yaxes(title_text="GMV (฿)", secondary_y=False)
            fig.update_yaxes(title_text="Growth %", secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need more than 1 month of data for MoM comparison")

        # ==========================================
        # AOV ANALYSIS
        # ==========================================
        st.markdown("---")
        st.markdown("### 💰 Average Order Value (AOV) Analysis")

        # Calculate AOV
        daily_agg['AOV'] = daily_agg.apply(
            lambda x: x['GMV'] / x['Orders'] if x['Orders'] > 0 else 0, axis=1
        )

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_aov = daily_agg['AOV'].mean()
            st.metric("Average AOV", format_currency(avg_aov))
        with col2:
            median_aov = daily_agg['AOV'].median()
            st.metric("Median AOV", format_currency(median_aov))
        with col3:
            max_aov = daily_agg['AOV'].max()
            st.metric("Max AOV", format_currency(max_aov))
        with col4:
            min_aov = daily_agg[daily_agg['AOV'] > 0]['AOV'].min()
            st.metric("Min AOV", format_currency(min_aov))

        # AOV trend over time
        col1, col2 = st.columns(2)

        with col1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=daily_agg['Date'],
                y=daily_agg['AOV'],
                mode='lines+markers',
                name='Daily AOV',
                line=dict(color='#9c27b0', width=1),
                marker=dict(size=4)
            ))

            # Rolling average
            daily_agg['AOV_Rolling'] = daily_agg['AOV'].rolling(window=7, min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=daily_agg['Date'],
                y=daily_agg['AOV_Rolling'],
                mode='lines',
                name='7-Day Avg',
                line=dict(color='#ee4d2d', width=2)
            ))

            fig.add_hline(y=avg_aov, line_dash="dash", line_color="#28a745",
                         annotation_text=f"Avg: {format_currency(avg_aov)}")

            fig.update_layout(
                title="AOV Trend Over Time",
                height=350,
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # AOV distribution
            fig = go.Figure(go.Histogram(
                x=daily_agg[daily_agg['AOV'] > 0]['AOV'],
                nbinsx=30,
                marker_color='#9c27b0',
                opacity=0.7
            ))
            fig.add_vline(x=avg_aov, line_dash="dash", line_color="#28a745",
                         annotation_text=f"Mean: {format_currency(avg_aov)}")
            fig.add_vline(x=median_aov, line_dash="dot", line_color="#0066cc",
                         annotation_text=f"Median: {format_currency(median_aov)}")
            fig.update_layout(
                title="AOV Distribution",
                xaxis_title="AOV (฿)",
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)

        # AOV by day of week
        st.markdown("#### AOV by Day of Week")
        dow_aov = daily_agg.groupby('DayOfWeek')['AOV'].mean()
        dow_aov.index = dow_names

        fig = go.Figure(go.Bar(
            x=dow_aov.index,
            y=dow_aov.values,
            marker_color=['#28a745' if v == dow_aov.max() else '#9c27b0' for v in dow_aov.values],
            text=[format_currency(v) for v in dow_aov.values],
            textposition='outside'
        ))
        fig.add_hline(y=avg_aov, line_dash="dash", line_color="#666666")
        fig.update_layout(
            title="Average AOV by Day of Week",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

        # ==========================================
        # TIME-BASED INSIGHTS
        # ==========================================
        st.markdown("---")
        st.markdown("### ⏰ Time-Based Insights")

        col1, col2 = st.columns(2)

        with col1:
            # Best performing days
            st.markdown("#### 🏆 Top 10 Best Days by GMV")
            top_days = daily_agg.nlargest(10, 'GMV')[['Date', 'GMV', 'Orders', 'AOV']].copy()
            top_days['Date'] = pd.to_datetime(top_days['Date']).dt.strftime('%Y-%m-%d (%a)')
            top_days['GMV'] = top_days['GMV'].apply(format_currency)
            top_days['AOV'] = top_days['AOV'].apply(format_currency)
            top_days.columns = ['Date', 'GMV', 'Orders', 'AOV']
            st.dataframe(top_days, use_container_width=True, hide_index=True)

        with col2:
            # Best performing days by AOV
            st.markdown("#### 💎 Top 10 Best Days by AOV")
            top_aov = daily_agg[daily_agg['Orders'] >= 5].nlargest(10, 'AOV')[['Date', 'AOV', 'GMV', 'Orders']].copy()
            top_aov['Date'] = pd.to_datetime(top_aov['Date']).dt.strftime('%Y-%m-%d (%a)')
            top_aov['GMV'] = top_aov['GMV'].apply(format_currency)
            top_aov['AOV'] = top_aov['AOV'].apply(format_currency)
            top_aov.columns = ['Date', 'AOV', 'GMV', 'Orders']
            st.dataframe(top_aov, use_container_width=True, hide_index=True)

        # Monthly AOV trend
        st.markdown("#### 📊 Monthly AOV Trend")
        monthly_aov = daily_agg.groupby(daily_agg['Date'].dt.to_period('M')).agg({
            'GMV': 'sum',
            'Orders': 'sum',
            'AOV': 'mean'
        }).reset_index()
        monthly_aov['Date'] = monthly_aov['Date'].astype(str)
        monthly_aov['Calculated_AOV'] = monthly_aov['GMV'] / monthly_aov['Orders']

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(go.Bar(
            x=monthly_aov['Date'],
            y=monthly_aov['GMV'],
            name='GMV',
            marker_color='#ee4d2d',
            opacity=0.7
        ), secondary_y=False)

        fig.add_trace(go.Scatter(
            x=monthly_aov['Date'],
            y=monthly_aov['Calculated_AOV'],
            name='AOV',
            mode='lines+markers',
            line=dict(color='#9c27b0', width=3),
            marker=dict(size=10)
        ), secondary_y=True)

        fig.update_layout(
            title="Monthly GMV & AOV Trend",
            height=350,
            hovermode='x unified'
        )
        fig.update_yaxes(title_text="GMV (฿)", secondary_y=False)
        fig.update_yaxes(title_text="AOV (฿)", secondary_y=True)

        st.plotly_chart(fig, use_container_width=True)

        # Key insights summary
        st.markdown("---")
        st.markdown("### 📋 Key Insights Summary")

        best_dow = dow_aov.idxmax()
        best_dow_gmv = dow_avg.idxmax()
        aov_trend = "📈 Increasing" if daily_agg['AOV_Rolling'].iloc[-1] > daily_agg['AOV_Rolling'].iloc[0] else "📉 Decreasing"

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Best Day for GMV", best_dow_gmv)
            st.caption(f"Avg: {format_currency(dow_avg[best_dow_gmv])}")
        with col2:
            st.metric("Best Day for AOV", best_dow)
            st.caption(f"Avg: {format_currency(dow_aov[best_dow])}")
        with col3:
            st.metric("AOV Trend", aov_trend)
            st.caption(f"Current: {format_currency(daily_agg['AOV_Rolling'].iloc[-1])}")

    else:
        st.info(f"Need at least 30 days of data for seasonality analysis. Current: {len(daily_gmv) if not daily_gmv.empty else 0} days")
