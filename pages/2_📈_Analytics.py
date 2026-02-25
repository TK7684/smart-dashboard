"""
Analytics & Predictive Models Page
Regression analysis, forecasting, and performance prediction
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
    load_tiktok_live, load_tiktok_video, load_daily_sales, COMMON_STYLES
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
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Last 30 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=30), max_date)
        if st.button("Q1", use_container_width=True):
            q1_end = min(datetime(max_date.year, 3, 31).date(), max_date)
            st.session_state.date_range = (datetime(max_date.year, 1, 1).date(), q1_end)

    with col2:
        if st.button("Last 90 Days", use_container_width=True):
            st.session_state.date_range = (max_date - timedelta(days=90), max_date)
        if st.button("This Year", use_container_width=True):
            st.session_state.date_range = (datetime(max_date.year, 1, 1).date(), max_date)

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
        "Analysis Focus",
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
st.markdown(f"**Period:** {start_date} to {end_date}")

if not SKLEARN_AVAILABLE:
    st.warning("⚠️ scikit-learn not installed. Some features limited. Run: `pip install scikit-learn`")

# ==========================================
# LOAD DATA
# ==========================================
# Load TikTok data
tiktok_live_raw = load_tiktok_live()
tiktok_video_raw = load_tiktok_video()

# Load Shopee data
shopee_live_raw = load_live_data()
shopee_video_raw = load_video_data()
daily_sales_raw = load_daily_sales()

# Filter by date
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

# ==========================================
# TABS
# ==========================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Correlation Analysis",
    "🔮 Sales Forecasting",
    "🎯 Performance Prediction",
    "📋 Feature Importance"
])

# ==========================================
# TAB 1: CORRELATION ANALYSIS
# ==========================================
with tab1:
    st.markdown("## 📊 Correlation Analysis")
    st.markdown("Understand which metrics drive GMV and engagement")

    if not tiktok_live.empty:
        st.markdown("### TikTok Live - Correlation Matrix")

        # Prepare numeric columns
        numeric_cols = ['GMV', 'Orders', 'Viewers', 'Views', 'Likes', 'Comments', 'Shares',
                       'New_Followers', 'Products_Sold', 'Unique_Customers', 'Engagement_Rate']

        available_cols = [c for c in numeric_cols if c in tiktok_live.columns]
        corr_df = tiktok_live[available_cols].apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()

        if len(corr_df) > 10:
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
                title="TikTok Live Metrics Correlation",
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

            # Scatter plot for top correlation
            st.markdown("### 📈 GMV vs Orders Relationship")
            fig_scatter = go.Figure()
            fig_scatter.add_trace(go.Scatter(
                x=corr_df['Orders'],
                y=corr_df['GMV'],
                mode='markers',
                marker=dict(size=8, color=corr_df['Engagement_Rate'], colorscale='Viridis', showscale=True),
                text=tiktok_live.loc[corr_df.index, 'Nickname'] if 'Nickname' in tiktok_live.columns else None,
                name='Sessions'
            ))
            fig_scatter.update_layout(
                title="GMV vs Orders (Color = Engagement Rate)",
                xaxis_title="Orders",
                yaxis_title="GMV (฿)",
                height=400
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("Not enough data for correlation analysis (need >10 sessions)")
    else:
        st.info("No TikTok Live data available for the selected period")

# ==========================================
# TAB 2: SALES FORECASTING
# ==========================================
with tab2:
    st.markdown("## 🔮 Sales Forecasting")
    st.markdown("Predict future GMV using time series analysis")

    if not tiktok_live.empty and 'Start_DateTime' in tiktok_live.columns:
        # Aggregate daily GMV
        tiktok_live['Date'] = tiktok_live['Start_DateTime'].dt.date
        daily_gmv = tiktok_live.groupby('Date').agg({
            'GMV': lambda x: x.apply(safe_float).sum(),
            'Orders': lambda x: x.apply(safe_float).sum(),
            'Viewers': lambda x: x.apply(safe_float).sum()
        }).reset_index()
        daily_gmv['Date'] = pd.to_datetime(daily_gmv['Date'])
        daily_gmv = daily_gmv.sort_values('Date')

        if len(daily_gmv) >= 7:
            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown("### 📊 GMV Trend & Forecast")

                # Simple exponential smoothing forecast
                alpha = 0.3  # Smoothing factor
                daily_gmv['GMV_Smooth'] = daily_gmv['GMV'].ewm(alpha=alpha).mean()

                # Calculate trend
                if len(daily_gmv) >= 14:
                    # Linear regression for trend
                    X = np.arange(len(daily_gmv)).reshape(-1, 1)
                    y = daily_gmv['GMV'].values

                    if SKLEARN_AVAILABLE:
                        model = LinearRegression()
                        model.fit(X, y)
                        trend = model.predict(X)
                        daily_gmv['Trend'] = trend

                        # Forecast next 7 days
                        future_X = np.arange(len(daily_gmv), len(daily_gmv) + 7).reshape(-1, 1)
                        forecast = model.predict(future_X)

                        # Create future dates
                        last_date = daily_gmv['Date'].max()
                        future_dates = [last_date + timedelta(days=i+1) for i in range(7)]

                # Plot
                fig = go.Figure()

                # Actual GMV
                fig.add_trace(go.Scatter(
                    x=daily_gmv['Date'],
                    y=daily_gmv['GMV'],
                    name='Actual GMV',
                    mode='lines+markers',
                    line=dict(color='#ee4d2d', width=2),
                    marker=dict(size=6)
                ))

                # Smoothed
                fig.add_trace(go.Scatter(
                    x=daily_gmv['Date'],
                    y=daily_gmv['GMV_Smooth'],
                    name='Smoothed (EWMA)',
                    line=dict(color='#0066cc', width=2, dash='dot')
                ))

                # Trend line
                if SKLEARN_AVAILABLE and len(daily_gmv) >= 14:
                    fig.add_trace(go.Scatter(
                        x=daily_gmv['Date'],
                        y=daily_gmv['Trend'],
                        name='Trend Line',
                        line=dict(color='#28a745', width=2, dash='dash')
                    ))

                    # Forecast
                    fig.add_trace(go.Scatter(
                        x=future_dates,
                        y=forecast,
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

                if SKLEARN_AVAILABLE and len(daily_gmv) >= 14:
                    # Stats
                    avg_gmv = daily_gmv['GMV'].mean()
                    last_gmv = daily_gmv['GMV'].iloc[-1]
                    trend_dir = "📈 Upward" if model.coef_[0] > 0 else "📉 Downward"

                    st.metric("Avg Daily GMV", format_currency(avg_gmv))
                    st.metric("Last Day GMV", format_currency(last_gmv))
                    st.metric("Trend Direction", trend_dir)

                    st.markdown("---")
                    st.markdown("**7-Day Forecast:**")
                    for i, (date, gmv) in enumerate(zip(future_dates[:7], forecast[:7])):
                        st.write(f"• {date.strftime('%Y-%m-%d')}: {format_currency(gmv)}")

                    # Total forecast
                    total_forecast = sum(forecast[:7])
                    st.markdown(f"**Total 7-Day Forecast:** {format_currency(total_forecast)}")
                else:
                    st.info("Need ≥14 days of data for forecasting")

            # Model accuracy
            if SKLEARN_AVAILABLE and len(daily_gmv) >= 14:
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
            st.info("Need at least 7 days of data for forecasting")
    else:
        st.info("No TikTok Live data available for forecasting")

# ==========================================
# TAB 3: PERFORMANCE PREDICTION
# ==========================================
with tab3:
    st.markdown("## 🎯 Performance Prediction Model")
    st.markdown("Predict GMV based on engagement metrics using Random Forest")

    if not tiktok_live.empty and SKLEARN_AVAILABLE:
        # Prepare features
        feature_cols = ['Viewers', 'Views', 'Likes', 'Comments', 'Shares', 'New_Followers', 'Products_Sold']
        available_features = [c for c in feature_cols if c in tiktok_live.columns]

        if len(available_features) >= 3 and len(tiktok_live) >= 30:
            # Create feature matrix
            X = tiktok_live[available_features].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
            y = tiktok_live['GMV'].apply(safe_float)

            # Remove zero GMV for better training
            mask = y > 0
            X = X[mask]
            y = y[mask]

            if len(X) >= 20:
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

                    # Interpretation
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
                        marker=dict(size=10, color='#ee4d2d'),
                        name='Test Data'
                    ))

                    # Perfect prediction line
                    max_val = max(y_test.max(), max(y_pred_test))
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
                    # Scale user input
                    user_features = np.array([[user_inputs[c] for c in available_features]])
                    user_scaled = scaler.transform(user_features)
                    predicted_gmv = rf_model.predict(user_scaled)[0]

                    st.metric("Predicted GMV", format_currency(predicted_gmv))

                    # Confidence interpretation
                    st.caption("Note: Prediction based on historical patterns. Actual results may vary.")
            else:
                st.info("Need at least 20 sessions with GMV > 0 for prediction model")
        else:
            st.info("Need at least 30 sessions with 3+ metrics for prediction model")
    else:
        if not SKLEARN_AVAILABLE:
            st.error("scikit-learn required. Install with: `pip install scikit-learn`")
        else:
            st.info("No TikTok Live data available for prediction")

# ==========================================
# TAB 4: FEATURE IMPORTANCE
# ==========================================
with tab4:
    st.markdown("## 📋 Feature Importance Analysis")
    st.markdown("Which metrics have the biggest impact on GMV?")

    if not tiktok_live.empty and SKLEARN_AVAILABLE:
        feature_cols = ['Viewers', 'Views', 'Likes', 'Comments', 'Shares', 'New_Followers',
                       'Products_Sold', 'Products_Added', 'Unique_Customers', 'Engagement_Rate']
        available_features = [c for c in feature_cols if c in tiktok_live.columns]

        if len(available_features) >= 3:
            X = tiktok_live[available_features].apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
            y = tiktok_live['GMV'].apply(safe_float)

            mask = y > 0
            X = X[mask]
            y = y[mask]

            if len(X) >= 20:
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
                        marker_color='#ee4d2d'
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
                    if 'Engagement_Rate' in top_3:
                        st.write("• Improve audience interaction")
                    if 'New_Followers' in top_3:
                        st.write("• Build follower base for long-term growth")

                # Statistical summary
                st.markdown("---")
                st.markdown("### 📈 Statistical Summary")
                st.dataframe(
                    tiktok_live[available_features + ['GMV']].describe().round(2),
                    use_container_width=True
                )
            else:
                st.info("Need ≥20 sessions for feature importance analysis")
        else:
            st.info("Need at least 3 metrics for feature analysis")
    else:
        if not SKLEARN_AVAILABLE:
            st.error("scikit-learn required")
        else:
            st.info("No data available")
