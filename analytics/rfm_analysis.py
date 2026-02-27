"""
RFM Analysis Module
===================
Customer segmentation using Recency, Frequency, Monetary analysis.

RFM scores customers on three dimensions:
- Recency: How recently did they purchase? (Higher score = more recent)
- Frequency: How often do they purchase? (Higher score = more frequent)
- Monetary: How much do they spend? (Higher score = more spend)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# RFM Segment definitions
SEGMENT_RULES = {
    # Segment Name: (R range, F range, M range) - min and max scores
    'Champions': [(4, 5), (4, 5), (4, 5)],
    'Loyal Customers': [(3, 5), (3, 5), (3, 5)],
    'Potential Loyalist': [(4, 5), (1, 3), (3, 5)],
    'New Customers': [(4, 5), (1, 2), (1, 5)],
    'Promising': [(3, 4), (1, 2), (1, 3)],
    'Need Attention': [(2, 3), (2, 3), (2, 4)],
    'About to Sleep': [(2, 3), (1, 2), (1, 2)],
    'At Risk': [(1, 2), (3, 5), (3, 5)],
    'Cant Lose Them': [(1, 1), (4, 5), (4, 5)],
    'Hibernating': [(1, 2), (1, 2), (1, 3)],
    'Lost': [(1, 1), (1, 1), (1, 2)],
}

# Segment strategies for marketing
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


def calculate_rfm(df, customer_id_col='Customer_ID', date_col='Order_Date',
                  revenue_col='Net_Sales', order_id_col='Order_ID',
                  reference_date=None):
    """
    Calculate RFM values for each customer.

    Parameters:
    -----------
    df : DataFrame
        Order data with customer ID, date, and revenue columns
    customer_id_col : str
        Name of customer identifier column
    date_col : str
        Name of order date column
    revenue_col : str
        Name of revenue/monetary column
    order_id_col : str
        Name of order ID column
    reference_date : datetime
        Reference date for recency calculation (default: max date in data)

    Returns:
    --------
    DataFrame with RFM values per customer
    """
    # Ensure date column is datetime
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # Set reference date
    if reference_date is None:
        reference_date = df[date_col].max() + timedelta(days=1)

    # Calculate RFM values
    rfm = df.groupby(customer_id_col).agg({
        date_col: lambda x: (reference_date - x.max()).days,  # Recency (days since last order)
        order_id_col: 'nunique',  # Frequency (number of orders)
        revenue_col: 'sum'  # Monetary (total spend)
    }).reset_index()

    rfm.columns = [customer_id_col, 'Recency', 'Frequency', 'Monetary']

    return rfm


def score_rfm(rfm_df, recency_col='Recency', frequency_col='Frequency',
              monetary_col='Monetary', n_bins=5):
    """
    Assign RFM scores (1-5) using quantile-based binning.

    Higher scores are better:
    - Recency: 5 = most recent, 1 = oldest
    - Frequency: 5 = most frequent, 1 = least frequent
    - Monetary: 5 = highest spend, 1 = lowest spend

    Parameters:
    -----------
    rfm_df : DataFrame
        DataFrame with Recency, Frequency, Monetary columns
    n_bins : int
        Number of bins for scoring (default 5)

    Returns:
    --------
    DataFrame with R_Score, F_Score, M_Score columns added
    """
    df = rfm_df.copy()

    # Recency: Lower is better, so reverse the labels
    # (most recent = 5, oldest = 1)
    df['R_Score'] = pd.qcut(df[recency_col], q=n_bins, labels=range(n_bins, 0, -1))

    # Frequency: Higher is better
    df['F_Score'] = pd.qcut(df[frequency_col].rank(method='first'), q=n_bins, labels=range(1, n_bins + 1))

    # Monetary: Higher is better
    df['M_Score'] = pd.qcut(df[monetary_col].rank(method='first'), q=n_bins, labels=range(1, n_bins + 1))

    # Convert to integers
    df['R_Score'] = df['R_Score'].astype(int)
    df['F_Score'] = df['F_Score'].astype(int)
    df['M_Score'] = df['M_Score'].astype(int)

    return df


def assign_segment(row):
    """
    Assign customer segment based on RFM scores.

    Parameters:
    -----------
    row : Series
        Row with R_Score, F_Score, M_Score

    Returns:
    --------
    str : Segment name
    """
    r, f, m = row['R_Score'], row['F_Score'], row['M_Score']

    # Check segments in priority order (most valuable first)
    for segment_name, (r_range, f_range, m_range) in SEGMENT_RULES.items():
        if (r_range[0] <= r <= r_range[1] and
            f_range[0] <= f <= f_range[1] and
            m_range[0] <= m <= m_range[1]):
            return segment_name

    # Default fallback based on RFM sum
    rfm_sum = r + f + m
    if rfm_sum >= 13:
        return 'Loyal Customers'
    elif rfm_sum >= 10:
        return 'Potential Loyalist'
    elif rfm_sum >= 7:
        return 'Need Attention'
    elif rfm_sum >= 4:
        return 'At Risk'
    else:
        return 'Lost'


def create_rfm_segmentation(df, customer_id_col='Customer_ID', date_col='Order_Date',
                            revenue_col='Net_Sales', order_id_col='Order_ID',
                            reference_date=None, include_platform=False):
    """
    Full RFM segmentation pipeline.

    Parameters:
    -----------
    df : DataFrame
        Order data
    customer_id_col : str
        Customer identifier column name
    date_col : str
        Order date column name
    revenue_col : str
        Revenue column name
    order_id_col : str
        Order ID column name
    reference_date : datetime
        Reference date for recency
    include_platform : bool
        Whether to include platform in grouping

    Returns:
    --------
    DataFrame with customer RFM scores and segments
    """
    # Calculate RFM values
    if include_platform and 'Platform' in df.columns:
        # Group by customer and platform
        group_cols = [customer_id_col, 'Platform']
    else:
        group_cols = [customer_id_col]

    # Calculate RFM
    rfm = calculate_rfm(
        df, customer_id_col, date_col, revenue_col, order_id_col, reference_date
    )

    # Score RFM
    rfm = score_rfm(rfm)

    # Assign segments
    rfm['RFM_Segment'] = rfm.apply(lambda x: f"{x['R_Score']}{x['F_Score']}{x['M_Score']}", axis=1)
    rfm['Segment'] = rfm.apply(assign_segment, axis=1)

    # Add segment strategy
    rfm['Strategy'] = rfm['Segment'].map(SEGMENT_STRATEGIES)

    # Add last updated timestamp
    rfm['Last_Updated'] = datetime.now()

    return rfm


def get_segment_summary(rfm_df):
    """
    Generate summary statistics by segment.

    Parameters:
    -----------
    rfm_df : DataFrame
        RFM data with Segment column

    Returns:
    --------
    DataFrame with segment summary
    """
    summary = rfm_df.groupby('Segment').agg({
        'Customer_ID': 'count' if 'Customer_ID' in rfm_df.columns else 'count',
        'Recency': 'mean',
        'Frequency': 'mean',
        'Monetary': ['mean', 'sum']
    }).round(2)

    summary.columns = ['Customers', 'Avg_Recency_Days', 'Avg_Frequency',
                       'Avg_Monetary', 'Total_Monetary']
    summary = summary.sort_values('Total_Monetary', ascending=False)

    # Add percentage of total
    total_customers = summary['Customers'].sum()
    total_monetary = summary['Total_Monetary'].sum()
    summary['Pct_Customers'] = (summary['Customers'] / total_customers * 100).round(1)
    summary['Pct_Revenue'] = (summary['Total_Monetary'] / total_monetary * 100).round(1)

    return summary.reset_index()


def identify_customers_for_campaign(rfm_df, segment, limit=None):
    """
    Get list of customers for a marketing campaign.

    Parameters:
    -----------
    rfm_df : DataFrame
        RFM data
    segment : str or list
        Segment name(s) to target
    limit : int
        Maximum number of customers to return

    Returns:
    --------
    DataFrame with customer IDs and their RFM details
    """
    if isinstance(segment, str):
        segment = [segment]

    target = rfm_df[rfm_df['Segment'].isin(segment)].copy()

    # Sort by monetary value (highest value customers first)
    target = target.sort_values('Monetary', ascending=False)

    if limit:
        target = target.head(limit)

    return target


# Segment color mapping for visualizations
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
