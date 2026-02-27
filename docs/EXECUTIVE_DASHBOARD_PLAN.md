# Executive Dashboard Implementation Plan

## 1. Executive Summary

### 1.1 Business Objectives

| Objective | Metric | Target |
|-----------|--------|--------|
| Reduce wasted ad spend | MER improvement | 15-20% reduction in Q1 |
| Track true Blended ROAS | Cross-platform attribution | Real-time visibility |
| Customer retention | LTV:CAC ratio | Minimum 3:1 |
| LINE broadcast effectiveness | 48-hour conversion rate | Track ROI per broadcast |

### 1.2 Key Challenges

1. **Attribution Complexity**: Cross-platform tracking where Facebook/TikTok both claim credit
2. **Data Silos**: Separate systems for discovery (ads) and conversion (sales)
3. **API Limitations**: Shopee API latency, LINE user ID mapping
4. **Thai Consumer Behavior**: Consultative buying via chat before purchase

### 1.3 Expected Outcomes

- Unified view of customer journey across platforms
- Real-time budget shifting capability between underperforming and high-converting channels
- Customer segmentation for targeted marketing
- Predictive insights for repurchase behavior

---

## 2. Technical Architecture

### 2.1 System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────┤
│   Shopee     │   TikTok     │   Facebook   │    LINE      │   Manual    │
│   Orders     │   Shop       │   Ads        │    OA        │   Imports   │
│   Ads        │   Live       │   Pixel      │   Messages   │   (Bank)    │
└──────┬───────┴──────┬───────┴──────┬───────┴──────┬───────┴──────┬──────┘
       │              │              │              │              │
       ▼              ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      ETL PIPELINE (Python)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │ File Watcher│  │   Clean &   │  │   Dedupe &  │  │  Incremental    │ │
│  │ (inotify)   │  │  Transform  │  │   Validate  │  │  Update System  │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (DuckDB)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │ daily_sales │  │  products   │  │   ads       │  │  tiktok_orders  │ │
│  │ geographic  │  │  rfm_scores │  │ attribution │  │  line_messages  │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS LAYER (Python)                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │ RFM Segments│  │  Blended    │  │  Market     │  │   Regression    │ │
│  │             │  │   ROAS      │  │   Basket    │  │   Models        │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER (Streamlit)                        │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  Module 1: Pulse (Global Overview)                                   ││
│  │  Module 2: Thai Funnel Breakdown (Platform Specifics)               ││
│  │  Module 3: SKU & Customer Intelligence                               ││
│  │  Module 4: Attribution & Forecasting                                 ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| ETL | Python + Pandas | Data extraction, transformation |
| Database | DuckDB | Fast analytical queries, single-file deployment |
| Analytics | scikit-learn, statsmodels | ML models, regression, clustering |
| Visualization | Streamlit + Plotly | Interactive dashboard |
| Scheduling | systemd + inotify | Automated pipeline triggers |

### 2.3 Data Flow

```
Source Files → Incremental Loader → Cleaner → Deduplicator → DuckDB
                                                              ↓
                         Analytics Engine ←───────────────────┘
                              ↓
                         Streamlit Dashboard
```

---

## 3. Data Models

### 3.1 Core Tables (Existing)

| Table | Granularity | Key Metrics |
|-------|-------------|-------------|
| `daily_sales` | Date + Platform | GMV, Orders, AOV, Net Revenue |
| `products` | SKU + Platform | Total GMV, Quantity, Segment |
| `ads_performance` | Campaign | ROAS, ACOS, Spend, Impressions |
| `tiktok_orders` | Order ID | Net Sales, Quantity, Province |
| `combined_live` | Session | GMV, Viewers, Duration |
| `combined_video` | Video | Views, Likes, Engagement |

### 3.2 New Tables (To Implement)

#### 3.2.1 Customer RFM Scores
```sql
CREATE TABLE customer_rfm (
    Customer_ID VARCHAR,
    Platform VARCHAR,
    Recency_Days INTEGER,      -- Days since last order
    Frequency INTEGER,         -- Total orders
    Monetary DECIMAL(12,2),    -- Total spend
    R_Score INTEGER,           -- 1-5 (5 = most recent)
    F_Score INTEGER,           -- 1-5 (5 = most frequent)
    M_Score INTEGER,           -- 1-5 (5 = highest spend)
    RFM_Segment VARCHAR,       -- e.g., "555" = Champion
    Segment_Name VARCHAR,      -- Champion, Loyal, At Risk, etc.
    Last_Updated TIMESTAMP
);
```

#### 3.2.2 Attribution Table
```sql
CREATE TABLE attribution (
    Date DATE,
    Order_ID VARCHAR,
    Platform VARCHAR,
    Source_Channel VARCHAR,    -- organic, ads_facebook, ads_tiktok, line_broadcast
    Campaign_ID VARCHAR,
    Touchpoint_Position VARCHAR, -- first, last, assist
    Attribution_Credit DECIMAL(4,3), -- 0.0 to 1.0
    GMV DECIMAL(12,2),
    Ad_Spend DECIMAL(10,2)
);
```

#### 3.2.3 LINE Message Tracking
```sql
CREATE TABLE line_broadcasts (
    Broadcast_ID VARCHAR,
    Send_Time TIMESTAMP,
    Message_Type VARCHAR,      -- promotional, informational
    Target_Segment VARCHAR,
    Recipients INTEGER,
    Open_Rate DECIMAL(5,2),
    Click_Rate DECIMAL(5,2),
    Orders_48h INTEGER,
    GMV_48h DECIMAL(12,2),
    Cost DECIMAL(10,2)
);
```

#### 3.2.4 Market Basket Analysis
```sql
CREATE TABLE product_associations (
    Product_A VARCHAR,
    Product_B VARCHAR,
    Co_occurrence INTEGER,     -- Bought together count
    Support DECIMAL(6,4),      -- P(A and B)
    Confidence DECIMAL(6,4),   -- P(B|A)
    Lift DECIMAL(6,2)          -- Confidence / P(B)
);
```

---

## 4. Dashboard Modules

### 4.1 Module 1: The "Pulse" (Global Overview)

**Target Users:** Executive team

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  PERIOD SELECTOR: [Today] [7D] [30D] [MTD] [QTD] [YTD] [Custom] │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  💰 Total GMV   │  📦 Orders      │  📊 Blended     │  🎯 MER   │
│  ฿XX,XXX,XXX    │  XXX,XXX        │  ROAS: X.Xx     │  X.Xx     │
│  +12.3% vs prev │  +8.5% vs prev  │  vs X.Xx prev   │           │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│                    GMV TREND (Time Series)                       │
│  [Area chart: Organic baseline vs Campaign spikes]               │
├─────────────────────────────────────────────────────────────────┤
│  PLATFORM BREAKDOWN                                              │
│  ┌─────────────┬─────────────┬─────────────┬─────────────────┐  │
│  │ 🛒 Shopee   │ 🎵 TikTok   │ 💬 LINE     │ 📱 Facebook     │  │
│  │ ฿X,XXX,XXX  │ ฿X,XXX,XXX  │ ฿X,XXX      │ X,XXX clicks    │  │
│  │ XX.X%       │ XX.X%       │ XX.X%       │ CPC: ฿X.XX      │  │
│  └─────────────┴─────────────┴─────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Metrics:**
1. **Total GMV** - Sum across all platforms
2. **Blended ROAS** = Total Revenue / Total Ad Spend (FB + TikTok + Shopee Ads)
3. **MER (Marketing Efficiency Ratio)** = Total GMV / Total Marketing Spend
4. **AOV** - Weighted average across platforms

### 4.2 Module 2: Thai Funnel Breakdown

#### 4.2.1 TikTok Shop & Ads (Shoppertainment)

```
┌─────────────────────────────────────────────────────────────────┐
│  🎵 TIKTOK SHOP PERFORMANCE                                      │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  📺 Live GMV    │  🎬 Video GMV   │  🛒 Shop GMV    │  📊 ROAS  │
│  ฿X,XXX,XXX     │  ฿XXX,XXX       │  ฿X,XXX,XXX     │  XX.Xx    │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│  LIVE vs VIDEO CONVERSION                                        │
│  [Bar chart: Views → Add to Cart → Purchase by channel]          │
├─────────────────────────────────────────────────────────────────┤
│  TOP PERFORMING CREATIVES                                        │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Hook: "สิวหายภายใน 7 วัน" │ CPAC: ฿XX │ Conv: X.X% │ Orders ││
│  │ Hook: "ผิวขาวใส..."      │ CPAC: ฿XX │ Conv: X.X% │ Orders ││
│  └─────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────┤
│  AFFILIATE vs ORGANIC                                            │
│  [Pie chart: Revenue source breakdown]                           │
└─────────────────────────────────────────────────────────────────┘
```

**Key Metrics:**
- Video Views vs GMV correlation
- Live Stream Revenue (real-time if possible)
- Cost Per Add-to-Cart by creative hook
- Affiliate commission rate

#### 4.2.2 Facebook & LINE OA (Chat & Shop Loop)

```
┌─────────────────────────────────────────────────────────────────┐
│  📱 FACEBOOK ADS → 💬 LINE OA FUNNEL                            │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  👆 FB Clicks   │  💬 Chat Starts │  🛒 Purchases   │  💰 GMV   │
│  XX,XXX         │  X,XXX          │  XXX            │ ฿XXX,XXX  │
│  CPC: ฿X.XX     │  CPA: ฿XX       │  Conv: X.X%     │           │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│  CONVERSION FUNNEL                                               │
│  [Funnel chart: Ad Click → Message → Quote → Payment → Complete]│
├─────────────────────────────────────────────────────────────────┤
│  LINE BROADCAST ROI                                              │
│  ┌─────────────┬─────────────┬─────────────┬─────────────────┐  │
│  │ Broadcast   │ Recipients  │ Open Rate   │ 48h GMV         │  │
│  │ 2026-02-20  │ 5,000       │ 45%         │ ฿XX,XXX         │  │
│  │ 2026-02-15  │ 3,500       │ 52%         │ ฿XX,XXX         │  │
│  └─────────────┴─────────────┴─────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Metrics:**
- Cost Per Message (CPA on FB)
- Chat-to-Purchase Conversion Rate
- LINE Broadcast 48-hour ROI
- PromptPay/Bank Transfer slip uploads

#### 4.2.3 Shopee

```
┌─────────────────────────────────────────────────────────────────┐
│  🛒 SHOPEE PERFORMANCE                                           │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  📈 GMV         │  📦 Orders      │  💵 AOV        │  📊 ROAS  │
│  ฿X,XXX,XXX     │  XX,XXX         │  ฿XXX          │  XX.Xx    │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│  TRAFFIC SOURCES                                                 │
│  [Stacked bar: CPAS Ads vs In-Platform Search vs External]       │
├─────────────────────────────────────────────────────────────────┤
│  TOP 10 SKUs BY CONVERSION                                       │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ SKU Name          │ Views │ Carts │ Purchases │ Conv Rate   ││
│  │ Vitamin C Serum   │ 5,000 │ 800   │ 320       │ 6.4%        ││
│  │ Niacinamide 10%   │ 4,200 │ 700   │ 280       │ 6.7%        ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 Module 3: SKU & Customer Intelligence

#### 4.3.1 Customer RFM Segmentation

```
┌─────────────────────────────────────────────────────────────────┐
│  👥 CUSTOMER SEGMENTS (RFM Analysis)                             │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  Champions      │ Loyal Customers │ At Risk        │ Lost      │
│  XXX customers  │ XXX customers   │ XXX customers  │ XXX cust  │
│  XX% of base    │ XX% of base     │ XX% of base    │ XX%       │
│  ฿XXX avg spend │ ฿XXX avg spend  │ ฿XXX avg spend │ ฿XXX      │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│  SEGMENT DISTRIBUTION                                            │
│  [Treemap: Customer count by segment, sized by total GMV]        │
├─────────────────────────────────────────────────────────────────┤
│  SEGMENT MIGRATION (vs last month)                               │
│  [Sankey diagram: Segment flows]                                 │
├─────────────────────────────────────────────────────────────────┤
│  FIRST-TIME vs REPEAT BUYERS                                     │
│  [Stacked bar by month: New vs Returning customer GMV]           │
└─────────────────────────────────────────────────────────────────┘
```

**RFM Segment Definitions:**
| Segment | R | F | M | Strategy |
|---------|---|---|---|----------|
| Champions | 5 | 5 | 5 | Reward, early access |
| Loyal | 4-5 | 4-5 | 3-5 | Upsell, cross-sell |
| Potential Loyalist | 4-5 | 1-3 | 3-5 | Membership program |
| New Customers | 5 | 1 | 1-5 | Onboarding series |
| At Risk | 2-3 | 4-5 | 4-5 | Win-back campaigns |
| Lost | 1 | 1-2 | 1-2 | Reactivation attempt |

#### 4.3.2 Product Intelligence

```
┌─────────────────────────────────────────────────────────────────┐
│  🏷️ PRODUCT PERFORMANCE MATRIX                                   │
├─────────────────────────────────────────────────────────────────┤
│  [Scatter plot: Volume (X) vs GMV (Y), sized by margin]          │
│  Quadrants: Star | Hero | Volume | Core                          │
├─────────────────────────────────────────────────────────────────┤
│  CREATIVE HOOK → INGREDIENT MAPPING                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Hook Keywords        │ Top Product    │ GMV      │ Conv     ││
│  │ "สิวหาย"              │ Acne Serum     │ ฿XXX,XXX │ X.X%     ││
│  │ "ผิวขาว"              │ Vitamin C      │ ฿XXX,XXX │ X.X%     ││
│  │ "ฝ้ากระ"             │ Niacinamide    │ ฿XXX,XXX │ X.X%     ││
│  └─────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────┤
│  MARKET BASKET ANALYSIS                                          │
│  "Customers who bought X also bought..."                         │
│  [Network graph: Product associations]                           │
│  Top pairs: Serum + Moisturizer (Lift: 2.3)                      │
└─────────────────────────────────────────────────────────────────┘
```

### 4.4 Module 4: Attribution & Forecasting

#### 4.4.1 Cross-Platform Attribution

```
┌─────────────────────────────────────────────────────────────────┐
│  🎯 ATTRIBUTION ANALYSIS                                         │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  First Touch    │ Last Touch      │ Linear          │ Time Decay│
│  [Radio button selection]                                         │
├─────────────────┴─────────────────┴─────────────────┴───────────┤
│  CHANNEL CONTRIBUTION TO GMV                                     │
│  [Stacked bar by month: Organic vs FB vs TikTok vs LINE]         │
├─────────────────────────────────────────────────────────────────┤
│  ASSISTED CONVERSIONS                                            │
│  [Sankey diagram: Path to purchase]                              │
│  FB Ad → LINE Chat → Shopee Purchase                             │
│  TikTok Video → TikTok Shop → Purchase                           │
├─────────────────────────────────────────────────────────────────┤
│  BLENDDED ROAS BY CHANNEL                                        │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Channel        │ Spend       │ Attributed GMV │ ROAS        ││
│  │ Facebook Ads   │ ฿XXX,XXX    │ ฿X,XXX,XXX     │ X.Xx        ││
│  │ TikTok Ads     │ ฿XXX,XXX    │ ฿X,XXX,XXX     │ X.Xx        ││
│  │ Shopee Ads     │ ฿XX,XXX     │ ฿XXX,XXX       │ XX.Xx       ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

#### 4.4.2 Forecasting & Predictive

```
┌─────────────────────────────────────────────────────────────────┐
│  📈 FORECASTING                                                  │
├─────────────────────────────────────────────────────────────────┤
│  GMV FORECAST (Next 30 Days)                                     │
│  [Line chart: Historical + Forecast with confidence interval]    │
│  Expected GMV: ฿X,XXX,XXX ± ฿XXX,XXX                             │
├─────────────────────────────────────────────────────────────────┤
│  SEASONALITY ANALYSIS                                            │
│  [Decomposition: Trend + Seasonal + Residual]                    │
│  Payday spikes (25th, 1st) | Double Days (3.3, 4.4, etc.)        │
├─────────────────────────────────────────────────────────────────┤
│  REPURCHASE PREDICTION                                           │
│  Customers likely to reorder in next 30 days: XXX                │
│  Top products for refill: [Product list]                         │
│  [Logistic regression model output]                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. Analytics Models

### 5.1 RFM Segmentation

**Algorithm:** K-means clustering on R, F, M scores

```python
# Pseudocode
def calculate_rfm(df):
    """Calculate RFM scores for each customer"""
    reference_date = df['Order_Date'].max()

    rfm = df.groupby('Customer_ID').agg({
        'Order_Date': lambda x: (reference_date - x.max()).days,  # Recency
        'Order_ID': 'nunique',  # Frequency
        'Net_Sales': 'sum'      # Monetary
    })

    # Score 1-5 using quantiles
    rfm['R_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5,4,3,2,1])
    rfm['F_Score'] = pd.qcut(rfm['Frequency'], 5, labels=[1,2,3,4,5])
    rfm['M_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1,2,3,4,5])

    # Assign segment
    rfm['RFM_Segment'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)
    rfm['Segment_Name'] = rfm.apply(assign_segment_name, axis=1)

    return rfm
```

### 5.2 Market Basket Analysis

**Algorithm:** Apriori algorithm for association rules

```python
from mlxtend.frequent_patterns import apriori, association_rules

def market_basket_analysis(transactions_df):
    """Find product associations"""
    # Create basket matrix
    basket = transactions_df.groupby(['Order_ID', 'Product_Name'])['Quantity'].sum().unstack().fillna(0)
    basket_encoded = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Find frequent itemsets
    frequent_items = apriori(basket_encoded, min_support=0.01, use_colnames=True)

    # Generate rules
    rules = association_rules(frequent_items, metric="lift", min_threshold=1.0)

    return rules.sort_values('lift', ascending=False)
```

### 5.3 Repurchase Prediction (Logistic Regression)

**Features:**
- Days since last purchase
- Total orders
- Average order value
- Product category
- Seasonality (month)

```python
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

def train_repurchase_model(df):
    """Predict if customer will repurchase in next 30 days"""
    # Create target: repurchased within 30 days?
    df['repurchase_30d'] = (df['days_to_next_order'] <= 30).astype(int)

    features = ['recency', 'frequency', 'monetary', 'avg_order_value', 'product_category']
    X = df[features]
    y = df['repurchase_30d']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    model = LogisticRegression()
    model.fit(X_train, y_train)

    return model
```

### 5.4 Time Series Forecasting

**Method:** Prophet or SARIMA for GMV forecasting

```python
from prophet import Prophet

def forecast_gmv(daily_sales_df, periods=30):
    """Forecast future GMV"""
    df = daily_sales_df[['Date', 'GMV']].copy()
    df.columns = ['ds', 'y']

    # Add seasonality for Thai e-commerce events
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )

    # Add Thai-specific seasonality (Payday, Double Days)
    model.add_seasonality(name='payday', period=30.5, fourier_order=3)

    model.fit(df)
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    return forecast
```

---

## 6. Implementation Phases

### Phase 1: Foundation (Week 1-2)
- [x] Incremental data pipeline
- [x] DuckDB database schema
- [ ] Add customer_rfm table
- [ ] Add attribution table
- [ ] Create LINE broadcast tracking

### Phase 2: Analytics Engine (Week 3-4)
- [ ] Implement RFM segmentation
- [ ] Build market basket analysis
- [ ] Create blended ROAS calculations
- [ ] Implement attribution logic

### Phase 3: Dashboard Modules (Week 5-6)
- [ ] Module 1: Global Overview (Pulse)
- [ ] Module 2: Platform-specific funnels
- [ ] Module 3: Customer & SKU intelligence
- [ ] Module 4: Attribution & Forecasting

### Phase 4: Advanced Features (Week 7-8)
- [ ] Repurchase prediction model
- [ ] GMV forecasting
- [ ] Alert system for anomalies
- [ ] Export functionality

---

## 7. KPIs to Track

### 7.1 North Star Metrics

| Metric | Formula | Target | Frequency |
|--------|---------|--------|-----------|
| MER (Marketing Efficiency Ratio) | Total GMV / Total Ad Spend | > 5.0x | Daily |
| LTV:CAC Ratio | Customer Lifetime Value / Acquisition Cost | > 3:1 | Weekly |
| LINE Broadcast ROI | 48h Revenue / Broadcast Cost | > 10x | Per broadcast |

### 7.2 Operational Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| ROAS Drop | Daily ROAS vs 7-day avg | < -20% |
| Cart Abandonment Spike | vs baseline | > +30% |
| Customer Churn Rate | Monthly | > 5% |
| Inventory Days | Stock / Daily sales | < 7 days |

---

## 8. File Structure

```
Shopee-dashboard/
├── data_pipeline.py          # Main ETL pipeline
├── dashboard.py              # Main dashboard (existing)
├── analytics/
│   ├── __init__.py
│   ├── rfm_analysis.py       # RFM segmentation
│   ├── market_basket.py      # Association rules
│   ├── attribution.py        # Cross-platform attribution
│   └── forecasting.py        # Time series models
├── pages/
│   ├── 1_🎬_Content_Commerce.py
│   ├── 2_📈_Analytics.py
│   ├── 3_👥_Customers.py      # NEW: RFM segments
│   ├── 4_🎯_Attribution.py    # NEW: Attribution
│   └── 5_📊_Forecasting.py    # NEW: Predictions
├── processed_data/
│   ├── shopee_dashboard.duckdb
│   └── file_manifest.json
└── docs/
    └── EXECUTIVE_DASHBOARD_PLAN.md
```

---

## 9. Next Steps

1. **Immediate**: Review plan and prioritize Phase 1 items
2. **Week 1**: Implement customer_rfm table and calculation
3. **Week 2**: Build attribution logic for cross-platform tracking
4. **Week 3**: Create new Streamlit pages for analytics modules

---

*Document Version: 1.0*
*Last Updated: 2026-02-27*
