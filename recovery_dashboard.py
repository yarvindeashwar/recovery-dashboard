"""
Recovery MIS Dashboard - Complete Streamlit Application
Copy this entire file and save as 'recovery_dashboard.py'

To run:
1. Install requirements: pip install streamlit pandas plotly google-cloud-bigquery pandas-gbq
2. Set up BigQuery credentials (see instructions below)
3. Run: streamlit run recovery_dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas_gbq
import json
from google.oauth2 import service_account

# ============================================
# CONFIGURATION
# ============================================

# Set your Google Cloud project ID
PROJECT_ID = "arboreal-vision-339901"  # Using your BigQuery MCP project

# Page config
st.set_page_config(
    page_title="Recovery MIS Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .big-font {
        font-size: 24px !important;
        font-weight: bold;
    }
    .medium-font {
        font-size: 18px !important;
    }
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# BIGQUERY CONNECTION
# ============================================

@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client"""
    try:
        # Try to use Streamlit secrets first (for cloud deployment)
        if 'gcp_credentials' in st.secrets:
            import os
            credentials_dict = dict(st.secrets["gcp_credentials"])
            
            # Create credentials from the dictionary
            from google.oauth2.credentials import Credentials
            
            credentials = Credentials(
                token=None,
                refresh_token=credentials_dict.get("refresh_token"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=credentials_dict.get("client_id"),
                client_secret=credentials_dict.get("client_secret")
            )
            
            # Set environment variable to avoid metadata server lookup
            os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
            
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        else:
            # Fall back to default credentials (for local development)
            client = bigquery.Client(project=PROJECT_ID)
        
        return client
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {e}")
        st.info("Please ensure you have set up Google Cloud credentials")
        return None

# ============================================
# DATA QUERIES
# ============================================

def get_credentials():
    """Get credentials for pandas_gbq queries"""
    if 'gcp_credentials' in st.secrets:
        credentials_dict = dict(st.secrets["gcp_credentials"])
        from google.oauth2.credentials import Credentials
        return Credentials(
            token=None,
            refresh_token=credentials_dict.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=credentials_dict.get("client_id"),
            client_secret=credentials_dict.get("client_secret")
        )
    return None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_monthly_performance(days_back=90):
    """Get monthly performance metrics"""
    query = f"""
    WITH chain_performance AS (
      SELECT 
        COALESCE(s.chain, 'Unknown') as chain,
        DATE_TRUNC(cs.chargeback_date, MONTH) as month,
        cs.platform,
        COUNT(DISTINCT cs.slug) as active_locations,
        SUM(cs.orders_count) as total_orders,
        SUM(cs.won_disputes_count) as disputes_won,
        ROUND(SUM(cs.customer_refunds + cs.unfulfilled_refunds + cs.unfulfilled_sales), 2) as total_potential,
        ROUND(SUM(cs.customer_refunds_won_disputes + cs.unfulfilled_refunds_won_disputes + cs.unfulfilled_sales_won_disputes), 2) as total_recovered
      FROM `merchant_portal_export.chargeback_split_summary` cs
      LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s
        ON cs.slug = s.slug
      WHERE cs.chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
      GROUP BY 1, 2, 3
    )
    SELECT 
      chain,
      month,
      platform,
      active_locations,
      total_recovered,
      ROUND(total_recovered / NULLIF(active_locations, 0), 2) as avg_recovery_per_location,
      ROUND(100.0 * total_recovered / NULLIF(total_potential, 0), 2) as recovery_rate_pct,
      ROUND(100.0 * disputes_won / NULLIF(total_orders, 0), 2) as win_rate_pct,
      total_potential
    FROM chain_performance
    ORDER BY month DESC, total_recovered DESC
    """
    
    credentials = get_credentials()
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    
    df['month'] = pd.to_datetime(df['month'])
    return df

@st.cache_data(ttl=3600)
def get_platform_performance():
    """Get platform-specific performance metrics"""
    query = """
    WITH performance_matrix AS (
      SELECT 
        platform,
        CASE 
          WHEN LOWER(COALESCE(error_category, '')) LIKE '%inaccurate%' 
            OR LOWER(COALESCE(error_subcategory, '')) LIKE '%wrong%'
            OR LOWER(COALESCE(error_subcategory, '')) LIKE '%missing%'
          THEN 'Inaccurate Order'
          WHEN LOWER(COALESCE(error_category, '')) LIKE '%cancel%'
            OR LOWER(COALESCE(error_subcategory, '')) LIKE '%cancel%'
          THEN 'Cancelled Order'
          ELSE 'Other'
        END AS issue_category,
        SUM(orders_count) as total_orders,
        SUM(won_disputes_count) as disputes_won,
        SUM(customer_refunds + unfulfilled_refunds + unfulfilled_sales) as potential_recovery,
        SUM(customer_refunds_won_disputes + unfulfilled_refunds_won_disputes + unfulfilled_sales_won_disputes) as actual_recovery
      FROM `merchant_portal_export.chargeback_split_summary`
      WHERE chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1, 2
    )
    SELECT 
      platform,
      issue_category,
      total_orders,
      disputes_won,
      ROUND(100.0 * disputes_won / NULLIF(total_orders, 0), 2) as win_rate_pct,
      ROUND(potential_recovery, 2) as potential_recovery,
      ROUND(actual_recovery, 2) as actual_recovery,
      ROUND(100.0 * actual_recovery / NULLIF(potential_recovery, 0), 2) as recovery_rate_pct
    FROM performance_matrix
    WHERE total_orders > 0
    ORDER BY actual_recovery DESC
    """
    
    credentials = get_credentials()
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)

@st.cache_data(ttl=3600)
def get_location_profitability():
    """Get location profitability distribution"""
    query = """
    WITH location_recovery AS (
      SELECT 
        cs.slug,
        COALESCE(s.chain, 'Unknown') as chain,
        SUM(cs.customer_refunds_won_disputes + cs.unfulfilled_refunds_won_disputes + cs.unfulfilled_sales_won_disputes) as monthly_recovery
      FROM `merchant_portal_export.chargeback_split_summary` cs
      LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s
        ON cs.slug = s.slug
      WHERE cs.chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1, 2
    )
    SELECT 
      COUNT(DISTINCT slug) as total_locations,
      COUNT(DISTINCT CASE WHEN monthly_recovery >= 500 THEN slug END) as tier_500_plus,
      COUNT(DISTINCT CASE WHEN monthly_recovery >= 300 THEN slug END) as tier_300_plus,
      COUNT(DISTINCT CASE WHEN monthly_recovery >= 150 THEN slug END) as tier_profitable,
      COUNT(DISTINCT CASE WHEN monthly_recovery >= 100 AND monthly_recovery < 150 THEN slug END) as tier_breakeven,
      COUNT(DISTINCT CASE WHEN monthly_recovery < 100 THEN slug END) as tier_unprofitable,
      ROUND(100.0 * COUNT(DISTINCT CASE WHEN monthly_recovery >= 150 THEN slug END) / COUNT(DISTINCT slug), 2) as pct_profitable,
      SUM(monthly_recovery) as total_recovered,
      ROUND(AVG(monthly_recovery), 2) as avg_recovery
    FROM location_recovery
    """
    
    credentials = get_credentials()
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)

@st.cache_data(ttl=3600)
def get_daily_trend(days=30):
    """Get daily recovery trend"""
    query = f"""
    SELECT 
      chargeback_date,
      SUM(orders_count) as daily_orders,
      SUM(won_disputes_count) as daily_won,
      ROUND(SUM(customer_refunds_won_disputes + unfulfilled_refunds_won_disputes + unfulfilled_sales_won_disputes), 2) as daily_recovery,
      ROUND(100.0 * SUM(won_disputes_count) / NULLIF(SUM(orders_count), 0), 2) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary`
    WHERE chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY 1
    ORDER BY 1 DESC
    """
    
    credentials = get_credentials()
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    df['chargeback_date'] = pd.to_datetime(df['chargeback_date'])
    return df

@st.cache_data(ttl=3600)
def get_top_chains():
    """Get top performing chains"""
    query = """
    WITH chain_metrics AS (
      SELECT 
        COALESCE(s.chain, 'Unknown') as chain,
        COUNT(DISTINCT cs.slug) as locations,
        SUM(cs.orders_count) as total_orders,
        SUM(cs.won_disputes_count) as disputes_won,
        ROUND(SUM(cs.customer_refunds + cs.unfulfilled_refunds + cs.unfulfilled_sales), 2) as total_potential,
        ROUND(SUM(cs.customer_refunds_won_disputes + cs.unfulfilled_refunds_won_disputes + cs.unfulfilled_sales_won_disputes), 2) as total_recovered
      FROM `merchant_portal_export.chargeback_split_summary` cs
      LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s
        ON cs.slug = s.slug
      WHERE cs.chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1
      HAVING total_orders > 100
    )
    SELECT 
      chain,
      locations,
      total_orders,
      total_recovered,
      ROUND(total_recovered / NULLIF(locations, 0), 2) as avg_per_location,
      ROUND(100.0 * disputes_won / NULLIF(total_orders, 0), 2) as win_rate,
      ROUND(100.0 * total_recovered / NULLIF(total_potential, 0), 2) as recovery_rate,
      CASE 
        WHEN total_recovered / NULLIF(locations, 0) >= 300 THEN '⭐ Star'
        WHEN total_recovered / NULLIF(locations, 0) >= 150 THEN '✅ Profitable'
        WHEN total_recovered / NULLIF(locations, 0) >= 100 THEN '⚠️ Break-even'
        ELSE '❌ Unprofitable'
      END as tier
    FROM chain_metrics
    ORDER BY total_recovered DESC
    LIMIT 20
    """
    
    credentials = get_credentials()
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)

# ============================================
# DASHBOARD LAYOUT
# ============================================

def main():
    # Header
    st.title("💰 Recovery MIS Dashboard")
    st.markdown("### Real-time Recovery Operations Intelligence")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Controls")
        
        # Date range selector
        date_range = st.selectbox(
            "Select Time Period",
            ["Last 30 Days", "Last 60 Days", "Last 90 Days", "Year to Date"]
        )
        
        # Map to days
        days_map = {
            "Last 30 Days": 30,
            "Last 60 Days": 60,
            "Last 90 Days": 90,
            "Year to Date": 365
        }
        days_back = days_map[date_range]
        
        # Dashboard view selector
        view = st.radio(
            "Select Dashboard View",
            ["📊 Executive Summary", "⚙️ Operations", "💵 Financial Impact", "📈 Trends & Analytics"]
        )
        
        st.markdown("---")
        st.info("Dashboard refreshes every hour")
    
    # Main content based on selected view
    if view == "📊 Executive Summary":
        show_executive_summary(days_back)
    elif view == "⚙️ Operations":
        show_operations_dashboard()
    elif view == "💵 Financial Impact":
        show_financial_dashboard(days_back)
    elif view == "📈 Trends & Analytics":
        show_trends_dashboard()

def show_executive_summary(days_back):
    """Executive Summary Dashboard"""
    
    # Load data
    with st.spinner("Loading data..."):
        monthly_df = get_monthly_performance(days_back)
        profitability_df = get_location_profitability()
        top_chains_df = get_top_chains()
    
    # KPI Cards
    st.markdown("### 📊 Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_recovered = monthly_df['total_recovered'].sum()
        st.metric(
            "Total Recovered",
            f"${total_recovered:,.0f}",
            f"{(total_recovered / 1000000):.1f}M"
        )
    
    with col2:
        avg_recovery_rate = monthly_df['recovery_rate_pct'].mean()
        st.metric(
            "Recovery Rate",
            f"{avg_recovery_rate:.1f}%",
            f"{'🟢' if avg_recovery_rate > 30 else '🔴'}"
        )
    
    with col3:
        if not profitability_df.empty:
            pct_profitable = profitability_df['pct_profitable'].iloc[0]
            st.metric(
                "Profitable Locations",
                f"{pct_profitable:.1f}%",
                f"{'🔴 Critical' if pct_profitable < 10 else '🟡'}"
            )
    
    with col4:
        active_locations = monthly_df['active_locations'].nunique()
        st.metric(
            "Active Locations",
            f"{active_locations:,}",
            "Total"
        )
    
    with col5:
        avg_per_location = monthly_df['avg_recovery_per_location'].mean()
        st.metric(
            "Avg per Location",
            f"${avg_per_location:.0f}",
            f"{'🔴 < $150' if avg_per_location < 150 else '🟢'}"
        )
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Monthly Recovery Trend")
        monthly_trend = monthly_df.groupby('month').agg({
            'total_recovered': 'sum',
            'recovery_rate_pct': 'mean'
        }).reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=monthly_trend['month'],
            y=monthly_trend['total_recovered'],
            name='Total Recovered',
            marker_color='lightblue'
        ))
        fig.add_trace(go.Scatter(
            x=monthly_trend['month'],
            y=monthly_trend['recovery_rate_pct'],
            name='Recovery Rate %',
            yaxis='y2',
            marker_color='red',
            mode='lines+markers'
        ))
        fig.update_layout(
            yaxis2=dict(
                title='Recovery Rate %',
                overlaying='y',
                side='right'
            ),
            height=400,
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Platform Performance")
        platform_summary = monthly_df.groupby('platform').agg({
            'total_recovered': 'sum',
            'recovery_rate_pct': 'mean',
            'win_rate_pct': 'mean'
        }).reset_index()
        
        fig = px.bar(
            platform_summary,
            x='platform',
            y='total_recovered',
            color='recovery_rate_pct',
            color_continuous_scale='RdYlGn',
            labels={'total_recovered': 'Total Recovered ($)', 'recovery_rate_pct': 'Recovery Rate %'},
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Location Profitability Distribution
    if not profitability_df.empty:
        st.markdown("### 💰 Location Profitability Distribution")
        
        prof_data = {
            'Category': ['$500+', '$300-500', '$150-300', '$100-150', '<$100'],
            'Locations': [
                profitability_df['tier_500_plus'].iloc[0],
                profitability_df['tier_300_plus'].iloc[0] - profitability_df['tier_500_plus'].iloc[0],
                profitability_df['tier_profitable'].iloc[0] - profitability_df['tier_300_plus'].iloc[0],
                profitability_df['tier_breakeven'].iloc[0],
                profitability_df['tier_unprofitable'].iloc[0]
            ]
        }
        
        prof_df = pd.DataFrame(prof_data)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                prof_df,
                x='Locations',
                y='Category',
                orientation='h',
                color='Category',
                color_discrete_map={
                    '$500+': '#0d9488',
                    '$300-500': '#10b981',
                    '$150-300': '#84cc16',
                    '$100-150': '#eab308',
                    '<$100': '#ef4444'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 🎯 Profitability Analysis")
            st.warning(f"⚠️ Only {profitability_df['pct_profitable'].iloc[0]:.1f}% of locations are profitable at $150/month pricing")
            st.info(f"Average Recovery: ${profitability_df['avg_recovery'].iloc[0]:.0f}/location")
            
            if profitability_df['pct_profitable'].iloc[0] < 10:
                st.error("🚨 Critical: Business model needs urgent review")
    
    # Top Chains Table
    st.markdown("### 🏆 Top Performing Chains")
    
    if not top_chains_df.empty:
        display_df = top_chains_df[['chain', 'locations', 'avg_per_location', 'recovery_rate', 'tier']].head(10)
        
        # Style the dataframe
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "chain": "Chain",
                "locations": st.column_config.NumberColumn("Locations", format="%d"),
                "avg_per_location": st.column_config.NumberColumn("Avg $/Location", format="$%.0f"),
                "recovery_rate": st.column_config.NumberColumn("Recovery %", format="%.1f%%"),
                "tier": "Status"
            }
        )

def show_operations_dashboard():
    """Operations Dashboard"""
    
    st.markdown("### ⚙️ Operations Dashboard")
    
    # Load data
    with st.spinner("Loading operational data..."):
        platform_df = get_platform_performance()
        daily_df = get_daily_trend(30)
    
    # Platform x Issue Type Matrix
    st.markdown("### 🎯 Platform & Issue Type Performance")
    
    if not platform_df.empty:
        # Pivot for heatmap
        pivot_df = platform_df.pivot_table(
            index='issue_category',
            columns='platform',
            values='recovery_rate_pct',
            fill_value=0
        )
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            fig = px.imshow(
                pivot_df,
                labels=dict(x="Platform", y="Issue Category", color="Recovery Rate %"),
                color_continuous_scale='RdYlGn',
                aspect="auto"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 🎯 Key Insights")
            
            # Find best and worst combinations
            best = platform_df.nlargest(1, 'recovery_rate_pct').iloc[0]
            worst = platform_df[platform_df['recovery_rate_pct'] > 0].nsmallest(1, 'recovery_rate_pct').iloc[0]
            
            st.success(f"**Best:** {best['platform']} - {best['issue_category']} ({best['recovery_rate_pct']:.1f}%)")
            st.error(f"**Worst:** {worst['platform']} - {worst['issue_category']} ({worst['recovery_rate_pct']:.1f}%)")
            
            # Recommendations
            cancelled_recovery = platform_df[platform_df['issue_category'] == 'Cancelled Order']['recovery_rate_pct'].mean()
            if cancelled_recovery < 1:
                st.warning(f"⚠️ Cancelled Orders: {cancelled_recovery:.1f}% recovery - Consider stopping disputes")
    
    # Daily Operations Trend
    st.markdown("### 📊 Daily Operations Trend")
    
    if not daily_df.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['daily_recovery'],
            name='Daily Recovery ($)',
            line=dict(color='blue', width=2),
            mode='lines+markers'
        ))
        
        # Add 7-day moving average
        daily_df['ma7'] = daily_df['daily_recovery'].rolling(window=7).mean()
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['ma7'],
            name='7-Day Average',
            line=dict(color='red', width=2, dash='dash'),
            mode='lines'
        ))
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            xaxis_title="Date",
            yaxis_title="Recovery ($)"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Win Rate Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🎯 Win Rate by Platform")
        platform_win = platform_df.groupby('platform')['win_rate_pct'].mean().reset_index()
        
        fig = px.bar(
            platform_win,
            x='platform',
            y='win_rate_pct',
            color='win_rate_pct',
            color_continuous_scale='RdYlGn',
            labels={'win_rate_pct': 'Win Rate %'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 💰 Recovery Value by Platform")
        platform_value = platform_df.groupby('platform')['actual_recovery'].sum().reset_index()
        
        fig = px.pie(
            platform_value,
            values='actual_recovery',
            names='platform',
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)

def show_financial_dashboard(days_back):
    """Financial Impact Dashboard"""
    
    st.markdown("### 💵 Financial Impact Dashboard")
    
    # Load monthly data
    monthly_df = get_monthly_performance(days_back)
    
    # Financial Summary
    col1, col2, col3, col4 = st.columns(4)
    
    total_potential = monthly_df['total_potential'].sum()
    total_recovered = monthly_df['total_recovered'].sum()
    total_unrecovered = total_potential - total_recovered
    recovery_rate = (total_recovered / total_potential * 100) if total_potential > 0 else 0
    
    with col1:
        st.metric("Total Opportunity", f"${total_potential:,.0f}")
    
    with col2:
        st.metric("Total Recovered", f"${total_recovered:,.0f}")
    
    with col3:
        st.metric("Unrecovered", f"${total_unrecovered:,.0f}")
    
    with col4:
        st.metric("Recovery Rate", f"{recovery_rate:.1f}%")
    
    st.markdown("---")
    
    # Monthly Financial Trend
    st.markdown("### 📈 Monthly Financial Performance")
    
    monthly_summary = monthly_df.groupby('month').agg({
        'total_potential': 'sum',
        'total_recovered': 'sum',
        'active_locations': 'nunique'
    }).reset_index()
    
    monthly_summary['unrecovered'] = monthly_summary['total_potential'] - monthly_summary['total_recovered']
    monthly_summary['estimated_fees'] = monthly_summary['active_locations'] * 150
    monthly_summary['net_value'] = monthly_summary['total_recovered'] - monthly_summary['estimated_fees']
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=monthly_summary['month'],
        y=monthly_summary['total_recovered'],
        name='Recovered',
        marker_color='green'
    ))
    
    fig.add_trace(go.Bar(
        x=monthly_summary['month'],
        y=monthly_summary['unrecovered'],
        name='Unrecovered',
        marker_color='red'
    ))
    
    fig.add_trace(go.Scatter(
        x=monthly_summary['month'],
        y=monthly_summary['net_value'],
        name='Net Value (Recovery - Fees)',
        line=dict(color='blue', width=3),
        mode='lines+markers'
    ))
    
    fig.update_layout(
        barmode='stack',
        height=500,
        hovermode='x unified',
        yaxis_title="Amount ($)",
        xaxis_title="Month"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Platform Financial Breakdown
    st.markdown("### 💰 Platform Financial Contribution")
    
    platform_financial = monthly_df.groupby('platform').agg({
        'total_potential': 'sum',
        'total_recovered': 'sum'
    }).reset_index()
    
    platform_financial['recovery_rate'] = (platform_financial['total_recovered'] / 
                                           platform_financial['total_potential'] * 100)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            platform_financial,
            x='platform',
            y=['total_recovered', 'total_potential'],
            barmode='group',
            labels={'value': 'Amount ($)', 'variable': 'Type'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Recovery rate gauge for each platform
        st.markdown("#### Recovery Rates by Platform")
        for _, row in platform_financial.iterrows():
            st.metric(
                row['platform'],
                f"{row['recovery_rate']:.1f}%",
                f"${row['total_recovered']:,.0f}"
            )

def show_trends_dashboard():
    """Trends & Analytics Dashboard"""
    
    st.markdown("### 📈 Trends & Analytics")
    
    # Load data
    daily_df = get_daily_trend(60)
    monthly_df = get_monthly_performance(180)
    
    # Trend Analysis
    st.markdown("### 📊 Recovery Trend Analysis")
    
    if not daily_df.empty:
        # Calculate moving averages
        daily_df['ma7'] = daily_df['daily_recovery'].rolling(window=7).mean()
        daily_df['ma30'] = daily_df['daily_recovery'].rolling(window=30).mean()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['daily_recovery'],
            name='Daily Recovery',
            line=dict(color='lightgray', width=1),
            mode='lines'
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['ma7'],
            name='7-Day MA',
            line=dict(color='blue', width=2),
            mode='lines'
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['ma30'],
            name='30-Day MA',
            line=dict(color='red', width=2),
            mode='lines'
        ))
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            xaxis_title="Date",
            yaxis_title="Recovery ($)"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Trend direction
        if len(daily_df) > 30:
            recent_avg = daily_df.head(7)['daily_recovery'].mean()
            older_avg = daily_df.iloc[23:30]['daily_recovery'].mean()
            
            if recent_avg > older_avg * 1.1:
                st.success("📈 Trend: Improving (7-day avg up >10% vs 30-day avg)")
            elif recent_avg < older_avg * 0.9:
                st.error("📉 Trend: Declining (7-day avg down >10% vs 30-day avg)")
            else:
                st.info("➡️ Trend: Stable")
    
    # Cohort Analysis
    st.markdown("### 👥 Monthly Cohort Performance")
    
    if not monthly_df.empty:
        # Create cohort view
        cohort_pivot = monthly_df.pivot_table(
            index='chain',
            columns='month',
            values='recovery_rate_pct',
            aggfunc='mean'
        ).fillna(0)
        
        # Take top 10 chains
        top_chains = monthly_df.groupby('chain')['total_recovered'].sum().nlargest(10).index
        cohort_pivot = cohort_pivot.loc[cohort_pivot.index.isin(top_chains)]
        
        fig = px.imshow(
            cohort_pivot,
            labels=dict(x="Month", y="Chain", color="Recovery Rate %"),
            color_continuous_scale='RdYlGn',
            aspect="auto"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Win Rate Trend
    st.markdown("### 🎯 Win Rate Trend")
    
    if not daily_df.empty:
        daily_df['ma7_win_rate'] = daily_df['win_rate'].rolling(window=7).mean()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['win_rate'],
            name='Daily Win Rate',
            line=dict(color='lightblue', width=1),
            mode='lines',
            fill='tozeroy'
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_df['chargeback_date'],
            y=daily_df['ma7_win_rate'],
            name='7-Day MA Win Rate',
            line=dict(color='darkblue', width=2),
            mode='lines'
        ))
        
        fig.update_layout(
            height=350,
            hovermode='x unified',
            xaxis_title="Date",
            yaxis_title="Win Rate (%)"
        )
        
        st.plotly_chart(fig, use_container_width=True)

# ============================================
# RUN THE APP
# ============================================

if __name__ == "__main__":
    # Check if client is initialized
    client = init_bigquery_client()
    
    if client:
        try:
            main()
        except Exception as e:
            st.error(f"Error running dashboard: {e}")
            st.info("""
            ### Setup Instructions:
            
            1. **Install Requirements:**
            ```bash
            pip install streamlit pandas plotly google-cloud-bigquery pandas-gbq
            ```
            
            2. **Set up Google Cloud Credentials:**
            - Option A: Set environment variable
            ```bash
            export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
            ```
            
            - Option B: Use gcloud CLI
            ```bash
            gcloud auth application-default login
            ```
            
            3. **Update Project ID:**
            - Change `PROJECT_ID` at the top of this file to your GCP project
            
            4. **Run the Dashboard:**
            ```bash
            streamlit run recovery_dashboard.py
            ```
            """)
    else:
        st.error("Failed to initialize BigQuery client")
        st.info("Please set up your Google Cloud credentials and update the PROJECT_ID")