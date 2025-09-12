"""
Enhanced Win Rate Dashboard with Filters and Trends
Includes date filters, chain filters, and trend line graphs
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date, timedelta
import os

# Disable metadata server to avoid timeout
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

from google.cloud import bigquery
import pandas_gbq

# Page configuration
st.set_page_config(
    page_title="Win Rate Dashboard - Enhanced",
    page_icon="📊",
    layout="wide"
)

# Configuration
PROJECT_ID = 'arboreal-vision-339901'

# Title
st.title("📊 Enhanced Win Rate Dashboard")
st.caption("Advanced analytics with filters and trend visualization")

# Sidebar for Filters
with st.sidebar:
    st.header("🎛️ Dashboard Filters")
    
    # Date Range Filter
    st.subheader("📅 Date Range")
    date_option = st.selectbox(
        "Select Period",
        ["Last 30 Days", "Last 90 Days", "Year 2025", "Year 2024", "Custom Range"]
    )
    
    if date_option == "Last 30 Days":
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()
    elif date_option == "Last 90 Days":
        start_date = date.today() - timedelta(days=90)
        end_date = date.today()
    elif date_option == "Year 2025":
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
    elif date_option == "Year 2024":
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
    else:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start", date.today() - timedelta(days=90))
        with col2:
            end_date = st.date_input("End", date.today())
    
    st.caption(f"📍 {start_date} to {end_date}")
    
    # Aggregation Level
    st.subheader("📊 Aggregation")
    aggregation = st.radio(
        "Group by",
        ["Daily", "Weekly", "Monthly"]
    )
    
    # Platform Filter
    st.subheader("🎯 Platform")
    platform_filter = st.multiselect(
        "Select Platforms",
        ["Doordash", "Grubhub", "UberEats"],
        default=["Doordash", "Grubhub", "UberEats"]
    )
    
    # Chain Filter
    st.subheader("🏢 Chain Filter")
    chain_search = st.text_input("Search chain (optional)", "")
    
    # Category Filter
    st.subheader("📋 Error Category")
    category_filter = st.multiselect(
        "Select Categories",
        ["INACCURATE", "CANCELLED", "MISSED", "UNCATEGORIZED"],
        default=["INACCURATE", "CANCELLED", "MISSED", "UNCATEGORIZED"]
    )
    
    # Subcategory Filter
    st.subheader("📑 Error Subcategory")
    subcategory_search = st.text_input("Search subcategory (optional)", "")
    
    # Common subcategories for quick selection
    common_subcategories = st.multiselect(
        "Or select common subcategories",
        ["Incomplete Order", "Wrong Order", "Technical", "Customer Cancelled", "Out of Stock"],
        default=[]
    )

# Validation Box
with st.container():
    st.error("""
    ⚠️ **CRITICAL VALIDATION POINTS**
    
    ✅ **Formula:** Win Rate = ACCEPTED / (ACCEPTED + DENIED)
    ✅ **Only Settled:** external_status IN ('ACCEPTED', 'DENIED')
    ❌ **Excludes:** IN_PROGRESS, TO_BE_RAISED, EXPIRED, etc.
    """)

# Query Functions with Filters
@st.cache_data(ttl=3600)
def get_chains_list():
    """Get list of available chains"""
    query = """
    SELECT DISTINCT 
        COALESCE(s.chain, 'Unknown') as chain
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE s.chain IS NOT NULL
    ORDER BY chain
    LIMIT 500
    """
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df['chain'].tolist()
    except:
        return []

@st.cache_data(ttl=3600)
def get_category_subcategory_breakdown(start_date, end_date, platforms, chain_search):
    """Get win rates by category and subcategory"""
    
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if chain_search:
        filters.append(f"UPPER(s.chain) LIKE UPPER('%{chain_search}%')")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    SELECT 
        cs.error_category,
        cs.error_subcategory,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE {where_clause}
    GROUP BY cs.error_category, cs.error_subcategory
    HAVING COUNT(*) > 10  -- Filter out very small samples
    ORDER BY total_settled DESC
    LIMIT 50
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_win_rate_trend(start_date, end_date, aggregation, platforms, chain_search, categories, subcategory_search, common_subcategories):
    """Get win rate trend data"""
    
    # Build date truncation based on aggregation
    if aggregation == "Daily":
        date_trunc = "DATE(chargeback_date)"
    elif aggregation == "Weekly":
        date_trunc = "DATE_TRUNC(chargeback_date, WEEK)"
    else:
        date_trunc = "DATE_TRUNC(chargeback_date, MONTH)"
    
    # Build filters
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if chain_search:
        filters.append(f"UPPER(s.chain) LIKE UPPER('%{chain_search}%')")
    
    # Category filter
    if categories:
        category_list = "','".join(categories)
        filters.append(f"UPPER(cs.error_category) IN ('{category_list}')")
    
    # Subcategory filter
    if subcategory_search:
        filters.append(f"UPPER(cs.error_subcategory) LIKE UPPER('%{subcategory_search}%')")
    
    if common_subcategories:
        subcategory_conditions = []
        for subcat in common_subcategories:
            subcategory_conditions.append(f"UPPER(cs.error_subcategory) LIKE UPPER('%{subcat}%')")
        if subcategory_conditions:
            filters.append(f"({' OR '.join(subcategory_conditions)})")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    SELECT 
        {date_trunc} as period,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE {where_clause}
    GROUP BY period
    ORDER BY period
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        df['period'] = pd.to_datetime(df['period'])
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_platform_trend(start_date, end_date, aggregation, platforms, chain_search, categories):
    """Get platform-specific win rate trends"""
    
    # Build date truncation
    if aggregation == "Daily":
        date_trunc = "DATE(chargeback_date)"
    elif aggregation == "Weekly":
        date_trunc = "DATE_TRUNC(chargeback_date, WEEK)"
    else:
        date_trunc = "DATE_TRUNC(chargeback_date, MONTH)"
    
    # Build filters
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if chain_search:
        filters.append(f"UPPER(s.chain) LIKE UPPER('%{chain_search}%')")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    SELECT 
        {date_trunc} as period,
        platform,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE {where_clause}
    GROUP BY period, platform
    ORDER BY period, platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        df['period'] = pd.to_datetime(df['period'])
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_issue_type_trend(start_date, end_date, aggregation, platforms, chain_search):
    """Get issue type win rate trends"""
    
    # Build date truncation
    if aggregation == "Daily":
        date_trunc = "DATE(chargeback_date)"
    elif aggregation == "Weekly":
        date_trunc = "DATE_TRUNC(chargeback_date, WEEK)"
    else:
        date_trunc = "DATE_TRUNC(chargeback_date, MONTH)"
    
    # Build filters
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if chain_search:
        filters.append(f"UPPER(s.chain) LIKE UPPER('%{chain_search}%')")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    WITH categorized AS (
        SELECT 
            {date_trunc} as period,
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' THEN 'Inaccurate'
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%CANCEL%' THEN 'Cancelled'
                ELSE 'Other'
            END AS issue_type,
            cs.external_status
        FROM `merchant_portal_export.chargeback_split_summary` cs
        LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
            ON cs.slug = s.slug
        WHERE {where_clause}
    )
    SELECT 
        period,
        issue_type,
        COUNT(CASE WHEN external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN external_status = 'DENIED' THEN 1 END) as denied,
        ROUND(100.0 * COUNT(CASE WHEN external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM categorized
    GROUP BY period, issue_type
    ORDER BY period, issue_type
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        df['period'] = pd.to_datetime(df['period'])
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_summary_metrics(start_date, end_date, platforms, chain_search, categories):
    """Get summary metrics for the selected period"""
    
    # Build filters
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if chain_search:
        filters.append(f"UPPER(s.chain) LIKE UPPER('%{chain_search}%')")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    SELECT 
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE {where_clause}
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_win_rates(start_date, end_date, platforms=None, categories=None, min_disputes=10):
    """Get win rates by restaurant chain"""
    
    filters = [
        f"DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'",
        "external_status IN ('ACCEPTED', 'DENIED')"
    ]
    
    if platforms:
        platform_list = "','".join(platforms)
        filters.append(f"platform IN ('{platform_list}')")
    
    if categories:
        category_list = "','".join(categories)
        filters.append(f"UPPER(cs.error_category) IN ('{category_list}')")
    
    where_clause = " AND ".join(filters)
    
    query = f"""
    SELECT 
        s.chain,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE {where_clause}
        AND s.chain IS NOT NULL
    GROUP BY s.chain
    HAVING COUNT(*) >= {min_disputes}
    ORDER BY total_settled DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df
    except Exception as e:
        st.error(f"Error loading chain data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_platform_matrix(start_date, end_date, top_n_chains=15):
    """Get win rates by chain and platform"""
    
    query = f"""
    WITH top_chains AS (
        SELECT s.chain
        FROM `merchant_portal_export.chargeback_split_summary` cs
        LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
            ON cs.slug = s.slug
        WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
            AND external_status IN ('ACCEPTED', 'DENIED')
            AND s.chain IS NOT NULL
        GROUP BY s.chain
        ORDER BY COUNT(*) DESC
        LIMIT {top_n_chains}
    )
    SELECT 
        s.chain,
        cs.platform,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    INNER JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    INNER JOIN top_chains tc ON s.chain = tc.chain
    WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
        AND external_status IN ('ACCEPTED', 'DENIED')
    GROUP BY s.chain, cs.platform
    HAVING COUNT(*) > 5
    ORDER BY s.chain, cs.platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df
    except Exception as e:
        st.error(f"Error loading chain-platform data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_issue_type_breakdown(start_date, end_date, top_n_chains=15):
    """Get win rates by chain and issue type"""
    
    query = f"""
    WITH top_chains AS (
        SELECT s.chain
        FROM `merchant_portal_export.chargeback_split_summary` cs
        LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
            ON cs.slug = s.slug
        WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
            AND external_status IN ('ACCEPTED', 'DENIED')
            AND s.chain IS NOT NULL
        GROUP BY s.chain
        ORDER BY COUNT(*) DESC
        LIMIT {top_n_chains}
    ),
    categorized AS (
        SELECT 
            s.chain,
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' THEN 'Inaccurate Order'
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%CANCEL%' THEN 'Cancelled Order'
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%MISSED%' THEN 'Missed Order'
                ELSE 'Other'
            END AS issue_type,
            cs.external_status
        FROM `merchant_portal_export.chargeback_split_summary` cs
        INNER JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
            ON cs.slug = s.slug
        INNER JOIN top_chains tc ON s.chain = tc.chain
        WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
            AND external_status IN ('ACCEPTED', 'DENIED')
    )
    SELECT 
        chain,
        issue_type,
        COUNT(CASE WHEN external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM categorized
    GROUP BY chain, issue_type
    ORDER BY chain, total_settled DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return df
    except Exception as e:
        st.error(f"Error loading chain-issue data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_recovery_per_location(start_date, end_date, top_n_chains=10):
    """Get average recovery amount per location for each chain by month"""
    
    query = f"""
    WITH top_chains AS (
        SELECT s.chain
        FROM `merchant_portal_export.chargeback_split_summary` cs
        LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
            ON cs.slug = s.slug
        WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
            AND external_status = 'ACCEPTED'
            AND s.chain IS NOT NULL
        GROUP BY s.chain
        ORDER BY SUM(COALESCE(customer_refunds_won_disputes, 0) + 
                     COALESCE(unfulfilled_refunds_won_disputes, 0) + 
                     COALESCE(unfulfilled_sales_won_disputes, 0)) DESC
        LIMIT {top_n_chains}
    )
    SELECT 
        DATE_TRUNC(cs.chargeback_date, MONTH) as month,
        s.chain,
        COUNT(DISTINCT cs.slug) as active_locations,
        SUM(COALESCE(cs.customer_refunds_won_disputes, 0) + 
            COALESCE(cs.unfulfilled_refunds_won_disputes, 0) + 
            COALESCE(cs.unfulfilled_sales_won_disputes, 0)) as total_recovered,
        ROUND(SUM(COALESCE(cs.customer_refunds_won_disputes, 0) + 
                  COALESCE(cs.unfulfilled_refunds_won_disputes, 0) + 
                  COALESCE(cs.unfulfilled_sales_won_disputes, 0)) / 
              NULLIF(COUNT(DISTINCT cs.slug), 0), 2) as avg_per_location
    FROM `merchant_portal_export.chargeback_split_summary` cs
    INNER JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    INNER JOIN top_chains tc ON s.chain = tc.chain
    WHERE DATE(cs.chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
        AND cs.external_status = 'ACCEPTED'
    GROUP BY month, s.chain
    ORDER BY month, s.chain
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        df['month'] = pd.to_datetime(df['month'])
        return df
    except Exception as e:
        st.error(f"Error loading recovery per location data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_trend(start_date, end_date, selected_chains, aggregation="Daily"):
    """Get win rate trends for specific chains"""
    
    if aggregation == "Daily":
        date_trunc = "DATE(chargeback_date)"
    elif aggregation == "Weekly":
        date_trunc = "DATE_TRUNC(chargeback_date, WEEK)"
    else:
        date_trunc = "DATE_TRUNC(chargeback_date, MONTH)"
    
    chain_list = "','".join(selected_chains)
    
    query = f"""
    SELECT 
        {date_trunc} as period,
        s.chain,
        COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) as accepted,
        COUNT(CASE WHEN cs.external_status = 'DENIED' THEN 1 END) as denied,
        COUNT(*) as total_settled,
        ROUND(100.0 * COUNT(CASE WHEN cs.external_status = 'ACCEPTED' THEN 1 END) / 
              NULLIF(COUNT(*), 0), 1) as win_rate
    FROM `merchant_portal_export.chargeback_split_summary` cs
    LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` s 
        ON cs.slug = s.slug
    WHERE DATE(chargeback_date) BETWEEN '{start_date}' AND '{end_date}'
        AND external_status IN ('ACCEPTED', 'DENIED')
        AND s.chain IN ('{chain_list}')
    GROUP BY period, s.chain
    ORDER BY period, s.chain
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        df['period'] = pd.to_datetime(df['period'])
        return df
    except Exception as e:
        st.error(f"Error loading chain trend data: {e}")
        return pd.DataFrame()

# Main Dashboard
def main():
    # Load data based on filters
    with st.spinner("Loading data..."):
        trend_df = get_win_rate_trend(start_date, end_date, aggregation, platform_filter, chain_search, 
                                     category_filter, subcategory_search, common_subcategories)
        platform_trend_df = get_platform_trend(start_date, end_date, aggregation, platform_filter, chain_search, category_filter)
        issue_trend_df = get_issue_type_trend(start_date, end_date, aggregation, platform_filter, chain_search)
        summary_df = get_summary_metrics(start_date, end_date, platform_filter, chain_search, category_filter)
        category_breakdown_df = get_category_subcategory_breakdown(start_date, end_date, platform_filter, chain_search)
        
        # Load chain data
        chain_df = get_chain_win_rates(start_date, end_date, platform_filter, category_filter)
        chain_platform_matrix = get_chain_platform_matrix(start_date, end_date)
        chain_issue_breakdown = get_chain_issue_type_breakdown(start_date, end_date)
        chain_recovery_df = get_chain_recovery_per_location(start_date, end_date)
    
    # Summary Metrics
    st.header("📈 Summary Metrics")
    
    if not summary_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Win Rate", f"{summary_df['win_rate'].iloc[0]:.1f}%")
        
        with col2:
            st.metric("Accepted", f"{summary_df['accepted'].iloc[0]:,.0f}")
        
        with col3:
            st.metric("Denied", f"{summary_df['denied'].iloc[0]:,.0f}")
        
        with col4:
            st.metric("Total Settled", f"{summary_df['total_settled'].iloc[0]:,.0f}")
    
    st.markdown("---")
    
    # Overall Win Rate Trend
    st.header(f"📊 Win Rate Trend ({aggregation})")
    
    if not trend_df.empty:
        fig = go.Figure()
        
        # Add win rate line
        fig.add_trace(go.Scatter(
            x=trend_df['period'],
            y=trend_df['win_rate'],
            mode='lines+markers',
            name='Win Rate',
            line=dict(color='#00cc96', width=3),
            marker=dict(size=8),
            hovertemplate='%{x|%Y-%m-%d}<br>Win Rate: %{y:.1f}%<extra></extra>'
        ))
        
        # Add volume bars
        fig.add_trace(go.Bar(
            x=trend_df['period'],
            y=trend_df['total_settled'],
            name='Volume',
            yaxis='y2',
            opacity=0.3,
            marker_color='lightgray',
            hovertemplate='%{x|%Y-%m-%d}<br>Volume: %{y:,.0f}<extra></extra>'
        ))
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            yaxis=dict(title='Win Rate (%)', side='left', range=[0, 100]),
            yaxis2=dict(title='Volume', side='right', overlaying='y'),
            xaxis_title=f'{aggregation} Period',
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Average Win Rate", f"{trend_df['win_rate'].mean():.1f}%")
        with col2:
            st.metric("Min Win Rate", f"{trend_df['win_rate'].min():.1f}%")
        with col3:
            st.metric("Max Win Rate", f"{trend_df['win_rate'].max():.1f}%")
    
    st.markdown("---")
    
    # Platform Comparison Trend
    st.header("🎯 Platform Win Rate Trends")
    
    if not platform_trend_df.empty:
        fig = go.Figure()
        
        for platform in platform_trend_df['platform'].unique():
            platform_data = platform_trend_df[platform_trend_df['platform'] == platform]
            
            fig.add_trace(go.Scatter(
                x=platform_data['period'],
                y=platform_data['win_rate'],
                mode='lines+markers',
                name=platform,
                line=dict(width=2),
                hovertemplate='%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:.1f}%<extra></extra>'
            ))
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            yaxis=dict(title='Win Rate (%)', range=[0, 100]),
            xaxis_title=f'{aggregation} Period',
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Issue Type Trend
    st.header("📋 Issue Type Win Rate Trends")
    
    if not issue_trend_df.empty:
        fig = go.Figure()
        
        colors = {'Inaccurate': '#00cc96', 'Cancelled': '#ef553b', 'Other': '#636efa'}
        
        for issue in issue_trend_df['issue_type'].unique():
            issue_data = issue_trend_df[issue_trend_df['issue_type'] == issue]
            
            fig.add_trace(go.Scatter(
                x=issue_data['period'],
                y=issue_data['win_rate'],
                mode='lines+markers',
                name=issue,
                line=dict(width=2, color=colors.get(issue, '#636efa')),
                hovertemplate='%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:.1f}%<extra></extra>'
            ))
        
        # Add annotation for cancelled orders
        fig.add_annotation(
            x=0.5, y=0.1,
            xref="paper", yref="paper",
            text="⚠️ Cancelled orders consistently below 5%",
            showarrow=False,
            bgcolor="rgba(255,0,0,0.1)",
            bordercolor="red",
            borderwidth=1
        )
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            yaxis=dict(title='Win Rate (%)', range=[0, 100]),
            xaxis_title=f'{aggregation} Period',
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Category and Subcategory Analysis
    st.header("📂 Category & Subcategory Win Rates")
    
    if not category_breakdown_df.empty:
        # Create a treemap for better hierarchical visualization
        fig = px.treemap(
            category_breakdown_df,
            path=['error_category', 'error_subcategory'],
            values='total_settled',
            color='win_rate',
            color_continuous_scale='RdYlGn',
            range_color=[0, 100],
            title="Win Rate by Category and Subcategory (Size = Volume, Color = Win Rate)",
            hover_data={'accepted': ':,.0f', 'denied': ':,.0f'}
        )
        fig.update_traces(textinfo="label+value+percent parent")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # Top and Bottom performers
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Top Performing Subcategories")
            top_performers = category_breakdown_df.nlargest(10, 'win_rate')[['error_category', 'error_subcategory', 'win_rate', 'total_settled']]
            st.dataframe(
                top_performers.style.format({
                    'win_rate': '{:.1f}%',
                    'total_settled': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            st.subheader("⚠️ Bottom Performing Subcategories")
            bottom_performers = category_breakdown_df.nsmallest(10, 'win_rate')[['error_category', 'error_subcategory', 'win_rate', 'total_settled']]
            st.dataframe(
                bottom_performers.style.format({
                    'win_rate': '{:.1f}%',
                    'total_settled': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        # Category Summary
        st.subheader("📊 Category Summary")
        category_summary = category_breakdown_df.groupby('error_category').agg({
            'accepted': 'sum',
            'denied': 'sum',
            'total_settled': 'sum'
        }).reset_index()
        category_summary['win_rate'] = (category_summary['accepted'] / category_summary['total_settled'] * 100).round(1)
        
        fig = px.bar(
            category_summary,
            x='error_category',
            y='win_rate',
            color='win_rate',
            color_continuous_scale='RdYlGn',
            range_color=[0, 100],
            text='win_rate',
            title="Win Rate by Category"
        )
        fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Restaurant Chain Analysis Section
    st.header("🏪 Restaurant Chain Analysis")
    
    # Chain Win Rates Overview
    st.subheader("📊 Chain Win Rate Rankings")
    
    if not chain_df.empty:
        # Top chains by win rate
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Bar chart of chain win rates
            fig = px.bar(
                chain_df.head(20),
                x='win_rate',
                y='chain',
                orientation='h',
                color='win_rate',
                color_continuous_scale='RdYlGn',
                range_color=[0, 100],
                text='win_rate',
                title="Top 20 Chains by Win Rate",
                labels={'win_rate': 'Win Rate (%)', 'chain': 'Restaurant Chain'},
                hover_data={'accepted': ':,.0f', 'denied': ':,.0f', 'total_settled': ':,.0f'}
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Metrics for top chains
            st.metric("Top Chain Win Rate", f"{chain_df.iloc[0]['win_rate']:.1f}%", 
                     f"{chain_df.iloc[0]['chain']}")
            st.metric("Average Win Rate", f"{chain_df['win_rate'].mean():.1f}%")
            st.metric("Total Chains", f"{len(chain_df):,}")
            
            # Volume distribution
            st.subheader("📈 Volume Distribution")
            volume_stats = {
                "High Volume (>1000)": len(chain_df[chain_df['total_settled'] > 1000]),
                "Medium (100-1000)": len(chain_df[(chain_df['total_settled'] >= 100) & (chain_df['total_settled'] <= 1000)]),
                "Low (<100)": len(chain_df[chain_df['total_settled'] < 100])
            }
            for label, count in volume_stats.items():
                st.write(f"{label}: {count} chains")
    
    # Chain × Platform Matrix
    st.subheader("🔀 Chain × Platform Win Rates")
    
    if not chain_platform_matrix.empty:
        # Pivot for heatmap
        pivot_matrix = chain_platform_matrix.pivot(
            index='chain', 
            columns='platform', 
            values='win_rate'
        ).fillna(0)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot_matrix.values,
            x=pivot_matrix.columns,
            y=pivot_matrix.index,
            colorscale='RdYlGn',
            zmid=50,
            text=pivot_matrix.values,
            texttemplate='%{text:.1f}%',
            textfont={"size": 10},
            colorbar=dict(title='Win Rate %')
        ))
        
        fig.update_layout(
            height=500,
            title="Win Rate Heatmap: Top Chains × Platform",
            xaxis_title='Platform',
            yaxis_title='Restaurant Chain',
            yaxis={'categoryorder':'array', 'categoryarray':pivot_matrix.index.tolist()[::-1]}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Chain × Issue Type Analysis
    st.subheader("📋 Chain × Issue Type Breakdown")
    
    if not chain_issue_breakdown.empty:
        # Select top chains for detailed view
        top_chains = chain_issue_breakdown.groupby('chain')['total_settled'].sum().nlargest(10).index.tolist()
        filtered_breakdown = chain_issue_breakdown[chain_issue_breakdown['chain'].isin(top_chains)]
        
        # Stacked bar chart
        fig = px.bar(
            filtered_breakdown,
            x='chain',
            y='total_settled',
            color='issue_type',
            title="Issue Type Distribution by Chain (Top 10)",
            text='win_rate',
            hover_data={'accepted': ':,.0f', 'denied': ':,.0f', 'win_rate': ':.1f'}
        )
        fig.update_traces(texttemplate='%{text:.0f}%', textposition='inside')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        with st.expander("📊 View Detailed Chain Performance"):
            detailed_df = chain_issue_breakdown.pivot_table(
                index='chain',
                columns='issue_type', 
                values='win_rate',
                aggfunc='mean'
            ).round(1)
            
            # Add overall win rate
            overall_by_chain = chain_issue_breakdown.groupby('chain').agg({
                'accepted': 'sum',
                'denied': 'sum',
                'total_settled': 'sum'
            })
            overall_by_chain['Overall'] = (overall_by_chain['accepted'] / overall_by_chain['total_settled'] * 100).round(1)
            detailed_df['Overall'] = overall_by_chain['Overall']
            
            # Sort by overall win rate
            detailed_df = detailed_df.sort_values('Overall', ascending=False).head(20)
            
            st.dataframe(
                detailed_df.style.format('{:.1f}%'),
                use_container_width=True
            )
    
    # Chain Trend Analysis
    st.subheader("📈 Chain Performance Trends")
    
    # Allow user to select chains for comparison
    if not chain_df.empty:
        selected_chains = st.multiselect(
            "Select chains to compare trends:",
            options=chain_df.head(30)['chain'].tolist(),
            default=chain_df.head(5)['chain'].tolist(),
            max_selections=10
        )
        
        if selected_chains:
            chain_trend_df = get_chain_trend(start_date, end_date, selected_chains, aggregation)
            
            if not chain_trend_df.empty:
                # Line chart for win rate trends
                fig = px.line(
                    chain_trend_df,
                    x='period',
                    y='win_rate',
                    color='chain',
                    markers=True,
                    title=f"Win Rate Trends by Chain ({aggregation})",
                    labels={'win_rate': 'Win Rate (%)', 'period': 'Period'},
                    hover_data={'accepted': ':,.0f', 'denied': ':,.0f', 'total_settled': ':,.0f'}
                )
                fig.update_layout(height=500, hovermode='x unified')
                st.plotly_chart(fig, use_container_width=True)
                
                # Volume trends
                fig = px.area(
                    chain_trend_df,
                    x='period',
                    y='total_settled',
                    color='chain',
                    title=f"Dispute Volume Trends by Chain ({aggregation})",
                    labels={'total_settled': 'Dispute Volume', 'period': 'Period'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    # Average Recovery Per Location
    st.subheader("💰 Average Recovery Per Location (Month-over-Month)")
    
    if not chain_recovery_df.empty:
        # Line chart showing avg recovery per location over time
        fig = px.line(
            chain_recovery_df,
            x='month',
            y='avg_per_location',
            color='chain',
            markers=True,
            title="Average Recovery Per Location by Chain (Monthly)",
            labels={
                'avg_per_location': 'Avg Recovery per Location ($)',
                'month': 'Month',
                'chain': 'Restaurant Chain'
            },
            hover_data={
                'active_locations': ':,.0f',
                'total_recovered': ':,.2f'
            }
        )
        fig.update_layout(
            height=500,
            hovermode='x unified',
            yaxis_tickformat='$,.0f'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics in columns
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart of latest month's performance
            latest_month = chain_recovery_df['month'].max()
            latest_data = chain_recovery_df[chain_recovery_df['month'] == latest_month].sort_values('avg_per_location', ascending=False)
            
            if not latest_data.empty:
                fig = px.bar(
                    latest_data,
                    x='avg_per_location',
                    y='chain',
                    orientation='h',
                    title=f"Latest Month Recovery per Location ({latest_month.strftime('%B %Y')})",
                    labels={'avg_per_location': 'Avg Recovery ($)', 'chain': 'Chain'},
                    text='avg_per_location',
                    hover_data={'active_locations': ':,.0f', 'total_recovered': ':,.2f'}
                )
                fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Table showing trend summary
            st.write("**Monthly Trend Summary**")
            
            # Calculate month-over-month growth
            pivot_recovery = chain_recovery_df.pivot(index='month', columns='chain', values='avg_per_location')
            growth_rates = pivot_recovery.pct_change().mean() * 100
            
            summary_stats = chain_recovery_df.groupby('chain').agg({
                'avg_per_location': ['mean', 'max', 'min'],
                'active_locations': 'mean'
            }).round(2)
            
            summary_stats.columns = ['Avg Recovery', 'Max Recovery', 'Min Recovery', 'Avg Locations']
            summary_stats['Avg Growth %'] = growth_rates.round(1)
            
            st.dataframe(
                summary_stats.style.format({
                    'Avg Recovery': '${:,.0f}',
                    'Max Recovery': '${:,.0f}',
                    'Min Recovery': '${:,.0f}',
                    'Avg Locations': '{:,.0f}',
                    'Avg Growth %': '{:.1f}%'
                }),
                use_container_width=True
            )
        
        # Heatmap showing recovery trends
        st.write("**Recovery Heatmap ($ per Location)**")
        
        pivot_for_heatmap = chain_recovery_df.pivot(
            index='chain',
            columns='month',
            values='avg_per_location'
        )
        
        # Format column names to show month-year
        pivot_for_heatmap.columns = [col.strftime('%b %Y') for col in pivot_for_heatmap.columns]
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_for_heatmap.values,
            x=pivot_for_heatmap.columns,
            y=pivot_for_heatmap.index,
            colorscale='Viridis',
            text=pivot_for_heatmap.values,
            texttemplate='$%{text:,.0f}',
            textfont={"size": 10},
            colorbar=dict(title='Avg Recovery ($)')
        ))
        
        fig.update_layout(
            height=400,
            title="Monthly Recovery per Location Heatmap",
            xaxis_title='Month',
            yaxis_title='Restaurant Chain',
            yaxis={'categoryorder':'array', 'categoryarray':pivot_for_heatmap.index.tolist()[::-1]}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Data Table
    with st.expander("📊 View Raw Data"):
        st.subheader("Win Rate Trend Data")
        if not trend_df.empty:
            display_df = trend_df.copy()
            display_df['period'] = display_df['period'].dt.strftime('%Y-%m-%d')
            st.dataframe(
                display_df.style.format({
                    'accepted': '{:,.0f}',
                    'denied': '{:,.0f}',
                    'total_settled': '{:,.0f}',
                    'win_rate': '{:.1f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"win_rate_data_{start_date}_{end_date}.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()