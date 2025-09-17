"""
Inaccurate Orders Recovery Dashboard
Shows recovery rate for inaccurate orders only
Correct calculation: Total disputed amount vs what was won/lost/pending
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date, timedelta
import os
from google.oauth2 import service_account
import pandas_gbq

# Page configuration
st.set_page_config(
    page_title="Inaccurate Orders Recovery",
    page_icon="📊",
    layout="wide"
)

# Authentication
if 'gcp_service_account' in st.secrets:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
else:
    os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
    credentials = None

# Configuration
PROJECT_ID = 'arboreal-vision-339901'

# Title
st.title("📊 Inaccurate Orders Recovery Dashboard")
st.caption("Tracking recovery rate for inaccurate orders only")

# Clear explanation
with st.container():
    st.info("""
    📊 **Inaccurate Orders Win Rate**
    
    **Win Rate = ($ Won / $ Settled) × 100**
    
    - **Won**: Amount recovered from ALL categories (enabled_won_disputes)
    - **Settled**: Amount resolved from INACCURATE orders only (ACCEPTED + DENIED)
    - **Lost**: Settled - Won (portion not recovered)
    - **Pending**: Amount still being processed (INACCURATE only)
    
    **Note**: Win Rate uses Won from all categories but Settled from inaccurate orders only
    """)

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Date range filter
default_start = date(2025, 1, 1)
default_end = date.today()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(default_start, default_end),
    min_value=date(2024, 1, 1),
    max_value=date.today(),
    key="date_range"
)

@st.cache_data(ttl=3600)
def get_platforms():
    """Get list of unique platforms"""
    query = """
    SELECT DISTINCT platform
    FROM `merchant_portal_export.chargeback_split_summary`
    WHERE platform IS NOT NULL
        AND UPPER(COALESCE(error_category, '')) LIKE '%INACCURATE%'
    ORDER BY platform
    """
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df['platform'].tolist()
    except:
        return []

@st.cache_data(ttl=3600)
def get_chains():
    """Get list of restaurant chains"""
    query = """
    SELECT DISTINCT sm.chain
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm
        ON cs.slug = sm.slug
    WHERE sm.chain IS NOT NULL
        AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
    ORDER BY sm.chain
    """
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df['chain'].tolist()
    except:
        return []

# Load filter options and create filters
with st.spinner("Loading filter options..."):
    platforms = get_platforms()
    chains = get_chains()

# Platform filter
platform_filter = st.sidebar.multiselect(
    "Platform",
    options=["All Platforms"] + platforms,
    default=["All Platforms"]
)

# Chain filter
chain_filter = st.sidebar.multiselect(
    "Restaurant Chain",
    options=["All Chains"] + chains,
    default=["All Chains"]
)

def build_filter_clause(date_range, platform_filter, chain_filter, table_alias='cs', include_chain=False):
    """Build WHERE clause for queries - ONLY INACCURATE ORDERS"""
    filters = []
    
    # ALWAYS filter for inaccurate orders
    filters.append(f"UPPER(COALESCE({table_alias}.error_category, '')) LIKE '%INACCURATE%'")
    
    # Date filter
    if date_range and len(date_range) == 2:
        filters.append(f"{table_alias}.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    # Platform filter
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        filters.append(f"{table_alias}.platform IN ('{platform_list}')")
    
    # Chain filter (only include if we're joining with the chain table)
    if include_chain and chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        filters.append(f"sm.chain IN ('{chain_list}')")
    
    return " AND ".join(filters)

@st.cache_data(ttl=3600)
def get_overall_recovery(date_range, platform_filter, chain_filter):
    """Get overall recovery metrics for inaccurate orders using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filters that apply to both queries
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and platform_filter != ["All Platforms"] and platform_filter != "All Platforms":
        if isinstance(platform_filter, list):
            platform_list = "', '".join(platform_filter)
        else:
            platform_list = platform_filter
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    # Handle chain filter with join
    chain_join = ""
    chain_where = ""
    if chain_filter and chain_filter != ["All Chains"] and chain_filter != "All Chains":
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        if isinstance(chain_filter, list):
            chain_list = "', '".join(chain_filter)
        else:
            chain_list = chain_filter
        chain_where = f" AND sm.chain IN ('{chain_list}')"
    
    base_where = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH won_all AS (
        -- Calculate Won from ALL error categories
        SELECT 
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won_all
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where}{chain_where}
    ),
    inaccurate_metrics AS (
        -- Calculate metrics from INACCURATE orders only
        SELECT 
            -- Total disputed amount (inaccurate only)
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'TO_BE_RAISED') 
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                WHEN cs.external_status IN ('IN_PROGRESS', 'DENIED') AND cs.loop_raised = true
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_contested,
            
            -- Calculate total settled (INACCURATE only)
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_settled,
            
            -- Amount pending (INACCURATE only)
            SUM(CASE 
                WHEN external_status = 'TO_BE_RAISED'
                THEN COALESCE(enabled_customer_refunds, 0)
                WHEN external_status = 'IN_PROGRESS' AND loop_raised = true
                THEN COALESCE(enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_pending,
            
            COUNT(*) as total_disputes,
            
            -- Count by status
            SUM(CASE WHEN external_status = 'ACCEPTED' THEN 1 ELSE 0 END) as accepted_count,
            SUM(CASE WHEN external_status = 'DENIED' AND loop_raised = true THEN 1 ELSE 0 END) as denied_count,
            SUM(CASE 
                WHEN external_status = 'TO_BE_RAISED' THEN 1
                WHEN external_status = 'IN_PROGRESS' AND loop_raised = true THEN 1
                ELSE 0 
            END) as pending_count
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where} 
            AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'{chain_where}
    )
    SELECT 
        total_contested,
        total_won_all as total_won,
        total_settled,
        0 as total_lost,
        total_pending,
        total_disputes,
        accepted_count,
        denied_count,
        pending_count
    FROM won_all, inaccurate_metrics
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        if not df.empty:
            row = df.iloc[0]
            # Calculate lost as Settled - Won
            df['total_lost'] = df['total_settled'] - df['total_won']
            # Calculate win rate (Won / Settled)
            if row['total_settled'] > 0:
                df['win_rate'] = round(100.0 * row['total_won'] / row['total_settled'], 2)
            else:
                df['win_rate'] = 0
        return df
    except Exception as e:
        st.error(f"Error loading overall recovery: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_monthly_recovery(date_range, platform_filter, chain_filter):
    """Get monthly recovery trends using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filters that apply to both queries
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and platform_filter != ["All Platforms"] and platform_filter != "All Platforms":
        if isinstance(platform_filter, list):
            platform_list = "', '".join(platform_filter)
        else:
            platform_list = platform_filter
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    base_where = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH monthly_data AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            
            -- Won from ALL categories
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as won_all,
            
            -- Settled, pending, contested from INACCURATE only
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as settled,
            
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                    AND cs.external_status = 'TO_BE_RAISED'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                    AND cs.external_status = 'IN_PROGRESS' AND cs.loop_raised = true
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as pending,
            
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                    AND cs.external_status IN ('ACCEPTED', 'TO_BE_RAISED') 
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                    AND cs.external_status IN ('IN_PROGRESS', 'DENIED') AND cs.loop_raised = true
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_contested,
            
            COUNT(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                THEN 1 ELSE NULL 
            END) as dispute_count
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        WHERE {base_where}
        GROUP BY month
    )
    SELECT 
        month,
        won_all as won,
        settled,
        pending,
        total_contested,
        dispute_count
    FROM monthly_data
    ORDER BY month
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        df['month'] = pd.to_datetime(df['month'])
        df['month_name'] = df['month'].dt.strftime('%B %Y')
        
        # Calculate lost as Settled - Won
        df['lost'] = df['settled'] - df['won']
        # Calculate win rate for each month (Won / Settled)
        df['win_rate'] = df.apply(
            lambda x: round(100.0 * x['won'] / x['settled'], 2) if x['settled'] > 0 else 0,
            axis=1
        )
        
        return df
    except Exception as e:
        st.error(f"Error loading monthly data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_platform_recovery(date_range, platform_filter, chain_filter):
    """Get recovery by platform using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filters that apply to both queries
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and platform_filter != ["All Platforms"] and platform_filter != "All Platforms":
        if isinstance(platform_filter, list):
            platform_list = "', '".join(platform_filter)
        else:
            platform_list = platform_filter
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    base_where = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH platform_data AS (
        SELECT 
            cs.platform,
            
            -- Won from ALL categories
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as won_all,
            
            -- Total contested, settled, pending from INACCURATE only
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_contested,
            
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as settled,
            
            SUM(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status NOT IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.customer_refunds, 0)
                ELSE 0 
            END) as pending,
            
            COUNT(CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' 
                THEN 1 ELSE NULL 
            END) as dispute_count
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        WHERE {base_where}
        GROUP BY cs.platform
    )
    SELECT 
        platform,
        total_contested,
        won_all as won,
        settled,
        pending,
        dispute_count
    FROM platform_data
    ORDER BY total_contested DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        
        # Calculate lost as Settled - Won
        df['lost'] = df['settled'] - df['won']
        # Calculate win rate (Won / Settled)
        df['win_rate'] = df.apply(
            lambda x: round(100.0 * x['won'] / x['settled'], 2) if x['settled'] > 0 else 0,
            axis=1
        )
        
        return df
    except Exception as e:
        st.error(f"Error loading platform data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_recovery(date_range, platform_filter, chain_filter):
    """Get recovery by restaurant chain for inaccurate orders"""
    filter_clause = build_filter_clause(date_range, platform_filter, chain_filter, 'cs', include_chain=True)
    
    # Build filter for Won from ALL categories
    won_filters = []
    if date_range and len(date_range) == 2:
        won_filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        won_filters.append(f"cs.platform IN ('{platform_list}')")
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        won_filters.append(f"sm.chain IN ('{chain_list}')")
    
    won_filter = " AND ".join(won_filters) if won_filters else ""
    
    query = f"""
    WITH won_by_chain AS (
        -- Calculate Won from ALL categories by chain
        SELECT 
            sm.chain,
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm
            ON cs.slug = sm.slug
        WHERE sm.chain IS NOT NULL
            {f"AND {won_filter}" if won_filter else ""}
        GROUP BY sm.chain
    ),
    inaccurate_metrics AS (
        -- Calculate metrics from INACCURATE only
        SELECT 
            sm.chain,
            
            -- Total disputed amount (inaccurate only)
            SUM(COALESCE(cs.enabled_customer_refunds, 0)) as total_contested,
            
            -- Amount settled (inaccurate only)
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as settled,
            
            -- Amount pending (inaccurate only)
            SUM(CASE 
                WHEN cs.external_status NOT IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as pending,
            
            COUNT(*) as dispute_count,
            COUNT(DISTINCT cs.slug) as location_count
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm
            ON cs.slug = sm.slug
        WHERE {filter_clause}
            AND sm.chain IS NOT NULL
        GROUP BY sm.chain
        HAVING dispute_count > 10
    )
    SELECT 
        i.chain,
        i.total_contested,
        COALESCE(w.total_won, 0) as won,
        i.settled,
        i.pending,
        i.dispute_count,
        i.location_count
    FROM inaccurate_metrics i
    LEFT JOIN won_by_chain w ON i.chain = w.chain
    ORDER BY i.total_contested DESC
    LIMIT 20
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        
        # Calculate lost as Settled - Won
        df['lost'] = df['settled'] - df['won']
        # Calculate win rate (Won / Settled)
        df['win_rate'] = df.apply(
            lambda x: round(100.0 * x['won'] / x['settled'], 2) if x['settled'] > 0 else 0,
            axis=1
        )
        
        return df
    except Exception as e:
        st.error(f"Error loading chain data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_platform_win_rate_trend(date_range, platform_filter=None, chain_filter=None):
    """Get win rate trend by platform over time"""
    
    # Build date filter
    date_clause = ""
    if date_range and len(date_range) == 2:
        date_clause = f"AND cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
    
    # Build platform filter
    platform_clause = ""
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        platform_clause = f"AND cs.platform IN ('{platform_list}')"
    
    # Build chain filter
    chain_clause = ""
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        chain_clause = f"AND sm.chain IN ('{chain_list}')"
    
    query = f"""
    WITH monthly_platform_rates AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            cs.platform,
            -- Won from ALL categories
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won,
            -- Settled from INACCURATE only
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_settled
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE cs.platform IS NOT NULL
            {date_clause}
            {platform_clause}
            {chain_clause}
        GROUP BY month, cs.platform
        HAVING total_settled > 0  -- Only include months with settled amounts
    )
    SELECT 
        month,
        platform,
        total_won,
        total_settled,
        ROUND(CASE 
            WHEN total_settled > 0 
            THEN (total_won / total_settled) * 100
            ELSE 0 
        END, 2) as win_rate
    FROM monthly_platform_rates
    ORDER BY month, platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        if not df.empty:
            df['month'] = pd.to_datetime(df['month'])
            df['month_str'] = df['month'].dt.strftime('%b %Y')
        return df
    except Exception as e:
        st.error(f"Error loading platform win rate trend: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_subcategory_recovery(date_range, platform_filter, chain_filter):
    """Get recovery rate by subcategory using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filters manually to avoid parsing issues
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    # Handle chain filter with join
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        chain_list = "', '".join(chain_filter)
        filters.append(f"sm.chain IN ('{chain_list}')")
    
    base_where = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH subcategory_data AS (
        SELECT 
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.error_subcategory, 'Unspecified')
                ELSE NULL
            END as subcategory,
            
            -- Won from ALL categories
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            
            -- Settled from INACCURATE only
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END as settled_amount,
            
            cs.slug,
            UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' as is_inaccurate
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where}
    )
    SELECT 
        subcategory,
        SUM(won_amount) as won,
        SUM(settled_amount) as settled,
        ROUND(100.0 * SUM(won_amount) / NULLIF(SUM(settled_amount), 0), 1) as win_rate,
        COUNT(DISTINCT CASE WHEN is_inaccurate THEN slug ELSE NULL END) as location_count,
        SUM(CASE WHEN is_inaccurate THEN 1 ELSE 0 END) as dispute_count
    FROM subcategory_data
    WHERE subcategory IS NOT NULL
    GROUP BY subcategory
    HAVING settled > 0
    ORDER BY settled DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading subcategory recovery: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_subcategory_recovery_monthly(date_range, platform_filter, chain_filter):
    """Get monthly recovery rate by subcategory using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filters manually to avoid parsing issues
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    # Handle chain filter with join
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        chain_list = "', '".join(chain_filter)
        filters.append(f"sm.chain IN ('{chain_list}')")
    
    base_where = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH monthly_subcategory_data AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.error_subcategory, 'Unspecified')
                ELSE NULL
            END as subcategory,
            
            -- Won from ALL categories
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            
            -- Settled from INACCURATE only
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END as settled_amount,
            
            UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%' as is_inaccurate
            
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where}
    )
    SELECT 
        month,
        subcategory,
        SUM(won_amount) as won,
        SUM(settled_amount) as settled,
        ROUND(100.0 * SUM(won_amount) / NULLIF(SUM(settled_amount), 0), 1) as win_rate,
        SUM(CASE WHEN is_inaccurate THEN 1 ELSE 0 END) as dispute_count
    FROM monthly_subcategory_data
    WHERE subcategory IS NOT NULL
    GROUP BY month, subcategory
    HAVING settled > 0
    ORDER BY subcategory, month ASC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading monthly subcategory recovery: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_subcategory_volume_monthly(date_range, platform_filter, chain_filter):
    """Get monthly dispute volume percentage by subcategory for inaccurate orders"""
    
    # Build filters manually to avoid parsing issues
    filters = []
    
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = [f"'{p}'" for p in platform_filter]
        filters.append(f"cs.platform IN ({', '.join(platform_list)})")
    
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = [f"'{c}'" for c in chain_filter]
        filters.append(f"sm.chain IN ({', '.join(chain_list)})")
    
    # Add date range filter
    start_date, end_date = date_range
    filters.append(f"cs.chargeback_date >= '{start_date}'")
    filters.append(f"cs.chargeback_date <= '{end_date}'")
    
    # Add inaccurate order filter
    filters.append("UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'")
    
    where_clause = " AND ".join(filters)
    
    # Chain join if needed
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_join = "LEFT JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
    
    query = f"""
    WITH monthly_totals AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            COUNT(*) as total_disputes_month
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {where_clause}
        GROUP BY month
    ),
    subcategory_monthly AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            TRIM(UPPER(cs.error_subcategory)) as subcategory,
            COUNT(*) as disputes_count
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {where_clause}
            AND cs.error_subcategory IS NOT NULL
            AND TRIM(cs.error_subcategory) != ''
        GROUP BY month, subcategory
    )
    SELECT 
        sm.month,
        sm.subcategory,
        sm.disputes_count,
        mt.total_disputes_month,
        ROUND(100.0 * sm.disputes_count / mt.total_disputes_month, 2) as percentage
    FROM subcategory_monthly sm
    JOIN monthly_totals mt ON sm.month = mt.month
    ORDER BY sm.subcategory, sm.month ASC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading monthly subcategory volume: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_status_breakdown(date_range, platform_filter, chain_filter):
    """Get detailed status breakdown for inaccurate orders"""
    filter_clause = build_filter_clause(date_range, platform_filter, chain_filter, 'cs', include_chain=False)
    
    query = f"""
    SELECT 
        external_status,
        
        -- Total disputed amount for this status (use enabled_customer_refunds when applicable)
        SUM(CASE 
            WHEN external_status IN ('ACCEPTED', 'TO_BE_RAISED') 
            THEN COALESCE(enabled_customer_refunds, 0)
            WHEN external_status IN ('IN_PROGRESS', 'DENIED') AND loop_raised = true
            THEN COALESCE(enabled_customer_refunds, 0)
            ELSE COALESCE(customer_refunds, 0)
        END) as amount,
        
        COUNT(*) as count
        
    FROM `merchant_portal_export.chargeback_split_summary` cs
    WHERE {filter_clause}
    GROUP BY external_status
    ORDER BY amount DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading status breakdown: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_cohort_analysis(date_range, platform_filter=None, chain_filter=None):
    """Get cohort analysis - recovery per location for top 20 chains over last 10 months"""
    
    # Use provided date range or default to last 10 months
    if date_range and len(date_range) == 2:
        start_date = date_range[0]
        end_date = date_range[1]
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=300)  # Approximately 10 months
    
    # Build platform filter
    platform_clause = ""
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        platform_clause = f"AND cs.platform IN ('{platform_list}')"
    
    # Build chain filter  
    chain_clause = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        chain_clause = f"AND sm.chain IN ('{chain_list}')"
    
    query = f"""
    WITH monthly_chain_data AS (
        -- Get Won from ALL categories and location count by chain and month
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            sm.chain,
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won,
            COUNT(DISTINCT cs.slug) as active_locations
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm
            ON cs.slug = sm.slug
        WHERE cs.chargeback_date >= '{start_date}'
            AND cs.chargeback_date <= '{end_date}'
            AND sm.chain IS NOT NULL
            {platform_clause}
            {chain_clause}
        GROUP BY month, sm.chain
    ),
    top_chains AS (
        -- Get top 20 chains by total volume
        SELECT 
            chain,
            SUM(total_won) as total_volume
        FROM monthly_chain_data
        GROUP BY chain
        ORDER BY total_volume DESC
        LIMIT 20
    )
    SELECT 
        m.chain,
        m.month,
        m.total_won,
        m.active_locations,
        ROUND(CASE 
            WHEN m.active_locations > 0 
            THEN m.total_won / m.active_locations 
            ELSE 0 
        END, 2) as recovery_per_location
    FROM monthly_chain_data m
    INNER JOIN top_chains t ON m.chain = t.chain
    ORDER BY t.total_volume DESC, m.chain, m.month
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        
        if not df.empty:
            # Pivot the data to create cohort matrix
            df['month'] = pd.to_datetime(df['month'])
            df['month_label'] = df['month'].dt.strftime('%b %Y')
            
            # Create pivot table with recovery per location
            cohort_matrix = df.pivot_table(
                index='chain',
                columns='month_label',
                values='recovery_per_location',
                aggfunc='first'
            )
            
            # Sort columns by date
            date_order = sorted(df['month'].unique())
            month_labels = [pd.to_datetime(d).strftime('%b %Y') for d in date_order]
            cohort_matrix = cohort_matrix[month_labels]
            
            return cohort_matrix
        
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Error loading cohort analysis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_ontime_dispute_analysis(date_range, platform_filter, chain_filter):
    """Analyze dispute processing time by looking at status transitions"""
    
    # Build filters
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    # Handle chain filter with join
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        chain_list = "', '".join(chain_filter)
        filters.append(f"sm.chain IN ('{chain_list}')")
    
    # Add inaccurate filter
    filters.append("UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'")
    
    where_clause = " AND ".join(filters) if filters else "1=1"
    
    # Analyze based on external_status to understand dispute timing
    query = f"""
    WITH dispute_analysis AS (
        SELECT 
            cs.platform,
            -- Define dispute windows by platform
            CASE 
                WHEN cs.platform = 'Doordash' THEN 14
                WHEN cs.platform = 'Grubhub' THEN 30
                WHEN cs.platform = 'UberEats' THEN 30
                ELSE 30  -- Default
            END as dispute_window,
            cs.external_status,
            COUNT(*) as dispute_count,
            SUM(COALESCE(cs.enabled_customer_refunds, 0)) as total_amount
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {where_clause}
        GROUP BY cs.platform, dispute_window, cs.external_status
    ),
    platform_summary AS (
        SELECT 
            platform,
            dispute_window,
            -- Total disputes (all statuses)
            SUM(dispute_count) as total_count,
            -- Disputes that were filed (ACCEPTED, DENIED, or IN_PROGRESS)
            SUM(CASE 
                WHEN external_status IN ('ACCEPTED', 'DENIED', 'IN_PROGRESS') THEN dispute_count 
                ELSE 0 
            END) as filed_count,
            -- Disputes not filed or expired (TO_BE_RAISED or EXPIRED)
            SUM(CASE 
                WHEN external_status IN ('TO_BE_RAISED', 'EXPIRED') THEN dispute_count 
                ELSE 0 
            END) as not_filed_count,
            SUM(total_amount) as total_amount
        FROM dispute_analysis
        GROUP BY platform, dispute_window
    )
    SELECT 
        platform,
        dispute_window,
        total_count,
        filed_count as on_time_count,
        not_filed_count as late_count,
        ROUND(100.0 * filed_count / NULLIF(filed_count + not_filed_count, 0), 1) as on_time_percentage,
        total_amount
    FROM platform_summary
    ORDER BY platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading on-time dispute analysis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_expiry_analysis(date_range, platform_filter, chain_filter):
    """Analyze orders disputed before vs after expiry date"""
    
    # Build filters
    filters = []
    
    if date_range and len(date_range) == 2:
        filters.append(f"cs.chargeback_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
    
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        filters.append(f"cs.platform IN ('{platform_list}')")
    
    # Handle chain filter with join
    chain_join = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_join = "JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        chain_list = "', '".join(chain_filter)
        filters.append(f"sm.chain IN ('{chain_list}')")
    
    # Add inaccurate filter
    filters.append("UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'")
    
    where_clause = " AND ".join(filters) if filters else "1=1"
    
    query = f"""
    WITH expiry_data AS (
        SELECT 
            cs.chargeback_date,
            cs.order_placed_at,
            cs.platform,
            cs.enabled_customer_refunds as dispute_amount,
            -- Calculate dispute expiry based on platform and chain
            CASE 
                WHEN cs.platform = 'Doordash' THEN DATE_ADD(DATE(cs.order_placed_at), INTERVAL 14 DAY)
                WHEN cs.platform = 'Grubhub' THEN DATE_ADD(DATE(cs.order_placed_at), INTERVAL 30 DAY)
                WHEN cs.platform = 'UberEats' AND {f"sm.chain = 'mcdonalds'" if chain_join else "FALSE"} THEN DATE_ADD(DATE(cs.order_placed_at), INTERVAL 14 DAY)
                WHEN cs.platform = 'UberEats' THEN DATE_ADD(DATE(cs.order_placed_at), INTERVAL 30 DAY)
                ELSE DATE_ADD(DATE(cs.order_placed_at), INTERVAL 30 DAY)  -- Default to 30 days
            END as calculated_expiry_date
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {where_clause}
            AND cs.order_placed_at IS NOT NULL
            AND cs.chargeback_date IS NOT NULL
            AND cs.platform IS NOT NULL
    )
    SELECT 
        CASE 
            WHEN chargeback_date <= calculated_expiry_date THEN 'Before Expiry'
            WHEN chargeback_date > calculated_expiry_date THEN 'After Expiry'
            ELSE 'Unknown'
        END as timing,
        COUNT(*) as dispute_count,
        SUM(COALESCE(dispute_amount, 0)) as total_amount
    FROM expiry_data
    GROUP BY timing
    HAVING timing != 'Unknown'
    ORDER BY timing DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading expiry analysis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_chains_requiring_attention(date_range, platform_filter=None, chain_filter=None):
    """Identify chains where 3-month win rate is 20% below 12-month average"""
    
    # Calculate dates for 12-month and 3-month periods
    today = pd.to_datetime('today')
    start_3m = today - pd.Timedelta(days=90)
    start_12m = today - pd.Timedelta(days=365)
    
    # Build platform filter
    platform_clause = ""
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        platform_clause = f"AND cs.platform IN ('{platform_list}')"
    else:
        # Default to Doordash and UberEats if no filter
        platform_clause = "AND cs.platform IN ('Doordash', 'UberEats')"
    
    # Build chain filter
    chain_clause = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        chain_clause = f"AND sm.chain IN ('{chain_list}')"
    
    query = f"""
    WITH monthly_performance AS (
        SELECT 
            sm.chain,
            cs.platform,
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as won,
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as settled
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date >= '{start_12m.strftime('%Y-%m-%d')}'
            AND cs.chargeback_date <= CURRENT_DATE()
            {platform_clause}
            {chain_clause}
            AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
        GROUP BY sm.chain, cs.platform, month
        HAVING settled > 0
    ),
    chain_metrics AS (
        SELECT 
            chain,
            platform,
            -- 12-month average (or since data available)
            AVG(CASE 
                WHEN month >= '{start_12m.strftime('%Y-%m-%d')}' 
                THEN 100.0 * won / NULLIF(settled, 0)
                ELSE NULL 
            END) as avg_12m_win_rate,
            -- 3-month average
            AVG(CASE 
                WHEN month >= '{start_3m.strftime('%Y-%m-%d')}' 
                THEN 100.0 * won / NULLIF(settled, 0)
                ELSE NULL 
            END) as avg_3m_win_rate,
            -- Count months of data
            COUNT(DISTINCT month) as months_of_data,
            -- Total volume
            SUM(settled) as total_settled_12m
        FROM monthly_performance
        GROUP BY chain, platform
        HAVING months_of_data >= 3  -- Need at least 3 months of data
    ),
    flagged_chains AS (
        SELECT 
            chain,
            platform,
            avg_12m_win_rate,
            avg_3m_win_rate,
            months_of_data,
            total_settled_12m,
            -- Calculate decline percentage
            (avg_12m_win_rate - avg_3m_win_rate) as win_rate_decline,
            100.0 * (avg_12m_win_rate - avg_3m_win_rate) / NULLIF(avg_12m_win_rate, 0) as decline_percentage
        FROM chain_metrics
        WHERE avg_12m_win_rate > 0  -- Avoid division by zero
            AND avg_3m_win_rate > 0
            AND total_settled_12m > 1000  -- Focus on chains with meaningful volume
    )
    SELECT 
        chain,
        platform,
        ROUND(avg_12m_win_rate, 2) as avg_12m_win_rate,
        ROUND(avg_3m_win_rate, 2) as avg_3m_win_rate,
        ROUND(win_rate_decline, 2) as win_rate_decline,
        ROUND(decline_percentage, 2) as decline_percentage,
        months_of_data,
        ROUND(total_settled_12m, 2) as total_settled_12m
    FROM flagged_chains
    WHERE decline_percentage >= 20  -- Flag chains with 20% or more decline
    ORDER BY decline_percentage DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading attention-required chains: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_win_rate_cohort(date_range, platform_filter=None, chain_filter=None):
    """Get win rate cohort analysis for top 10 chains over last 10 months"""
    
    # Use provided date range or default to last 10 months
    if date_range and len(date_range) == 2:
        start_date = date_range[0]
        end_date = date_range[1]
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=300)  # Approximately 10 months
    
    # Build platform filter
    platform_clause = ""
    if platform_filter and "All Platforms" not in platform_filter:
        platform_list = "', '".join(platform_filter)
        platform_clause = f"AND cs.platform IN ('{platform_list}')"
    
    # Build chain filter
    chain_clause = ""
    if chain_filter and "All Chains" not in chain_filter:
        chain_list = "', '".join(chain_filter)
        chain_clause = f"AND sm.chain IN ('{chain_list}')"
    
    query = f"""
    WITH monthly_win_rates AS (
        -- Calculate win rates by chain and month
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            sm.chain,
            -- Won from ALL categories
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won,
            -- Settled from INACCURATE only
            SUM(CASE 
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0 
            END) as total_settled
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm
            ON cs.slug = sm.slug
        WHERE cs.chargeback_date >= '{start_date}'
            AND cs.chargeback_date <= '{end_date}'
            AND sm.chain IS NOT NULL
            {platform_clause}
            {chain_clause}
        GROUP BY month, sm.chain
    ),
    top_chains AS (
        -- Get top 10 chains by total settled volume
        SELECT 
            chain,
            SUM(total_settled) as total_volume
        FROM monthly_win_rates
        GROUP BY chain
        ORDER BY total_volume DESC
        LIMIT 10
    )
    SELECT 
        m.chain,
        m.month,
        m.total_won,
        m.total_settled,
        ROUND(CASE 
            WHEN m.total_settled > 0 
            THEN (m.total_won / m.total_settled) * 100
            ELSE 0 
        END, 2) as win_rate
    FROM monthly_win_rates m
    INNER JOIN top_chains t ON m.chain = t.chain
    WHERE m.total_settled > 0  -- Only show months with settled amounts
    ORDER BY t.total_volume DESC, m.chain, m.month
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        
        if not df.empty:
            # Pivot the data to create win rate matrix
            df['month'] = pd.to_datetime(df['month'])
            df['month_label'] = df['month'].dt.strftime('%b %Y')
            
            # Create pivot table with win rates
            win_rate_matrix = df.pivot_table(
                index='chain',
                columns='month_label', 
                values='win_rate',
                aggfunc='first'
            )
            
            # Sort columns by date
            date_order = sorted(df['month'].unique())
            month_labels = [pd.to_datetime(d).strftime('%b %Y') for d in date_order]
            win_rate_matrix = win_rate_matrix[month_labels]
            
            return win_rate_matrix
        
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Error loading win rate cohort: {e}")
        return pd.DataFrame()

# Get win rate by order value brackets
@st.cache_data(ttl=300)
def get_win_rate_by_order_value(date_range, platform_filter, chain_filter):
    """Analyze win rate by order value brackets using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filter conditions (no INACCURATE filter here - we need ALL orders)
    base_where = """
        cs.chargeback_date >= '{start_date}'
        AND cs.chargeback_date <= '{end_date}'
    """.format(
        start_date=date_range[0].strftime('%Y-%m-%d'),
        end_date=date_range[1].strftime('%Y-%m-%d')
    )
    
    # Add platform filter if specified
    platform_where = ""
    if platform_filter and platform_filter != ["All Platforms"] and "All Platforms" not in platform_filter:
        if isinstance(platform_filter, list):
            platform_list = "', '".join(platform_filter)
            platform_where = f" AND cs.platform IN ('{platform_list}')"
        else:
            platform_where = f" AND cs.platform = '{platform_filter}'"
    
    # Add chain filter if specified
    chain_join = ""
    chain_where = ""
    if chain_filter and chain_filter != ["All Chains"] and "All Chains" not in chain_filter:
        chain_join = " INNER JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        if isinstance(chain_filter, list):
            chain_list = "', '".join(chain_filter)
            chain_where = f" AND sm.chain IN ('{chain_list}')"
        else:
            chain_where = f" AND sm.chain = '{chain_filter}'"
    
    # Build the SELECT fields dynamically
    chain_field = "sm.chain," if chain_join else ""
    
    query = f"""
    WITH order_value_brackets AS (
        SELECT 
            cs.slug,
            cs.platform,
            {chain_field}
            cs.error_category,
            COALESCE(cs.subtotal, 0) as order_value,
            CASE
                WHEN COALESCE(cs.subtotal, 0) <= 20 THEN '$0-20'
                WHEN COALESCE(cs.subtotal, 0) <= 40 THEN '$20-40'
                WHEN COALESCE(cs.subtotal, 0) <= 60 THEN '$40-60'
                WHEN COALESCE(cs.subtotal, 0) <= 80 THEN '$60-80'
                WHEN COALESCE(cs.subtotal, 0) <= 100 THEN '$80-100'
                WHEN COALESCE(cs.subtotal, 0) <= 150 THEN '$100-150'
                WHEN COALESCE(cs.subtotal, 0) <= 200 THEN '$150-200'
                ELSE '$200+'
            END as value_bracket,
            CASE
                WHEN COALESCE(cs.subtotal, 0) <= 20 THEN 1
                WHEN COALESCE(cs.subtotal, 0) <= 40 THEN 2
                WHEN COALESCE(cs.subtotal, 0) <= 60 THEN 3
                WHEN COALESCE(cs.subtotal, 0) <= 80 THEN 4
                WHEN COALESCE(cs.subtotal, 0) <= 100 THEN 5
                WHEN COALESCE(cs.subtotal, 0) <= 150 THEN 6
                WHEN COALESCE(cs.subtotal, 0) <= 200 THEN 7
                ELSE 8
            END as bracket_order,
            cs.external_status,
            -- Won from ALL categories
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            -- Settled from INACCURATE only
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0
            END as settled_amount,
            -- Count only INACCURATE disputes
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN 1
                ELSE 0
            END as is_inaccurate_dispute
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where}{platform_where}{chain_where}
    ),
    bracket_summary AS (
        SELECT 
            value_bracket,
            bracket_order,
            SUM(is_inaccurate_dispute) as total_disputes,  -- Count INACCURATE disputes only
            SUM(settled_amount) as total_settled,  -- Settled from INACCURATE only
            SUM(won_amount) as total_won,  -- Won from ALL categories
            -- Calculate win rate
            CASE 
                WHEN SUM(settled_amount) > 0 
                THEN ROUND((SUM(won_amount) / SUM(settled_amount)) * 100, 2)
                ELSE 0
            END as win_rate,
            -- Average order value in bracket (for INACCURATE orders only)
            ROUND(AVG(CASE WHEN is_inaccurate_dispute = 1 THEN order_value ELSE NULL END), 2) as avg_order_value
        FROM order_value_brackets
        GROUP BY value_bracket, bracket_order
        HAVING SUM(settled_amount) > 0  -- Only show brackets with settled INACCURATE disputes
    )
    SELECT 
        value_bracket,
        total_disputes,
        total_settled,
        total_won,
        win_rate,
        avg_order_value
    FROM bracket_summary
    WHERE total_disputes > 0
    ORDER BY bracket_order
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading win rate by order value: {e}")
        return pd.DataFrame()

# Get win rate by order value brackets monthly trend
@st.cache_data(ttl=300)
def get_win_rate_by_order_value_monthly(date_range, platform_filter, chain_filter):
    """Analyze win rate by order value brackets with monthly breakdown using CORRECT calculation:
    Won = ALL error categories, Settled = INACCURATE only"""
    
    # Build filter conditions (no INACCURATE filter here - we need ALL orders)
    base_where = """
        cs.chargeback_date >= '{start_date}'
        AND cs.chargeback_date <= '{end_date}'
    """.format(
        start_date=date_range[0].strftime('%Y-%m-%d'),
        end_date=date_range[1].strftime('%Y-%m-%d')
    )
    
    # Add platform filter if specified
    platform_where = ""
    if platform_filter and platform_filter != ["All Platforms"] and "All Platforms" not in platform_filter:
        if isinstance(platform_filter, list):
            platform_list = "', '".join(platform_filter)
            platform_where = f" AND cs.platform IN ('{platform_list}')"
        else:
            platform_where = f" AND cs.platform = '{platform_filter}'"
    
    # Add chain filter if specified
    chain_join = ""
    chain_where = ""
    if chain_filter and chain_filter != ["All Chains"] and "All Chains" not in chain_filter:
        chain_join = " INNER JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug"
        if isinstance(chain_filter, list):
            chain_list = "', '".join(chain_filter)
            chain_where = f" AND sm.chain IN ('{chain_list}')"
        else:
            chain_where = f" AND sm.chain = '{chain_filter}'"
    
    # Build the SELECT fields dynamically  
    chain_field = "sm.chain," if chain_join else ""
    
    query = f"""
    WITH order_value_brackets AS (
        SELECT 
            DATE_TRUNC(cs.chargeback_date, MONTH) as month,
            cs.slug,
            {chain_field}
            cs.error_category,
            COALESCE(cs.subtotal, 0) as order_value,
            CASE
                WHEN COALESCE(cs.subtotal, 0) <= 20 THEN '$0-20'
                WHEN COALESCE(cs.subtotal, 0) <= 40 THEN '$20-40'
                WHEN COALESCE(cs.subtotal, 0) <= 60 THEN '$40-60'
                WHEN COALESCE(cs.subtotal, 0) <= 80 THEN '$60-80'
                WHEN COALESCE(cs.subtotal, 0) <= 100 THEN '$80-100'
                WHEN COALESCE(cs.subtotal, 0) <= 150 THEN '$100-150'
                WHEN COALESCE(cs.subtotal, 0) <= 200 THEN '$150-200'
                ELSE '$200+'
            END as value_bracket,
            CASE
                WHEN COALESCE(cs.subtotal, 0) <= 20 THEN 1
                WHEN COALESCE(cs.subtotal, 0) <= 40 THEN 2
                WHEN COALESCE(cs.subtotal, 0) <= 60 THEN 3
                WHEN COALESCE(cs.subtotal, 0) <= 80 THEN 4
                WHEN COALESCE(cs.subtotal, 0) <= 100 THEN 5
                WHEN COALESCE(cs.subtotal, 0) <= 150 THEN 6
                WHEN COALESCE(cs.subtotal, 0) <= 200 THEN 7
                ELSE 8
            END as bracket_order,
            cs.external_status,
            -- Won from ALL categories
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            -- Settled from INACCURATE only
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                    AND cs.external_status IN ('ACCEPTED', 'DENIED')
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0
            END as settled_amount,
            -- Count only INACCURATE disputes
            CASE 
                WHEN UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN 1
                ELSE 0
            END as is_inaccurate_dispute
        FROM `merchant_portal_export.chargeback_split_summary` cs
        {chain_join}
        WHERE {base_where}{platform_where}{chain_where}
    ),
    monthly_bracket_summary AS (
        SELECT 
            month,
            value_bracket,
            bracket_order,
            SUM(is_inaccurate_dispute) as total_disputes,  -- Count INACCURATE disputes only
            SUM(settled_amount) as total_settled,  -- Settled from INACCURATE only
            SUM(won_amount) as total_won,  -- Won from ALL categories
            -- Calculate win rate
            CASE 
                WHEN SUM(settled_amount) > 0 
                THEN ROUND((SUM(won_amount) / SUM(settled_amount)) * 100, 1)
                ELSE 0
            END as win_rate
        FROM order_value_brackets
        GROUP BY month, value_bracket, bracket_order
        HAVING SUM(settled_amount) > 0  -- Only show brackets with settled INACCURATE disputes
    )
    SELECT 
        month,
        value_bracket,
        bracket_order,
        win_rate,
        total_disputes,
        total_settled
    FROM monthly_bracket_summary
    ORDER BY bracket_order, month ASC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
        return df
    except Exception as e:
        st.error(f"Error loading monthly win rate by order value: {e}")
        return pd.DataFrame()

# Main dashboard
def main():
    # Load data
    with st.spinner("Loading inaccurate orders data..."):
        overall_df = get_overall_recovery(date_range, platform_filter, chain_filter)
        monthly_df = get_monthly_recovery(date_range, platform_filter, chain_filter)
        platform_df = get_platform_recovery(date_range, platform_filter, chain_filter)
        chain_df = get_chain_recovery(date_range, platform_filter, chain_filter)
        subcategory_df = get_subcategory_recovery(date_range, platform_filter, chain_filter)
        subcategory_monthly_df = get_subcategory_recovery_monthly(date_range, platform_filter, chain_filter)
        subcategory_volume_df = get_subcategory_volume_monthly(date_range, platform_filter, chain_filter)
        order_value_df = get_win_rate_by_order_value(date_range, platform_filter, chain_filter)
        order_value_monthly_df = get_win_rate_by_order_value_monthly(date_range, platform_filter, chain_filter)
        platform_trend_df = get_platform_win_rate_trend(date_range, platform_filter, chain_filter)
        cohort_df = get_cohort_analysis(date_range, platform_filter, chain_filter)
        win_rate_cohort_df = get_win_rate_cohort(date_range, platform_filter, chain_filter)
        attention_df = get_chains_requiring_attention(date_range, platform_filter, chain_filter)
    
    # Overall Metrics
    st.header("📊 Overall Recovery Metrics - Inaccurate Orders Only")
    
    if not overall_df.empty:
        row = overall_df.iloc[0]
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Win Rate",
                f"{row['win_rate']:.1f}%",
                help="Percentage of resolved disputes that were won (Won / (Won + Lost))"
            )
        
        with col2:
            st.metric(
                "Total Contested",
                f"${row['total_contested']:,.0f}",
                help="Total dollar amount disputed for inaccurate orders"
            )
        
        with col3:
            st.metric(
                "Won",
                f"${row['total_won']:,.0f}",
                f"{row['accepted_count']} disputes",
                delta_color="normal"
            )
        
        with col4:
            st.metric(
                "Lost",
                f"${row['total_lost']:,.0f}",
                f"{row['denied_count']} disputes",
                delta_color="inverse"
            )
        
        with col5:
            st.metric(
                "Pending",
                f"${row['total_pending']:,.0f}",
                f"{row['pending_count']} disputes",
                delta_color="off"
            )
    
    
    st.markdown("---")
    
    # Subcategory Recovery Analysis
    if not subcategory_df.empty:
        st.header("🎯 Recovery Rate by Subcategory")
        st.caption("Win rate breakdown by inaccurate order subcategory")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Bar chart of win rates by subcategory
            fig = px.bar(
                subcategory_df.head(15),  # Top 15 subcategories
                x='win_rate',
                y='subcategory',
                orientation='h',
                title='Win Rate by Subcategory (Top 15)',
                color='win_rate',
                color_continuous_scale='RdYlGn',
                text='win_rate'
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Win Rate (%)",
                yaxis_title="Subcategory",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Summary metrics
            st.subheader("Key Metrics")
            
            # Top performer
            best_category = subcategory_df.loc[subcategory_df['win_rate'].idxmax()]
            st.metric(
                "Best Performing",
                f"{best_category['win_rate']:.1f}%",
                help=f"{best_category['subcategory']}"
            )
            
            # Worst performer
            worst_category = subcategory_df.loc[subcategory_df['win_rate'].idxmin()]
            st.metric(
                "Needs Attention",
                f"{worst_category['win_rate']:.1f}%",
                help=f"{worst_category['subcategory']}"
            )
            
            # Volume leader
            volume_leader = subcategory_df.loc[subcategory_df['settled'].idxmax()]
            st.metric(
                "Highest Volume",
                f"${volume_leader['settled']:,.0f}",
                help=f"{volume_leader['subcategory']}"
            )
        
        # Detailed table
        with st.expander("View All Subcategories"):
            display_sub = subcategory_df.copy()
            display_sub.columns = ['Subcategory', 'Won ($)', 'Settled ($)', 'Win Rate (%)', 'Locations', 'Disputes']
            
            st.dataframe(
                display_sub.style.format({
                    'Won ($)': '${:,.0f}',
                    'Settled ($)': '${:,.0f}',
                    'Win Rate (%)': '{:.1f}%',
                    'Locations': '{:,.0f}',
                    'Disputes': '{:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        # Monthly Recovery Rate by Subcategory Table
        st.subheader("📅 Monthly Recovery Rate by Subcategory")
        
        if not subcategory_monthly_df.empty:
            # Create pivot table for monthly trends
            monthly_pivot = subcategory_monthly_df.pivot_table(
                index='subcategory',
                columns='month',
                values='win_rate',
                fill_value=0
            )
            
            # Get sorted months (chronological order - January to current month)
            sorted_months = sorted(subcategory_monthly_df['month'].unique(), reverse=False)
            month_labels = [pd.to_datetime(month).strftime('%b %Y') for month in sorted_months]
            
            # Limit to last 12 months if needed
            month_labels = month_labels[-12:] if len(month_labels) > 12 else month_labels
            sorted_months = sorted_months[-12:] if len(sorted_months) > 12 else sorted_months
            
            # Filter pivot table to only include the months we want
            monthly_pivot = monthly_pivot[sorted_months]
            monthly_pivot.columns = month_labels
            
            # Add overall average column
            monthly_pivot['Overall Avg'] = subcategory_monthly_df.groupby('subcategory')['win_rate'].mean()
            
            # Style the dataframe with background gradient
            styled_monthly = monthly_pivot.style.format('{:.1f}%').background_gradient(
                cmap='RdYlGn', 
                vmin=0, 
                vmax=100,
                subset=[col for col in monthly_pivot.columns if col != 'Overall Avg']
            ).background_gradient(
                cmap='RdYlGn', 
                vmin=0, 
                vmax=100,
                subset=['Overall Avg']
            )
            
            st.dataframe(styled_monthly, use_container_width=True)
            
            # Summary statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                best_month = monthly_pivot.drop('Overall Avg', axis=1).mean().idxmax()
                best_rate = monthly_pivot.drop('Overall Avg', axis=1).mean().max()
                st.metric("Best Month", best_month, f"{best_rate:.1f}%")
            
            with col2:
                best_subcategory = monthly_pivot['Overall Avg'].idxmax()
                best_subcategory_rate = monthly_pivot['Overall Avg'].max()
                st.metric("Best Subcategory", best_subcategory, f"{best_subcategory_rate:.1f}%")
            
            with col3:
                overall_trend = "↗️" if monthly_pivot.drop('Overall Avg', axis=1).mean().iloc[-1] > monthly_pivot.drop('Overall Avg', axis=1).mean().iloc[0] else "↘️"
                st.metric("Trend", "Overall", overall_trend)
        
        else:
            st.info("No monthly subcategory data available for the selected filters.")
        
        # Monthly Dispute Volume by Subcategory (Percentage)
        st.subheader("📊 Monthly Dispute Volume by Subcategory (%)")
        
        if not subcategory_volume_df.empty:
            # Create pivot table for volume percentages
            volume_pivot = subcategory_volume_df.pivot_table(
                index='subcategory',
                columns='month',
                values='percentage',
                fill_value=0
            )
            
            # Get sorted months (chronological order - January to current month)
            sorted_months = sorted(subcategory_volume_df['month'].unique(), reverse=False)
            month_labels = [pd.to_datetime(month).strftime('%b %Y') for month in sorted_months]
            
            # Limit to last 12 months if needed
            month_labels = month_labels[-12:] if len(month_labels) > 12 else month_labels
            sorted_months = sorted_months[-12:] if len(sorted_months) > 12 else sorted_months
            
            # Filter pivot table to only include the months we want
            volume_pivot = volume_pivot[sorted_months]
            volume_pivot.columns = month_labels
            
            # Add overall average column
            volume_pivot['Overall Avg'] = subcategory_volume_df.groupby('subcategory')['percentage'].mean()
            
            # Style the dataframe with background gradient (different color scheme for volume)
            styled_volume = volume_pivot.style.format('{:.1f}%').background_gradient(
                cmap='Blues', 
                vmin=0, 
                vmax=volume_pivot.drop('Overall Avg', axis=1).max().max(),
                subset=[col for col in volume_pivot.columns if col != 'Overall Avg']
            ).background_gradient(
                cmap='Blues', 
                vmin=0, 
                vmax=volume_pivot['Overall Avg'].max(),
                subset=['Overall Avg']
            )
            
            st.dataframe(styled_volume, use_container_width=True)
            
            # Summary statistics for volume
            col1, col2, col3 = st.columns(3)
            with col1:
                peak_month = volume_pivot.drop('Overall Avg', axis=1).mean().idxmax()
                peak_rate = volume_pivot.drop('Overall Avg', axis=1).mean().max()
                st.metric("Peak Volume Month", peak_month, f"{peak_rate:.1f}%")
            
            with col2:
                highest_volume_category = volume_pivot['Overall Avg'].idxmax()
                highest_volume_rate = volume_pivot['Overall Avg'].max()
                st.metric("Highest Volume Category", highest_volume_category, f"{highest_volume_rate:.1f}%")
            
            with col3:
                total_categories = len(volume_pivot)
                st.metric("Categories Tracked", str(total_categories), "subcategories")
        
        else:
            st.info("No monthly subcategory volume data available for the selected filters.")
    
    st.markdown("---")
    
    # Win Rate by Order Value Brackets
    if not order_value_df.empty:
        st.header("💰 Win Rate by Order Value")
        st.caption("Success rate of disputes by order value brackets")
        
        # Overall summary bar chart
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Bar chart showing win rate by order value brackets
            fig = px.bar(
                order_value_df,
                x='value_bracket',
                y='win_rate',
                title='Overall Win Rate by Order Value Brackets',
                color='win_rate',
                color_continuous_scale='RdYlGn',
                text='win_rate',
                hover_data={
                    'total_disputes': ':,.0f',
                    'total_settled': ':,.2f',
                    'avg_order_value': ':.2f'
                }
            )
            
            fig.update_traces(
                texttemplate='%{text:.1f}%',
                textposition='outside',
                hovertemplate=(
                    '<b>%{x}</b><br>' +
                    'Win Rate: %{y:.1f}%<br>' +
                    'Total Disputes: %{customdata[0]:,.0f}<br>' +
                    'Total Settled: $%{customdata[1]:,.0f}<br>' +
                    'Avg Order Value: $%{customdata[2]:.2f}<br>' +
                    '<extra></extra>'
                )
            )
            
            fig.update_layout(
                xaxis_title="Order Value Bracket",
                yaxis_title="Win Rate (%)",
                showlegend=False,
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Summary statistics
            st.subheader("Key Insights")
            
            # Best performing bracket
            best_bracket = order_value_df.loc[order_value_df['win_rate'].idxmax()]
            st.metric(
                "Best Performing",
                best_bracket['value_bracket'],
                f"{best_bracket['win_rate']:.1f}% win rate"
            )
            
            # Worst performing bracket
            worst_bracket = order_value_df.loc[order_value_df['win_rate'].idxmin()]
            st.metric(
                "Needs Attention",
                worst_bracket['value_bracket'],
                f"{worst_bracket['win_rate']:.1f}% win rate",
                delta_color="inverse"
            )
            
            # Volume distribution
            total_disputes = order_value_df['total_disputes'].sum()
            most_disputes = order_value_df.loc[order_value_df['total_disputes'].idxmax()]
            st.metric(
                "Highest Volume",
                most_disputes['value_bracket'],
                f"{(most_disputes['total_disputes']/total_disputes*100):.1f}% of disputes"
            )
        
        # Monthly trend table
        if not order_value_monthly_df.empty:
            st.subheader("📊 Monthly Win Rate Trend by Order Value")
            st.caption("Win rate percentage by order value bracket over the last 12 months")
            
            # Pivot the data to create a table with brackets as rows and months as columns
            order_value_monthly_df['month'] = pd.to_datetime(order_value_monthly_df['month'])
            order_value_monthly_df['month_label'] = order_value_monthly_df['month'].dt.strftime('%b %Y')
            
            # Create pivot table
            pivot_df = order_value_monthly_df.pivot_table(
                index='value_bracket',
                columns='month_label',
                values='win_rate',
                aggfunc='first'
            )
            
            # Sort columns by date (oldest first - chronological order)
            sorted_months = sorted(order_value_monthly_df['month'].unique(), reverse=False)
            month_labels = [pd.to_datetime(m).strftime('%b %Y') for m in sorted_months]
            
            # Limit to last 12 months (take from the end since we're in chronological order)
            month_labels = month_labels[-12:] if len(month_labels) > 12 else month_labels
            pivot_df = pivot_df[month_labels]
            
            # Sort rows by bracket order
            bracket_order = ['$0-20', '$20-40', '$40-60', '$60-80', '$80-100', '$100-150', '$150-200', '$200+']
            pivot_df = pivot_df.reindex([b for b in bracket_order if b in pivot_df.index])
            
            # Create a styled dataframe with color coding
            styled_df = pivot_df.style.format('{:.1f}%', na_rep='-')\
                .background_gradient(cmap='RdYlGn', vmin=0, vmax=100, axis=None)\
                .set_properties(**{'text-align': 'center'})
            
            st.dataframe(styled_df, use_container_width=True)
            
            # Add a heatmap visualization
            with st.expander("View as Heatmap"):
                fig_heatmap = go.Figure(data=go.Heatmap(
                    z=pivot_df.values,
                    x=pivot_df.columns,
                    y=pivot_df.index,
                    colorscale='RdYlGn',
                    text=pivot_df.values,
                    texttemplate='%{text:.1f}%',
                    textfont={"size": 10},
                    colorbar=dict(title="Win Rate (%)")
                ))
                
                fig_heatmap.update_layout(
                    title='Win Rate Heatmap by Order Value and Month',
                    xaxis_title='Month',
                    yaxis_title='Order Value Bracket',
                    height=400
                )
                
                st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Add dispute volume table as percentage
            st.subheader("📊 Monthly Dispute Distribution by Order Value")
            st.caption("Percentage of disputed orders by order value bracket for each month")
            
            # Create pivot table for dispute counts
            volume_pivot_df = order_value_monthly_df.pivot_table(
                index='value_bracket',
                columns='month_label',
                values='total_disputes',
                aggfunc='first'
            )
            
            # Use the same sorted months and bracket order
            volume_pivot_df = volume_pivot_df[month_labels]
            volume_pivot_df = volume_pivot_df.reindex([b for b in bracket_order if b in volume_pivot_df.index])
            
            # Convert to percentages - each column (month) should sum to 100%
            volume_pct_df = volume_pivot_df.div(volume_pivot_df.sum(axis=0), axis=1) * 100
            
            # Create a styled dataframe with color coding for percentages
            styled_volume_df = volume_pct_df.style.format('{:.1f}%', na_rep='-')\
                .background_gradient(cmap='Blues', axis=None)\
                .set_properties(**{'text-align': 'center'})
            
            st.dataframe(styled_volume_df, use_container_width=True)
            
            # Add a note about the percentages
            st.caption("💡 Each column sums to 100% - showing the distribution of disputes across order value brackets for that month")
            
            # Add total row at the bottom
            col1, col2, col3 = st.columns(3)
            with col1:
                total_disputes = order_value_monthly_df['total_disputes'].sum()
                st.metric("Total Disputes", f"{total_disputes:,.0f}")
            with col2:
                avg_order_value = order_value_df['avg_order_value'].mean() if not order_value_df.empty else 0
                st.metric("Avg Order Value", f"${avg_order_value:.2f}")
            with col3:
                highest_volume_bracket = order_value_df.loc[order_value_df['total_disputes'].idxmax(), 'value_bracket'] if not order_value_df.empty else "N/A"
                st.metric("Highest Volume Bracket", highest_volume_bracket)
    
    st.markdown("---")
    
    # Monthly Trend
    st.header("📈 Monthly Recovery Trend - Inaccurate Orders")
    
    if not monthly_df.empty:
        # Create dual-axis chart
        fig = go.Figure()
        
        # Stacked bar chart for amounts
        fig.add_trace(go.Bar(
            x=monthly_df['month'],
            y=monthly_df['won'],
            name='Won',
            marker_color='#2ecc71',
            text=monthly_df['won'].apply(lambda x: f'${x:,.0f}'),
            textposition='auto'
        ))
        
        fig.add_trace(go.Bar(
            x=monthly_df['month'],
            y=monthly_df['lost'],
            name='Lost',
            marker_color='#e74c3c',
            text=monthly_df['lost'].apply(lambda x: f'${x:,.0f}'),
            textposition='auto'
        ))
        
        fig.add_trace(go.Bar(
            x=monthly_df['month'],
            y=monthly_df['pending'],
            name='Pending',
            marker_color='#95a5a6',
            text=monthly_df['pending'].apply(lambda x: f'${x:,.0f}'),
            textposition='auto'
        ))
        
        # Win rate line
        fig.add_trace(go.Scatter(
            x=monthly_df['month'],
            y=monthly_df['win_rate'],
            mode='lines+markers',
            name='Win Rate %',
            line=dict(color='#f39c12', width=3),
            marker=dict(size=10),
            yaxis='y2'
        ))
        
        fig.update_layout(
            barmode='stack',
            yaxis=dict(title='Dollar Amount', side='left'),
            yaxis2=dict(title='Win Rate %', overlaying='y', side='right', range=[0, 100]),
            hovermode='x unified',
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Monthly details table
        with st.expander("View Monthly Details"):
            display_df = monthly_df[['month_name', 'total_contested', 'won', 'lost', 'pending', 'win_rate']].copy()
            display_df.columns = ['Month', 'Contested', 'Won', 'Lost', 'Pending', 'Win Rate %']
            
            st.dataframe(
                display_df.style.format({
                    'Contested': '${:,.0f}',
                    'Won': '${:,.0f}',
                    'Lost': '${:,.0f}',
                    'Pending': '${:,.0f}',
                    'Win Rate %': '{:.1f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    
    # Chains Requiring Attention
    st.header("⚠️ Chains Requiring Attention")
    st.caption("Chains where 3-month win rate is 20% below 12-month average (DoorDash & UberEats only)")
    
    if attention_df is not None and not attention_df.empty:
        # Format the dataframe for display
        display_attention = attention_df.copy()
        display_attention['Alert'] = display_attention.apply(
            lambda row: "🔴 Critical" if row['decline_percentage'] > 30 else "⚠️ Warning", axis=1
        )
        
        # Select columns to display
        display_cols = ['chain', 'platform', 'Alert', 'avg_12m_win_rate', 'avg_3m_win_rate', 
                       'decline_percentage', 'months_of_data', 'total_settled_12m']
        display_attention = display_attention[display_cols].copy()
        display_attention.columns = ['Chain', 'Platform', 'Alert', '12M Win Rate %', '3M Win Rate %', 
                                     'Decline %', 'Months of Data', 'Total Settled ($)']
        
        # Apply formatting
        st.dataframe(
            display_attention.style.format({
                '12M Win Rate %': '{:.1f}%',
                '3M Win Rate %': '{:.1f}%',
                'Decline %': '-{:.1f}%',
                'Total Settled ($)': '${:,.0f}'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Chains Flagged", len(display_attention))
        with col2:
            critical_count = len(display_attention[display_attention['Alert'] == "🔴 Critical"])
            st.metric("Critical (>30% decline)", critical_count)
        with col3:
            avg_decline = attention_df['decline_percentage'].mean()
            st.metric("Average Decline", f"{avg_decline:.1f}%")
    else:
        st.info("No chains currently showing significant win rate decline.")
    
    st.markdown("---")
    
    # Platform Win Rate Trend
    st.header("📈 Platform Win Rate Trends")
    
    if not platform_trend_df.empty:
        # Create line chart for platform win rates
        fig_platform = go.Figure()
        
        # Add a line for each platform
        for platform in platform_trend_df['platform'].unique():
            platform_data = platform_trend_df[platform_trend_df['platform'] == platform]
            fig_platform.add_trace(go.Scatter(
                x=platform_data['month'],
                y=platform_data['win_rate'],
                mode='lines+markers',
                name=platform,
                line=dict(width=2),
                marker=dict(size=8),
                hovertemplate='%{y:.1f}%<br>%{x|%b %Y}<extra></extra>'
            ))
        
        fig_platform.update_layout(
            title="Win Rate Trends by Platform",
            xaxis_title="Month",
            yaxis_title="Win Rate (%)",
            yaxis=dict(range=[0, 100], ticksuffix="%"),
            hovermode='x unified',
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig_platform, use_container_width=True)
        
        # Summary statistics by platform
        with st.expander("View Platform Statistics"):
            platform_summary = platform_trend_df.groupby('platform').agg({
                'win_rate': ['mean', 'min', 'max'],
                'total_won': 'sum',
                'total_settled': 'sum'
            }).round(2)
            platform_summary.columns = ['Avg Win Rate %', 'Min Win Rate %', 'Max Win Rate %', 'Total Won $', 'Total Settled $']
            
            st.dataframe(
                platform_summary.style.format({
                    'Avg Win Rate %': '{:.1f}%',
                    'Min Win Rate %': '{:.1f}%',
                    'Max Win Rate %': '{:.1f}%',
                    'Total Won $': '${:,.0f}',
                    'Total Settled $': '${:,.0f}'
                }),
                use_container_width=True
            )
    
    st.markdown("---")
    
    # Platform and Chain Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("🎮 Recovery by Platform")
        if not platform_df.empty:
            fig = px.bar(
                platform_df.head(10),
                x='platform',
                y='win_rate',
                color='total_contested',
                color_continuous_scale='Blues',
                labels={'win_rate': 'Win Rate %', 'total_contested': 'Total Contested'},
                text='win_rate',
                title='Win Rate by Platform (Inaccurate Orders)'
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.header("🍔 Top Restaurant Chains")
        if not chain_df.empty:
            fig = px.scatter(
                chain_df.head(15),
                x='win_rate',
                y='total_contested',
                size='dispute_count',
                color='win_rate',
                color_continuous_scale='RdYlGn',
                range_color=[0, 100],
                labels={'win_rate': 'Win Rate %', 'total_contested': 'Total Contested ($)'},
                title='Chains: Win Rate vs Volume (Inaccurate Orders)',
                hover_name='chain',
                hover_data={'dispute_count': ':,.0f', 'location_count': ':,.0f'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Chain details table
    if not chain_df.empty:
        st.header("📋 Restaurant Chain Details - Inaccurate Orders")
        
        display_chain = chain_df[['chain', 'total_contested', 'won', 'lost', 'pending', 'win_rate', 'location_count']].copy()
        display_chain.columns = ['Chain', 'Contested', 'Won', 'Lost', 'Pending', 'Win Rate %', 'Locations']
        
        st.dataframe(
            display_chain.style.format({
                'Contested': '${:,.0f}',
                'Won': '${:,.0f}',
                'Lost': '${:,.0f}',
                'Pending': '${:,.0f}',
                'Win Rate %': '{:.1f}%',
                'Locations': '{:,.0f}'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    st.markdown("---")
    
    # Cohort Analysis Section
    st.header("📈 Cohort Analysis - Recovery per Location")
    st.caption("Top 20 chains by total recovery volume - showing average recovery $ per active location per month")
    
    if not cohort_df.empty:
        # Create heatmap using plotly
        fig = go.Figure(data=go.Heatmap(
            z=cohort_df.values,
            x=cohort_df.columns,
            y=cohort_df.index,
            colorscale='RdYlGn',
            text=cohort_df.values,
            texttemplate='$%{text:,.0f}',
            textfont={"size": 10},
            colorbar=dict(
                title="$/Location"
            )
        ))
        
        fig.update_layout(
            title="Recovery per Location by Chain and Month",
            xaxis_title="Month",
            yaxis_title="Restaurant Chain",
            height=600,
            xaxis={'side': 'top'},
            yaxis={'autorange': 'reversed'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show the raw data table
        with st.expander("📊 View Cohort Data Table"):
            st.caption("Recovery amount per location ($ Won / # Active Locations)")
            
            # Format the dataframe for display
            display_cohort = cohort_df.copy()
            
            # Apply formatting without background gradient (matplotlib not installed)
            st.dataframe(
                display_cohort.style.format('${:,.0f}'),
                use_container_width=True
            )
            
            # Summary statistics
            st.caption("Summary Statistics:")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average $/Location", f"${cohort_df.mean().mean():,.0f}")
            with col2:
                st.metric("Max $/Location", f"${cohort_df.max().max():,.0f}")
            with col3:
                st.metric("Min $/Location", f"${cohort_df.min().min():,.0f}")
    else:
        st.info("No cohort data available for the selected filters.")
    
    st.markdown("---")
    
    # Dispute Filing Status Analysis
    st.header("📊 Dispute Filing Status Analysis")
    st.caption("Analysis of disputes by their processing status - Filed (Accepted/Denied/In-Progress) vs Not Filed (To-Be-Raised/Expired)")
    
    with st.spinner("Loading on-time dispute analysis..."):
        ontime_df = get_ontime_dispute_analysis(date_range, platform_filter, chain_filter)
    
    if not ontime_df.empty:
        # Display metrics by platform
        cols = st.columns(len(ontime_df))
        
        for idx, (col, row) in enumerate(zip(cols, ontime_df.itertuples())):
            with col:
                # Color based on performance
                if row.on_time_percentage >= 90:
                    color = "🟢"
                elif row.on_time_percentage >= 75:
                    color = "🟡"
                else:
                    color = "🔴"
                
                st.metric(
                    f"{color} {row.platform}",
                    f"{row.on_time_percentage:.1f}%",
                    f"{row.on_time_count:,} of {row.on_time_count + row.late_count:,} disputes",
                    help="Disputes that have been filed (ACCEPTED/DENIED/IN_PROGRESS) vs not filed (TO_BE_RAISED/EXPIRED)"
                )
        
        # Visualization
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart of on-time percentages
            fig_bar = px.bar(
                ontime_df,
                x='platform',
                y='on_time_percentage',
                title='Dispute Filing Rate by Platform',
                labels={'on_time_percentage': 'On-Time %', 'platform': 'Platform'},
                color='on_time_percentage',
                color_continuous_scale='RdYlGn',
                range_color=[0, 100],
                text='on_time_percentage'
            )
            fig_bar.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Pie chart showing overall on-time vs late
            total_on_time = ontime_df['on_time_count'].sum()
            total_late = ontime_df['late_count'].sum()
            
            pie_data = pd.DataFrame({
                'status': ['Filed', 'Not Filed'],
                'count': [total_on_time, total_late]
            })
            
            fig_pie = px.pie(
                pie_data,
                values='count',
                names='status',
                title='Overall Filed vs Not Filed Disputes',
                color_discrete_map={
                    'Filed': '#2ecc71',
                    'Not Filed': '#e74c3c'
                }
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)
        
        # Detailed table
        with st.expander("View Platform Details"):
            display_df = ontime_df[['platform', 'dispute_window', 'total_count', 'on_time_count', 'late_count', 'on_time_percentage', 'total_amount']].copy()
            display_df.columns = ['Platform', 'Window (Days)', 'Total Disputes', 'Filed', 'Not Filed', 'Filed %', 'Total Amount']
            
            st.dataframe(
                display_df.style.format({
                    'Total Disputes': '{:,.0f}',
                    'Filed': '{:,.0f}',
                    'Not Filed': '{:,.0f}',
                    'Filed %': '{:.1f}%',
                    'Total Amount': '${:,.0f}'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    
    # Win Rate Cohort Analysis Section
    st.header("🎆 Win Rate Cohort Analysis")
    st.caption("Top 10 chains by settled volume - showing win rates (%) by month")
    
    if not win_rate_cohort_df.empty:
        # Create heatmap for win rates
        fig_wr = go.Figure(data=go.Heatmap(
            z=win_rate_cohort_df.values,
            x=win_rate_cohort_df.columns,
            y=win_rate_cohort_df.index,
            colorscale='RdYlGn',
            text=win_rate_cohort_df.values,
            texttemplate='%{text:.1f}%',
            textfont={"size": 10},
            colorbar=dict(
                title="Win Rate %"
            ),
            zmin=0,
            zmax=100
        ))
        
        fig_wr.update_layout(
            title="Win Rate % by Chain and Month",
            xaxis_title="Month",
            yaxis_title="Restaurant Chain",
            height=500,
            xaxis={'side': 'top'},
            yaxis={'autorange': 'reversed'}
        )
        
        st.plotly_chart(fig_wr, use_container_width=True)
        
        # Show the raw data table
        with st.expander("📊 View Win Rate Data Table"):
            st.caption("Monthly win rates (%) for top 10 chains")
            
            # Format the dataframe for display
            display_win_rate = win_rate_cohort_df.copy()
            
            # Apply formatting
            st.dataframe(
                display_win_rate.style.format('{:.1f}%'),
                use_container_width=True
            )
            
            # Summary statistics
            st.caption("Summary Statistics:")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average Win Rate", f"{win_rate_cohort_df.mean().mean():.1f}%")
            with col2:
                st.metric("Max Win Rate", f"{win_rate_cohort_df.max().max():.1f}%")
            with col3:
                st.metric("Min Win Rate", f"{win_rate_cohort_df.min().min():.1f}%")
    else:
        st.info("No win rate cohort data available for the selected filters.")

if __name__ == "__main__":
    main()