"""
Weekly Scorecard Dashboard for Recovery Health Monitoring
P0-P4 Chain Segmentation with Monthly Overview
"""

import streamlit as st
import pandas as pd
import pandas_gbq
import os
from datetime import date, timedelta, datetime
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import calendar
from google.oauth2 import service_account

# Configuration
PROJECT_ID = 'arboreal-vision-339901'

# Page config
st.set_page_config(
    page_title="Weekly Recovery Scorecard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize credentials
try:
    # Try to use Streamlit secrets first (for cloud deployment)
    if 'gcp_service_account' in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
    else:
        # For local development
        os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
        credentials = None
except Exception as e:
    st.error(f"Error loading credentials: {e}")
    credentials = None

# Title and description
st.title("📊 Weekly Recovery Scorecard")
st.markdown("*Executive health monitoring dashboard with P0-P4 chain segmentation*")

# Add filters in sidebar
st.sidebar.header("🔍 Filters")

# Get available chains, platforms, and locations for filters
@st.cache_data(ttl=3600)
def get_filter_options():
    """Get unique chains, platforms, and b_names for filters"""

    chains_query = """
    SELECT DISTINCT chain
    FROM `restaurant_aggregate_metrics.slug_am_mapping`
    WHERE chain IS NOT NULL AND chain != ''
    ORDER BY chain
    """

    platforms_query = """
    SELECT DISTINCT platform
    FROM `merchant_portal_export.chargeback_split_summary`
    WHERE platform IS NOT NULL
    ORDER BY platform
    """

    bnames_query = """
    SELECT DISTINCT b_name
    FROM `restaurant_aggregate_metrics.slug_am_mapping`
    WHERE b_name IS NOT NULL AND b_name != ''
    ORDER BY b_name
    LIMIT 1000
    """

    try:
        chains_df = pandas_gbq.read_gbq(chains_query, project_id=PROJECT_ID, credentials=credentials)
        platforms_df = pandas_gbq.read_gbq(platforms_query, project_id=PROJECT_ID, credentials=credentials)
        bnames_df = pandas_gbq.read_gbq(bnames_query, project_id=PROJECT_ID, credentials=credentials)

        return (
            chains_df['chain'].tolist() if not chains_df.empty else [],
            platforms_df['platform'].tolist() if not platforms_df.empty else [],
            bnames_df['b_name'].tolist() if not bnames_df.empty else []
        )
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return [], [], []

# Get filter options
chains_list, platforms_list, bnames_list = get_filter_options()

# Create filter widgets
selected_chains = st.sidebar.multiselect(
    "Select Chain(s)",
    options=["All"] + chains_list,
    default=["All"],
    help="Filter by restaurant chain"
)

selected_platforms = st.sidebar.multiselect(
    "Select Platform(s)",
    options=["All"] + platforms_list,
    default=["All"],
    help="Filter by delivery platform"
)

selected_bnames = st.sidebar.multiselect(
    "Select Location(s)",
    options=["All"] + bnames_list,
    default=["All"],
    help="Filter by location name (b_name)"
)

# Process filter selections
filter_chains = None if "All" in selected_chains else selected_chains
filter_platforms = None if "All" in selected_platforms else selected_platforms
filter_bnames = None if "All" in selected_bnames else selected_bnames

st.sidebar.markdown("---")
st.sidebar.caption("Filters will be applied to all dashboard sections")

# Get current date info for last 30 and 90 days calculation
today = date.today()
current_month_start = today - timedelta(days=30)
current_month_end = today
last_90_start = today - timedelta(days=90)
last_90_end = today

# Calculate previous months
def get_month_dates(months_back):
    """Get start and end dates for a month N months back"""
    year = today.year
    month = today.month - months_back

    while month <= 0:
        month += 12
        year -= 1

    start = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = date(year, month, last_day)

    return start, end

# Get dates for last 3 complete months
last_month_start, last_month_end = get_month_dates(1)
month_2_start, month_2_end = get_month_dates(2)
month_3_start, month_3_end = get_month_dates(3)

@st.cache_data(ttl=3600)
def get_monthly_overview(start_date, end_date, month_label, filter_chains=None, filter_platforms=None, filter_bnames=None):
    """Get monthly overview metrics including dispute counts"""

    # Build filter conditions
    filter_conditions = []
    filter_conditions_coe = []  # For chargeback_orders_enriched table

    if filter_chains:
        chains_str = "', '".join(filter_chains)
        filter_conditions.append(f"sm.chain IN ('{chains_str}')")
        filter_conditions_coe.append(f"chain IN ('{chains_str}')")
    if filter_platforms:
        platforms_str = "', '".join(filter_platforms)
        filter_conditions.append(f"TRIM(cs.platform) IN ('{platforms_str}')")
        filter_conditions_coe.append(f"TRIM(platform) IN ('{platforms_str}')")
    if filter_bnames:
        bnames_str = "', '".join(filter_bnames)
        filter_conditions.append(f"sm.b_name IN ('{bnames_str}')")
        filter_conditions_coe.append(f"b_name IN ('{bnames_str}')")

    filter_clause = " AND " + " AND ".join(filter_conditions) if filter_conditions else ""
    filter_clause_coe = " AND " + " AND ".join(filter_conditions_coe) if filter_conditions_coe else ""

    query = f"""
    WITH monthly_data AS (
        SELECT
            sm.chain,
            sm.slug,
            sm.b_name_id,
            cs.platform,
            cs.external_status,
            cs.error_category,
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            CASE
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0
            END as settled_amount,
            CASE
                WHEN cs.external_status = 'ACCEPTED' THEN 'won'
                WHEN cs.external_status = 'DENIED' THEN 'lost'
                WHEN cs.external_status IN ('IN_PROGRESS', 'TO_BE_RAISED') THEN 'pending'
                ELSE 'other'
            END as dispute_status
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{start_date}' AND '{end_date}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
            {filter_clause}
    ),
    -- Get location count from chargeback_orders_enriched table
    location_counts AS (
        SELECT
            COUNT(DISTINCT CONCAT(chain, b_name)) as unique_locations
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
            {filter_clause_coe}
    ),
    platform_metrics AS (
        SELECT
            chain,
            -- Count unique slug-platform combinations (match actual platform values in database)
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Doordash' THEN slug END) as dd_locations,
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'UberEats' THEN slug END) as ue_locations,
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Grubhub' THEN slug END) as gh_locations,
            COUNT(*) as total_disputed,
            SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as disputes_won,
            SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as disputes_lost,
            SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as disputes_pending,
            SUM(CASE WHEN external_status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as disputes_in_progress,
            SUM(CASE WHEN external_status = 'TO_BE_RAISED' THEN 1 ELSE 0 END) as disputes_to_be_raised,
            SUM(CASE WHEN external_status = 'EXPIRED' THEN 1 ELSE 0 END) as disputes_expired,
            SUM(won_amount) as total_won,
            SUM(settled_amount) as total_settled
        FROM monthly_data
        GROUP BY chain
    )
    SELECT
        COUNT(DISTINCT chain) as chain_count,
        (SELECT unique_locations FROM location_counts) as unique_locations,
        SUM(dd_locations) as dd_locations,
        SUM(ue_locations) as ue_locations,
        SUM(gh_locations) as gh_locations,
        SUM(total_disputed) as total_disputed,
        SUM(disputes_won) as disputes_won,
        SUM(disputes_lost) as disputes_lost,
        SUM(disputes_pending) as disputes_pending,
        SUM(disputes_in_progress) as disputes_in_progress,
        SUM(disputes_to_be_raised) as disputes_to_be_raised,
        SUM(disputes_expired) as disputes_expired,
        SUM(total_won) as total_recovered,
        SUM(total_settled) as total_settled,
        ROUND(SAFE_DIVIDE(SUM(total_won), NULLIF(SUM(total_settled), 0)) * 100, 2) as win_rate,
        -- Use total location count (sum of all platforms) for avg calculation
        ROUND(SAFE_DIVIDE(SUM(total_won), NULLIF(SUM(dd_locations) + SUM(ue_locations) + SUM(gh_locations), 0)), 2) as avg_per_location
    FROM platform_metrics
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        if not df.empty:
            row = df.iloc[0]
            # Calculate days in period for monthly average
            days_in_period = (end_date - start_date).days + 1
            days_in_month = 30  # Standardize to 30 days for monthly average
            
            return {
                'month': month_label,
                'chains': int(row['chain_count']) if pd.notna(row['chain_count']) else 0,
                'unique_locations': int(row['unique_locations']) if pd.notna(row['unique_locations']) else 0,
                'dd_locations': int(row['dd_locations']) if pd.notna(row['dd_locations']) else 0,
                'ue_locations': int(row['ue_locations']) if pd.notna(row['ue_locations']) else 0,
                'gh_locations': int(row['gh_locations']) if pd.notna(row['gh_locations']) else 0,
                'disputed': int(row['total_disputed']) if pd.notna(row['total_disputed']) else 0,
                'won': int(row['disputes_won']) if pd.notna(row['disputes_won']) else 0,
                'lost': int(row['disputes_lost']) if pd.notna(row['disputes_lost']) else 0,
                'pending': int(row['disputes_pending']) if pd.notna(row['disputes_pending']) else 0,
                'in_progress': int(row['disputes_in_progress']) if pd.notna(row['disputes_in_progress']) else 0,
                'to_be_raised': int(row['disputes_to_be_raised']) if pd.notna(row['disputes_to_be_raised']) else 0,
                'expired': int(row['disputes_expired']) if pd.notna(row['disputes_expired']) else 0,
                'recovered': float(row['total_recovered']) if pd.notna(row['total_recovered']) else 0,
                'settled': float(row['total_settled']) if pd.notna(row['total_settled']) else 0,
                'win_rate': float(row['win_rate']) if pd.notna(row['win_rate']) else 0,
                'avg_per_location_per_month': float(row['avg_per_location']) * (days_in_month / days_in_period) if pd.notna(row['avg_per_location']) else 0
            }
        return None
    except Exception as e:
        st.error(f"Error fetching monthly data: {e}")
        return None

@st.cache_data(ttl=3600)
def get_chains_movement(current_start, current_end, previous_start, previous_end):
    """Get chains that entered or exited Recover between two periods"""
    
    query = f"""
    WITH current_month_chains AS (
        SELECT DISTINCT sm.chain
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{current_start}' AND '{current_end}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
    ),
    previous_month_chains AS (
        SELECT DISTINCT sm.chain
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{previous_start}' AND '{previous_end}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
    ),
    chain_segments AS (
        SELECT
            chain,
            COUNT(DISTINCT CONCAT(chain, b_name)) as location_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT CONCAT(chain, b_name)) DESC) as rank_by_locations
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
            AND chain IS NOT NULL
            AND chain != ''
        GROUP BY chain
    ),
    segmented_chains AS (
        SELECT 
            chain,
            location_count,
            CASE 
                WHEN rank_by_locations <= 15 THEN 'P0'
                WHEN rank_by_locations <= 40 THEN 'P1'
                WHEN rank_by_locations <= 70 THEN 'P2'
                WHEN rank_by_locations <= 132 THEN 'P3'
                ELSE 'P4'
            END as segment
        FROM chain_segments
    ),
    movement AS (
        SELECT 
            COALESCE(c.chain, p.chain) as chain,
            CASE 
                WHEN c.chain IS NOT NULL AND p.chain IS NULL THEN 'entered'
                WHEN c.chain IS NULL AND p.chain IS NOT NULL THEN 'exited'
                ELSE 'stayed'
            END as movement_type
        FROM current_month_chains c
        FULL OUTER JOIN previous_month_chains p ON c.chain = p.chain
    )
    SELECT 
        m.chain,
        m.movement_type,
        COALESCE(s.segment, 'P4') as segment,
        COALESCE(s.location_count, 0) as location_count
    FROM movement m
    LEFT JOIN segmented_chains s ON m.chain = s.chain
    WHERE m.movement_type IN ('entered', 'exited')
    ORDER BY s.segment, m.movement_type, s.location_count DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        return df
    except Exception as e:
        st.error(f"Error fetching chain movement: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_platform_breakdown(start_date, end_date, month_label, filter_chains=None, filter_platforms=None, filter_bnames=None):
    """Get platform-specific metrics"""

    # Build filter conditions
    filter_conditions = []
    if filter_chains:
        chains_str = "', '".join(filter_chains)
        filter_conditions.append(f"sm.chain IN ('{chains_str}')")
    if filter_platforms:
        platforms_str = "', '".join(filter_platforms)
        filter_conditions.append(f"TRIM(cs.platform) IN ('{platforms_str}')")
    if filter_bnames:
        bnames_str = "', '".join(filter_bnames)
        filter_conditions.append(f"sm.b_name IN ('{bnames_str}')")

    filter_clause = " AND " + " AND ".join(filter_conditions) if filter_conditions else ""

    query = f"""
    WITH monthly_data AS (
        SELECT
            cs.platform,
            sm.b_name_id,
            sm.slug,
            sm.chain,
            sm.b_name,
            cs.external_status,
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            CASE
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0
            END as settled_amount,
            CASE
                WHEN cs.external_status = 'ACCEPTED' THEN 'won'
                WHEN cs.external_status = 'DENIED' THEN 'lost'
                WHEN cs.external_status IN ('IN_PROGRESS', 'TO_BE_RAISED') THEN 'pending'
                ELSE 'other'
            END as dispute_status
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{start_date}' AND '{end_date}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
            {filter_clause}
    ),
    -- Get location counts per platform from chargeback_orders_enriched
    platform_locations AS (
        SELECT
            platform,
            COUNT(DISTINCT CONCAT(chain, b_name)) as unique_locations
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
        GROUP BY platform
    )
    SELECT
        TRIM(md.platform) as platform,
        COALESCE(pl.unique_locations, 0) as unique_locations,
        COUNT(DISTINCT md.slug) as slug_count,
        COUNT(*) as total_disputed,
        SUM(CASE WHEN md.dispute_status = 'won' THEN 1 ELSE 0 END) as disputes_won,
        SUM(CASE WHEN md.dispute_status = 'lost' THEN 1 ELSE 0 END) as disputes_lost,
        SUM(CASE WHEN md.dispute_status = 'pending' THEN 1 ELSE 0 END) as disputes_pending,
        SUM(CASE WHEN md.external_status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as disputes_in_progress,
        SUM(CASE WHEN md.external_status = 'TO_BE_RAISED' THEN 1 ELSE 0 END) as disputes_to_be_raised,
        SUM(CASE WHEN md.external_status = 'EXPIRED' THEN 1 ELSE 0 END) as disputes_expired,
        SUM(md.won_amount) as total_recovered,
        SUM(md.settled_amount) as total_settled,
        ROUND(SAFE_DIVIDE(SUM(md.won_amount), NULLIF(SUM(md.settled_amount), 0)) * 100, 2) as win_rate
    FROM monthly_data md
    LEFT JOIN platform_locations pl ON TRIM(md.platform) = pl.platform
    GROUP BY md.platform, pl.unique_locations
    ORDER BY md.platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        return df
    except Exception as e:
        st.error(f"Error fetching platform breakdown: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_chain_segmentation():
    """Get P0-P4 segmentation based on our finalized classification"""
    
    # Based on the smart segmentation analysis results
    # P0: Top 15 chains (339+ locations)
    # P1: Next 25 chains (123-311 locations)
    # P2: Next 30 chains (72-122 locations)
    # P3: Next 62 chains (27-69 locations)
    # P4: Remaining chains (1-26 locations)
    
    query = """
    WITH chain_sizes AS (
        SELECT
            chain,
            COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
            AND chain IS NOT NULL
            AND chain != ''
        GROUP BY chain
    ),
    -- Get volume from chargeback_split_summary for ranking purposes
    chain_volumes AS (
        SELECT
            sm.chain,
            SUM(COALESCE(cs.enabled_customer_refunds, 0)) as total_volume
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
        GROUP BY sm.chain
    ),
    ranked_chains AS (
        SELECT
            cs.chain,
            cs.location_count,
            COALESCE(cv.total_volume, 0) as total_volume,
            ROW_NUMBER() OVER (ORDER BY cs.location_count DESC) as rank_by_locations
        FROM chain_sizes cs
        LEFT JOIN chain_volumes cv ON cs.chain = cv.chain
    )
    SELECT
        chain,
        location_count,
        total_volume,
        rank_by_locations,
        CASE
            WHEN rank_by_locations <= 15 THEN 'P0'
            WHEN rank_by_locations <= 40 THEN 'P1'
            WHEN rank_by_locations <= 70 THEN 'P2'
            WHEN rank_by_locations <= 132 THEN 'P3'
            ELSE 'P4'
        END as segment
    FROM ranked_chains
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        return df
    except Exception as e:
        st.error(f"Error fetching segmentation: {e}")
        return pd.DataFrame()

# Header with date context
st.markdown(f"**Report Date**: {today.strftime('%B %d, %Y')} | **Last 30 Days**: {current_month_start.strftime('%b %d')} - {current_month_end.strftime('%b %d')}")
st.markdown("---")

# Section 1: Overall Monthly Performance
st.header("📈 Overall Monthly Performance")

# Fetch data for all 5 periods
with st.spinner("Loading monthly overview data..."):
    mtd_data = get_monthly_overview(current_month_start, current_month_end, "Last 30 Days", filter_chains, filter_platforms, filter_bnames)
    last_90_data = get_monthly_overview(last_90_start, last_90_end, "Last 90 Days", filter_chains, filter_platforms, filter_bnames)
    month_1_data = get_monthly_overview(last_month_start, last_month_end, last_month_start.strftime('%B'), filter_chains, filter_platforms, filter_bnames)
    month_2_data = get_monthly_overview(month_2_start, month_2_end, month_2_start.strftime('%B'), filter_chains, filter_platforms, filter_bnames)
    month_3_data = get_monthly_overview(month_3_start, month_3_end, month_3_start.strftime('%B'), filter_chains, filter_platforms, filter_bnames)

# Create a comprehensive table view
if all([mtd_data, last_90_data, month_1_data, month_2_data, month_3_data]):
    # Prepare data for table
    table_data = []

    for data in [mtd_data, last_90_data, month_1_data, month_2_data, month_3_data]:
        if data:
            recovered_per_location = data['recovered'] / data['unique_locations'] if data['unique_locations'] > 0 else 0
            row = {
                'Period': data['month'],
                'Chains (chain)': f"{data['chains']:,}",
                'Locations (chain + b_name)': f"{data['unique_locations']:,}",
                'DoorDash (slug)': f"{data['dd_locations']:,}",
                'UberEats (slug)': f"{data['ue_locations']:,}",
                'Grubhub (slug)': f"{data['gh_locations']:,}",
                'Disputed': f"{data['disputed']:,}",
                'Won': f"{data['won']:,}",
                'Lost': f"{data['lost']:,}",
                'Pending': f"{data['pending']:,}",
                'In Progress': f"{data['in_progress']:,}",
                'To Be Raised': f"{data['to_be_raised']:,}",
                'Expired': f"{data['expired']:,}",
                'Total Recovered (enabled_won_disputes)': f"${data['recovered']:,.0f}",
                '$/Location': f"${recovered_per_location:.0f}",
                'Win Rate': f"{data['win_rate']:.1f}%"
            }
            table_data.append(row)
    
    # Display as DataFrame
    overview_df = pd.DataFrame(table_data)
    
    # Style the dataframe
    styled_df = overview_df.style.set_properties(**{
        'background-color': '#f5f5f5',
        'color': 'black',
        'border-color': 'white'
    })
    
    # Highlight Last 30 Days and Last 90 Days rows
    def highlight_mtd(row):
        if 'Last 30 Days' in row['Period']:
            return ['background-color: #ffe6e6'] * len(row)
        elif 'Last 90 Days' in row['Period']:
            return ['background-color: #e6f2ff'] * len(row)
        else:
            return [''] * len(row)
    
    styled_df = styled_df.apply(highlight_mtd, axis=1)
    
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    # Platform Breakdown Section
    st.markdown("---")
    st.subheader("📱 Platform-Specific Performance")
    
    # Fetch platform data for all periods
    platform_mtd = get_platform_breakdown(current_month_start, current_month_end, "Last 30 Days")
    platform_90d = get_platform_breakdown(last_90_start, last_90_end, "Last 90 Days")
    platform_m1 = get_platform_breakdown(last_month_start, last_month_end, last_month_start.strftime('%B'))
    platform_m2 = get_platform_breakdown(month_2_start, month_2_end, month_2_start.strftime('%B'))
    platform_m3 = get_platform_breakdown(month_3_start, month_3_end, month_3_start.strftime('%B'))

    # Create tabs for each time period
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Last 30 Days",
                                             "Last 90 Days",
                                             last_month_start.strftime('%B'),
                                             month_2_start.strftime('%B'),
                                             month_3_start.strftime('%B')])
    
    def format_platform_table(df, period_label):
        if df.empty:
            return pd.DataFrame()
        
        # Add calculated columns
        df['recovered_per_location'] = df['total_recovered'] / df['unique_locations'].replace(0, 1)
        
        # Format for display
        formatted_data = []
        for _, row in df.iterrows():
            formatted_data.append({
                'Platform': row['platform'],
                'Locations (chain + b_name)': f"{int(row['unique_locations']):,}",
                'Slugs (slug)': f"{int(row['slug_count']):,}",
                'Disputed': f"{int(row['total_disputed']):,}",
                'Won': f"{int(row['disputes_won']):,}",
                'Lost': f"{int(row['disputes_lost']):,}",
                'Pending': f"{int(row['disputes_pending']):,}",
                'In Progress': f"{int(row['disputes_in_progress']):,}",
                'To Be Raised': f"{int(row['disputes_to_be_raised']):,}",
                'Expired': f"{int(row['disputes_expired']):,}",
                'Total Recovered (enabled_won_disputes)': f"${row['total_recovered']:,.0f}",
                '$/Location': f"${row['recovered_per_location']:.0f}",
                'Win Rate': f"{row['win_rate']:.1f}%"
            })
        
        return pd.DataFrame(formatted_data)
    
    with tab1:
        if not platform_mtd.empty:
            st.dataframe(format_platform_table(platform_mtd, "Last 30 Days"), use_container_width=True, hide_index=True)
        else:
            st.info("No data available for Last 30 Days")

    with tab2:
        if not platform_90d.empty:
            st.dataframe(format_platform_table(platform_90d, "Last 90 Days"), use_container_width=True, hide_index=True)
        else:
            st.info("No data available for Last 90 Days")

    with tab3:
        if not platform_m1.empty:
            st.dataframe(format_platform_table(platform_m1, last_month_start.strftime('%B')), use_container_width=True, hide_index=True)
        else:
            st.info(f"No data available for {last_month_start.strftime('%B')}")

    with tab4:
        if not platform_m2.empty:
            st.dataframe(format_platform_table(platform_m2, month_2_start.strftime('%B')), use_container_width=True, hide_index=True)
        else:
            st.info(f"No data available for {month_2_start.strftime('%B')}")

    with tab5:
        if not platform_m3.empty:
            st.dataframe(format_platform_table(platform_m3, month_3_start.strftime('%B')), use_container_width=True, hide_index=True)
        else:
            st.info(f"No data available for {month_3_start.strftime('%B')}")
    
    # Chain Movement Section (NEW)
    st.markdown("---")
    st.subheader("🔄 Chain Movement (Last 30 Days vs Previous Month)")
    
    # Get chain movement data
    chains_movement = get_chains_movement(current_month_start, current_month_end, last_month_start, last_month_end)
    
    if not chains_movement.empty:
        # Group by segment
        segments = ['P0', 'P1', 'P2', 'P3', 'P4']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Entered chains summary
            st.markdown("**📈 Chains that Entered Recover**")
            entered_summary = []
            for segment in segments:
                entered_chains = chains_movement[(chains_movement['segment'] == segment) & 
                                                (chains_movement['movement_type'] == 'entered')]
                if not entered_chains.empty:
                    entered_summary.append({
                        'Segment': segment,
                        'Count': len(entered_chains),
                        'Total Locations': entered_chains['location_count'].sum()
                    })
            
            if entered_summary:
                entered_df = pd.DataFrame(entered_summary)
                st.dataframe(entered_df, use_container_width=True, hide_index=True)
            else:
                st.info("No chains entered Recover this month")
        
        with col2:
            # Exited chains summary
            st.markdown("**📉 Chains that Exited Recover**")
            exited_summary = []
            for segment in segments:
                exited_chains = chains_movement[(chains_movement['segment'] == segment) & 
                                               (chains_movement['movement_type'] == 'exited')]
                if not exited_chains.empty:
                    exited_summary.append({
                        'Segment': segment,
                        'Count': len(exited_chains),
                        'Total Locations': exited_chains['location_count'].sum()
                    })
            
            if exited_summary:
                exited_df = pd.DataFrame(exited_summary)
                st.dataframe(exited_df, use_container_width=True, hide_index=True)
            else:
                st.info("No chains exited Recover this month")
    else:
        st.info("No chain movement detected between current and previous month")
    
    # Add summary metrics below the table
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        mtd_vs_last = ((mtd_data['win_rate'] - month_1_data['win_rate']) / month_1_data['win_rate'] * 100) if month_1_data['win_rate'] > 0 else 0
        st.metric(
            "L30D vs Last Month (Win Rate)",
            f"{mtd_data['win_rate']:.1f}%",
            f"{mtd_vs_last:+.1f}%"
        )

    with col2:
        mtd_vol_vs_last = ((mtd_data['recovered'] - month_1_data['recovered']) / month_1_data['recovered'] * 100) if month_1_data['recovered'] > 0 else 0
        st.metric(
            "L30D vs Last Month (Recovery)",
            f"${mtd_data['recovered']/1000:.1f}K",
            f"{mtd_vol_vs_last:+.1f}%"
        )
    
    with col3:
        total_disputes = sum([d['disputed'] for d in [mtd_data, last_90_data, month_1_data, month_2_data, month_3_data]])
        st.metric("Total Disputes (5 periods)", f"{total_disputes:,}")
    
    with col4:
        total_pending = mtd_data['pending']
        st.metric("Current Pending", f"{total_pending:,}")
else:
    st.error("Unable to load monthly overview data")

# Trend Analysis
if all([mtd_data, month_1_data, month_2_data, month_3_data]):
    st.markdown("---")
    st.subheader("📊 Trend Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Win Rate Trend
        months = [month_3_data['month'], month_2_data['month'], month_1_data['month'], mtd_data['month']]
        win_rates = [month_3_data['win_rate'], month_2_data['win_rate'], month_1_data['win_rate'], mtd_data['win_rate']]
        
        fig_wr = go.Figure()
        fig_wr.add_trace(go.Scatter(
            x=months, 
            y=win_rates,
            mode='lines+markers',
            name='Win Rate',
            line=dict(color='#00cc88', width=3),
            marker=dict(size=10)
        ))
        fig_wr.update_layout(
            title="Win Rate Trend",
            yaxis_title="Win Rate (%)",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig_wr, use_container_width=True)
    
    with col2:
        # Recovery Volume Trend
        volumes = [month_3_data['recovered']/1000, month_2_data['recovered']/1000, 
                  month_1_data['recovered']/1000, mtd_data['recovered']/1000]
        
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=months,
            y=volumes,
            name='Recovered',
            marker_color=['#636EFA', '#636EFA', '#636EFA', '#ff4b4b']
        ))
        fig_vol.update_layout(
            title="Recovery Volume Trend",
            yaxis_title="Recovered ($K)",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig_vol, use_container_width=True)

st.markdown("---")

# Section 2: P0-P4 Segment Performance (Last 30 Days)
st.header("🎯 Segment Performance (Last 30 Days)")

# Get segmentation data
segmentation_df = get_chain_segmentation()

if not segmentation_df.empty:
    # Get current month performance by segment
    @st.cache_data(ttl=3600)
    def get_segment_performance():
        """Get Last 30 Days performance by segment"""
        
        query = f"""
        WITH chain_segments AS (
            SELECT
                chain,
                COUNT(DISTINCT CONCAT(chain, b_name)) as location_count,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT CONCAT(chain, b_name)) DESC) as rank_by_locations
            FROM `merchant_portal_export.chargeback_orders_enriched`
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                AND is_loop_enabled = true
                AND loop_raised_timestamp IS NOT NULL
                AND chain IS NOT NULL
                AND chain != ''
            GROUP BY chain
        ),
        segmented_chains AS (
            SELECT
                chain,
                location_count,
                CASE
                    WHEN rank_by_locations <= 15 THEN 'P0'
                    WHEN rank_by_locations <= 40 THEN 'P1'
                    WHEN rank_by_locations <= 70 THEN 'P2'
                    WHEN rank_by_locations <= 132 THEN 'P3'
                    ELSE 'P4'
                END as segment
            FROM chain_segments
        ),
        mtd_performance AS (
            SELECT
                sc.segment,
                COUNT(DISTINCT sm.chain) as chain_count,
                (
                    SELECT COUNT(DISTINCT CONCAT(coe.chain, coe.b_name))
                    FROM `merchant_portal_export.chargeback_orders_enriched` coe
                    JOIN (SELECT DISTINCT chain FROM segmented_chains WHERE segment = sc.segment) seg_chains
                        ON coe.chain = seg_chains.chain
                    WHERE coe.order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
                        AND coe.is_loop_enabled = true
                        AND coe.loop_raised_timestamp IS NOT NULL
                ) as total_locations,
                SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_won,
                SUM(CASE
                    WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    THEN COALESCE(cs.enabled_customer_refunds, 0)
                    ELSE 0
                END) as total_settled,
                COUNT(*) as dispute_count
            FROM `merchant_portal_export.chargeback_split_summary` cs
            JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
            JOIN (SELECT DISTINCT chain, segment, location_count FROM segmented_chains) sc ON sm.chain = sc.chain
            WHERE cs.chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}'
            GROUP BY sc.segment
        )
        SELECT
            segment,
            chain_count,
            total_locations as location_count,
            total_won,
            total_settled,
            dispute_count,
            ROUND(SAFE_DIVIDE(total_won, NULLIF(total_settled, 0)) * 100, 2) as win_rate,
            ROUND(SAFE_DIVIDE(total_won, NULLIF(total_locations, 0)), 2) as avg_per_location
        FROM mtd_performance
        ORDER BY segment
        """
        
        return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
    
    segment_perf = get_segment_performance()
    
    if not segment_perf.empty:
        # Display segment cards
        cols = st.columns(5)
        
        segment_colors = {
            'P0': '#ff4b4b',
            'P1': '#ffa500',
            'P2': '#ffd700',
            'P3': '#00cc88',
            'P4': '#808080'
        }
        
        segment_names = {
            'P0': 'Critical (Monthly)',
            'P1': 'Major (Monthly)',
            'P2': 'Important (Monthly)',
            'P3': 'Growing (Monthly)',
            'P4': 'Long Tail (Monthly)'
        }
        
        for i, segment in enumerate(['P0', 'P1', 'P2', 'P3', 'P4']):
            seg_data = segment_perf[segment_perf['segment'] == segment]
            
            with cols[i]:
                if not seg_data.empty:
                    row = seg_data.iloc[0]
                    
                    st.markdown(f"""
                    <div style='background-color: {segment_colors[segment]}20; 
                                padding: 10px; 
                                border-radius: 10px; 
                                border-left: 4px solid {segment_colors[segment]}'>
                        <h4 style='color: {segment_colors[segment]}; margin: 0;'>{segment}</h4>
                        <p style='margin: 0; font-size: 0.8em;'>{segment_names[segment]}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.metric("Chains", f"{int(row['chain_count'])}")
                    st.metric("Locations", f"{int(row['location_count']):,}")
                    st.metric("Win Rate", f"{row['win_rate']:.1f}%")
                    st.metric("Recovered", f"${row['total_won']/1000:.1f}K")
                else:
                    st.markdown(f"### {segment}")
                    st.info("No data")

# Section 3: P0-P4 Segment Breakdown
st.markdown("---")
st.header("📊 Segment Performance Breakdown")

# Get segment-specific monthly data
@st.cache_data(ttl=3600)
def get_segment_monthly_data(start_date, end_date, month_label, segment):
    """Get monthly metrics for a specific segment"""
    
    query = f"""
    WITH chain_segments AS (
        SELECT 
            sm.chain,
            COUNT(DISTINCT sm.slug) as location_count,
            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT sm.slug) DESC) as rank_by_locations
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
        GROUP BY sm.chain
    ),
    segmented_chains AS (
        SELECT 
            chain,
            CASE 
                WHEN rank_by_locations <= 15 THEN 'P0'
                WHEN rank_by_locations <= 40 THEN 'P1'
                WHEN rank_by_locations <= 70 THEN 'P2'
                WHEN rank_by_locations <= 132 THEN 'P3'
                ELSE 'P4'
            END as segment
        FROM chain_segments
    ),
    monthly_data AS (
        SELECT
            sm.chain,
            sm.slug,
            sm.b_name_id,
            cs.platform,
            cs.external_status,
            COALESCE(cs.enabled_won_disputes, 0) as won_amount,
            CASE
                WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                    AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                THEN COALESCE(cs.enabled_customer_refunds, 0)
                ELSE 0
            END as settled_amount,
            CASE
                WHEN cs.external_status = 'ACCEPTED' THEN 'won'
                WHEN cs.external_status = 'DENIED' THEN 'lost'
                WHEN cs.external_status IN ('IN_PROGRESS', 'TO_BE_RAISED') THEN 'pending'
                ELSE 'other'
            END as dispute_status
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        JOIN segmented_chains sc ON sm.chain = sc.chain
        WHERE cs.chargeback_date BETWEEN '{start_date}' AND '{end_date}'
            AND sc.segment = '{segment}'
    ),
    -- Get location count for the segment from chargeback_orders_enriched
    segment_locations AS (
        SELECT
            COUNT(DISTINCT CONCAT(coe.chain, coe.b_name)) as unique_locations
        FROM `merchant_portal_export.chargeback_orders_enriched` coe
        JOIN (
            SELECT DISTINCT chain FROM segmented_chains WHERE segment = '{segment}'
        ) sc ON coe.chain = sc.chain
        WHERE coe.order_date BETWEEN '{start_date}' AND '{end_date}'
            AND coe.is_loop_enabled = true
            AND coe.loop_raised_timestamp IS NOT NULL
    ),
    platform_metrics AS (
        SELECT
            chain,
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Doordash' THEN slug END) as dd_locations,
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'UberEats' THEN slug END) as ue_locations,
            COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Grubhub' THEN slug END) as gh_locations,
            COUNT(*) as total_disputed,
            SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as disputes_won,
            SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as disputes_lost,
            SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as disputes_pending,
            SUM(CASE WHEN external_status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as disputes_in_progress,
            SUM(CASE WHEN external_status = 'TO_BE_RAISED' THEN 1 ELSE 0 END) as disputes_to_be_raised,
            SUM(CASE WHEN external_status = 'EXPIRED' THEN 1 ELSE 0 END) as disputes_expired,
            SUM(won_amount) as total_won,
            SUM(settled_amount) as total_settled
        FROM monthly_data
        GROUP BY chain
    )
    SELECT
        COUNT(DISTINCT chain) as chain_count,
        (SELECT unique_locations FROM segment_locations) as unique_locations,
        SUM(dd_locations) as dd_locations,
        SUM(ue_locations) as ue_locations,
        SUM(gh_locations) as gh_locations,
        SUM(total_disputed) as total_disputed,
        SUM(disputes_won) as disputes_won,
        SUM(disputes_lost) as disputes_lost,
        SUM(disputes_pending) as disputes_pending,
        SUM(disputes_in_progress) as disputes_in_progress,
        SUM(disputes_to_be_raised) as disputes_to_be_raised,
        SUM(disputes_expired) as disputes_expired,
        SUM(total_won) as total_recovered,
        SUM(total_settled) as total_settled,
        ROUND(SAFE_DIVIDE(SUM(total_won), NULLIF(SUM(total_settled), 0)) * 100, 2) as win_rate
    FROM platform_metrics
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        if not df.empty:
            row = df.iloc[0]
            return {
                'month': month_label,
                'chains': int(row['chain_count']) if pd.notna(row['chain_count']) else 0,
                'unique_locations': int(row['unique_locations']) if pd.notna(row['unique_locations']) else 0,
                'dd_locations': int(row['dd_locations']) if pd.notna(row['dd_locations']) else 0,
                'ue_locations': int(row['ue_locations']) if pd.notna(row['ue_locations']) else 0,
                'gh_locations': int(row['gh_locations']) if pd.notna(row['gh_locations']) else 0,
                'disputed': int(row['total_disputed']) if pd.notna(row['total_disputed']) else 0,
                'won': int(row['disputes_won']) if pd.notna(row['disputes_won']) else 0,
                'lost': int(row['disputes_lost']) if pd.notna(row['disputes_lost']) else 0,
                'pending': int(row['disputes_pending']) if pd.notna(row['disputes_pending']) else 0,
                'in_progress': int(row['disputes_in_progress']) if pd.notna(row['disputes_in_progress']) else 0,
                'to_be_raised': int(row['disputes_to_be_raised']) if pd.notna(row['disputes_to_be_raised']) else 0,
                'expired': int(row['disputes_expired']) if pd.notna(row['disputes_expired']) else 0,
                'recovered': float(row['total_recovered']) if pd.notna(row['total_recovered']) else 0,
                'settled': float(row['total_settled']) if pd.notna(row['total_settled']) else 0,
                'win_rate': float(row['win_rate']) if pd.notna(row['win_rate']) else 0
            }
        return None
    except Exception as e:
        st.error(f"Error fetching segment data: {e}")
        return None

# Get chain movement data once for all segments
chains_movement = get_chains_movement(current_month_start, current_month_end, last_month_start, last_month_end)

# Display each segment
segments = ['P0', 'P1', 'P2', 'P3', 'P4']
segment_colors = {
    'P0': '#ff4b4b',
    'P1': '#ffa500',
    'P2': '#ffd700',
    'P3': '#00cc88',
    'P4': '#808080'
}
segment_names = {
    'P0': 'Critical (Monthly Monitoring)',
    'P1': 'Major (Monthly Monitoring)',
    'P2': 'Important (Monthly Monitoring)',
    'P3': 'Growing (Monthly Monitoring)',
    'P4': 'Long Tail (Monthly Monitoring)'
}

for segment in segments:
    st.markdown("---")
    st.subheader(f"{segment} - {segment_names[segment]}")
    
    with st.spinner(f"Loading {segment} data..."):
        # Fetch data for all periods for this segment
        seg_mtd = get_segment_monthly_data(current_month_start, current_month_end, "Last 30 Days", segment)
        seg_m1 = get_segment_monthly_data(last_month_start, last_month_end, last_month_start.strftime('%B'), segment)
        seg_m2 = get_segment_monthly_data(month_2_start, month_2_end, month_2_start.strftime('%B'), segment)
        seg_m3 = get_segment_monthly_data(month_3_start, month_3_end, month_3_start.strftime('%B'), segment)
    
    if all([seg_mtd, seg_m1, seg_m2, seg_m3]):
        # Create table for this segment
        seg_table_data = []
        
        for data in [seg_mtd, seg_m1, seg_m2, seg_m3]:
            if data:
                recovered_per_location = data['recovered'] / data['unique_locations'] if data['unique_locations'] > 0 else 0
                row = {
                    'Period': data['month'],
                    'Chains (chain)': f"{data['chains']:,}",
                    'Locations (chain + b_name)': f"{data['unique_locations']:,}",
                    'DoorDash (slug)': f"{data['dd_locations']:,}",
                    'UberEats (slug)': f"{data['ue_locations']:,}",
                    'Grubhub (slug)': f"{data['gh_locations']:,}",
                    'Disputed': f"{data['disputed']:,}",
                    'Won': f"{data['won']:,}",
                    'Lost': f"{data['lost']:,}",
                    'Pending': f"{data['pending']:,}",
                    'In Progress': f"{data['in_progress']:,}",
                    'To Be Raised': f"{data['to_be_raised']:,}",
                    'Expired': f"{data['expired']:,}",
                    'Total Recovered (enabled_won_disputes)': f"${data['recovered']:,.0f}",
                    '$/Location': f"${recovered_per_location:.0f}",
                    'Win Rate': f"{data['win_rate']:.1f}%"
                }
                seg_table_data.append(row)
        
        # Display as DataFrame
        seg_df = pd.DataFrame(seg_table_data)
        
        # Style the dataframe
        styled_seg_df = seg_df.style.set_properties(**{
            'background-color': f'{segment_colors[segment]}20',
            'color': 'black',
            'border-color': 'white'
        })
        
        # Highlight Last 30 Days row
        def highlight_seg_mtd(row):
            if 'Last 30 Days' in row['Period']:
                return [f'background-color: {segment_colors[segment]}40'] * len(row)
            else:
                return [''] * len(row)
        
        styled_seg_df = styled_seg_df.apply(highlight_seg_mtd, axis=1)
        
        st.dataframe(styled_seg_df, use_container_width=True, hide_index=True)
        
        # Quick metrics for this segment
        col1, col2, col3 = st.columns(3)
        
        with col1:
            mtd_vs_last_wr = ((seg_mtd['win_rate'] - seg_m1['win_rate']) / seg_m1['win_rate'] * 100) if seg_m1['win_rate'] > 0 else 0
            st.metric(
                f"{segment} L30D vs Last Month (Win Rate)",
                f"{seg_mtd['win_rate']:.1f}%",
                f"{mtd_vs_last_wr:+.1f}%"
            )

        with col2:
            mtd_vs_last_vol = ((seg_mtd['recovered'] - seg_m1['recovered']) / seg_m1['recovered'] * 100) if seg_m1['recovered'] > 0 else 0
            st.metric(
                f"{segment} L30D vs Last Month (Recovery)",
                f"${seg_mtd['recovered']/1000:.1f}K",
                f"{mtd_vs_last_vol:+.1f}%"
            )

        with col3:
            st.metric(f"{segment} Current Pending", f"{seg_mtd['pending']:,}")
        
        # Add expandable section for chain-level breakdown
        with st.expander(f"View {segment} chains breakdown"):
            # Get chain-level data for this segment
            chain_query = f"""
            WITH chain_segments AS (
                SELECT
                    chain,
                    COUNT(DISTINCT CONCAT(chain, b_name)) as location_count,
                    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT CONCAT(chain, b_name)) DESC) as rank_by_locations
                FROM `merchant_portal_export.chargeback_orders_enriched`
                WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    AND is_loop_enabled = true
                    AND loop_raised_timestamp IS NOT NULL
                    AND chain IS NOT NULL
                    AND chain != ''
                GROUP BY chain
            ),
            segmented_chains AS (
                SELECT
                    chain,
                    CASE
                        WHEN rank_by_locations <= 15 THEN 'P0'
                        WHEN rank_by_locations <= 40 THEN 'P1'
                        WHEN rank_by_locations <= 70 THEN 'P2'
                        WHEN rank_by_locations <= 132 THEN 'P3'
                        ELSE 'P4'
                    END as segment
                FROM chain_segments
            ),
            chain_monthly_data AS (
                SELECT 
                    sm.chain,
                    sm.slug,
                    cs.platform,
                    cs.chargeback_date,
                    cs.external_status,
                    COALESCE(cs.enabled_won_disputes, 0) as won_amount,
                    CASE
                        WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                            AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
                        THEN COALESCE(cs.enabled_customer_refunds, 0)
                        ELSE 0
                    END as settled_amount,
                    CASE
                        WHEN cs.external_status = 'ACCEPTED' THEN 'won'
                        WHEN cs.external_status = 'DENIED' THEN 'lost'
                        WHEN cs.external_status IN ('IN_PROGRESS', 'TO_BE_RAISED') THEN 'pending'
                        ELSE 'other'
                    END as dispute_status
                FROM `merchant_portal_export.chargeback_split_summary` cs
                JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
                JOIN segmented_chains sc ON sm.chain = sc.chain
                WHERE sc.segment = '{segment}'
            ),
            chain_metrics AS (
                SELECT 
                    chain,
                    -- MTD metrics
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND TRIM(platform) = 'Doordash' THEN slug END) as mtd_dd,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND TRIM(platform) = 'UberEats' THEN slug END) as mtd_ue,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND TRIM(platform) = 'Grubhub' THEN slug END) as mtd_gh,
                    COUNT(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' THEN 1 END) as mtd_disputes,
                    SUM(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND dispute_status = 'won' THEN 1 ELSE 0 END) as mtd_won,
                    SUM(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND dispute_status = 'lost' THEN 1 ELSE 0 END) as mtd_lost,
                    SUM(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' AND dispute_status = 'pending' THEN 1 ELSE 0 END) as mtd_pending,
                    SUM(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' THEN won_amount ELSE 0 END) as mtd_recovered,
                    SUM(CASE WHEN chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}' THEN settled_amount ELSE 0 END) as mtd_settled,
                    -- Last month metrics
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND TRIM(platform) = 'Doordash' THEN slug END) as m1_dd,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND TRIM(platform) = 'UberEats' THEN slug END) as m1_ue,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND TRIM(platform) = 'Grubhub' THEN slug END) as m1_gh,
                    COUNT(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' THEN 1 END) as m1_disputes,
                    SUM(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND dispute_status = 'won' THEN 1 ELSE 0 END) as m1_won,
                    SUM(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND dispute_status = 'lost' THEN 1 ELSE 0 END) as m1_lost,
                    SUM(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' AND dispute_status = 'pending' THEN 1 ELSE 0 END) as m1_pending,
                    SUM(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' THEN won_amount ELSE 0 END) as m1_recovered,
                    SUM(CASE WHEN chargeback_date BETWEEN '{last_month_start}' AND '{last_month_end}' THEN settled_amount ELSE 0 END) as m1_settled,
                    -- Month-2 metrics
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND TRIM(platform) = 'Doordash' THEN slug END) as m2_dd,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND TRIM(platform) = 'UberEats' THEN slug END) as m2_ue,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND TRIM(platform) = 'Grubhub' THEN slug END) as m2_gh,
                    COUNT(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' THEN 1 END) as m2_disputes,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND dispute_status = 'won' THEN 1 ELSE 0 END) as m2_won,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND dispute_status = 'lost' THEN 1 ELSE 0 END) as m2_lost,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' AND dispute_status = 'pending' THEN 1 ELSE 0 END) as m2_pending,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' THEN won_amount ELSE 0 END) as m2_recovered,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_2_start}' AND '{month_2_end}' THEN settled_amount ELSE 0 END) as m2_settled,
                    -- Month-3 metrics
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND TRIM(platform) = 'Doordash' THEN slug END) as m3_dd,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND TRIM(platform) = 'UberEats' THEN slug END) as m3_ue,
                    COUNT(DISTINCT CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND TRIM(platform) = 'Grubhub' THEN slug END) as m3_gh,
                    COUNT(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' THEN 1 END) as m3_disputes,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND dispute_status = 'won' THEN 1 ELSE 0 END) as m3_won,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND dispute_status = 'lost' THEN 1 ELSE 0 END) as m3_lost,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' AND dispute_status = 'pending' THEN 1 ELSE 0 END) as m3_pending,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' THEN won_amount ELSE 0 END) as m3_recovered,
                    SUM(CASE WHEN chargeback_date BETWEEN '{month_3_start}' AND '{month_3_end}' THEN settled_amount ELSE 0 END) as m3_settled
                FROM chain_monthly_data
                GROUP BY chain
            )
            SELECT 
                chain,
                -- MTD
                mtd_dd, mtd_ue, mtd_gh, mtd_disputes, mtd_won, mtd_lost, mtd_pending, mtd_recovered, mtd_settled,
                ROUND(SAFE_DIVIDE(mtd_recovered, NULLIF(mtd_settled, 0)) * 100, 2) as mtd_win_rate,
                -- M1
                m1_dd, m1_ue, m1_gh, m1_disputes, m1_won, m1_lost, m1_pending, m1_recovered, m1_settled,
                ROUND(SAFE_DIVIDE(m1_recovered, NULLIF(m1_settled, 0)) * 100, 2) as m1_win_rate,
                -- M2
                m2_dd, m2_ue, m2_gh, m2_disputes, m2_won, m2_lost, m2_pending, m2_recovered, m2_settled,
                ROUND(SAFE_DIVIDE(m2_recovered, NULLIF(m2_settled, 0)) * 100, 2) as m2_win_rate,
                -- M3
                m3_dd, m3_ue, m3_gh, m3_disputes, m3_won, m3_lost, m3_pending, m3_recovered, m3_settled,
                ROUND(SAFE_DIVIDE(m3_recovered, NULLIF(m3_settled, 0)) * 100, 2) as m3_win_rate
            FROM chain_metrics
            ORDER BY mtd_recovered DESC
            """
            
            try:
                chain_df = pandas_gbq.read_gbq(chain_query, project_id=PROJECT_ID, credentials=credentials, auth_local_webserver=False)
                
                if not chain_df.empty:
                    # Create tabs for each period
                    tab1, tab2, tab3, tab4 = st.tabs(["Last 30 Days",
                                                      last_month_start.strftime('%B'),
                                                      month_2_start.strftime('%B'),
                                                      month_3_start.strftime('%B')])
                    
                    with tab1:
                        mtd_display = pd.DataFrame({
                            'Chain': chain_df['chain'],
                            'DoorDash': chain_df['mtd_dd'],
                            'UberEats': chain_df['mtd_ue'],
                            'Grubhub': chain_df['mtd_gh'],
                            'Disputed': chain_df['mtd_disputes'],
                            'Won': chain_df['mtd_won'],
                            'Lost': chain_df['mtd_lost'],
                            'Pending': chain_df['mtd_pending'],
                            'Recovered': chain_df['mtd_recovered'].apply(lambda x: f"${x:,.0f}"),
                            'Win Rate': chain_df['mtd_win_rate'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%")
                        })
                        st.dataframe(mtd_display, use_container_width=True, hide_index=True)
                    
                    with tab2:
                        m1_display = pd.DataFrame({
                            'Chain': chain_df['chain'],
                            'DoorDash': chain_df['m1_dd'],
                            'UberEats': chain_df['m1_ue'],
                            'Grubhub': chain_df['m1_gh'],
                            'Disputed': chain_df['m1_disputes'],
                            'Won': chain_df['m1_won'],
                            'Lost': chain_df['m1_lost'],
                            'Pending': chain_df['m1_pending'],
                            'Recovered': chain_df['m1_recovered'].apply(lambda x: f"${x:,.0f}"),
                            'Win Rate': chain_df['m1_win_rate'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%")
                        })
                        st.dataframe(m1_display, use_container_width=True, hide_index=True)
                    
                    with tab3:
                        m2_display = pd.DataFrame({
                            'Chain': chain_df['chain'],
                            'DoorDash': chain_df['m2_dd'],
                            'UberEats': chain_df['m2_ue'],
                            'Grubhub': chain_df['m2_gh'],
                            'Disputed': chain_df['m2_disputes'],
                            'Won': chain_df['m2_won'],
                            'Lost': chain_df['m2_lost'],
                            'Pending': chain_df['m2_pending'],
                            'Recovered': chain_df['m2_recovered'].apply(lambda x: f"${x:,.0f}"),
                            'Win Rate': chain_df['m2_win_rate'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%")
                        })
                        st.dataframe(m2_display, use_container_width=True, hide_index=True)
                    
                    with tab4:
                        m3_display = pd.DataFrame({
                            'Chain': chain_df['chain'],
                            'DoorDash': chain_df['m3_dd'],
                            'UberEats': chain_df['m3_ue'],
                            'Grubhub': chain_df['m3_gh'],
                            'Disputed': chain_df['m3_disputes'],
                            'Won': chain_df['m3_won'],
                            'Lost': chain_df['m3_lost'],
                            'Pending': chain_df['m3_pending'],
                            'Recovered': chain_df['m3_recovered'].apply(lambda x: f"${x:,.0f}"),
                            'Win Rate': chain_df['m3_win_rate'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%")
                        })
                        st.dataframe(m3_display, use_container_width=True, hide_index=True)
                else:
                    st.info("No chain data available for this segment")
                    
            except Exception as e:
                st.error(f"Error loading chain breakdown: {e}")
        
        # Add chain movement expander
        with st.expander(f"View {segment} chain movement (entered/exited)"):
            # Filter chain movement data for this segment
            segment_movement = chains_movement[chains_movement['segment'] == segment] if not chains_movement.empty else pd.DataFrame()
            
            if not segment_movement.empty:
                # Separate entered and exited chains
                entered_chains = segment_movement[segment_movement['movement_type'] == 'entered']
                exited_chains = segment_movement[segment_movement['movement_type'] == 'exited']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**📈 Chains that Entered {segment}**")
                    if not entered_chains.empty:
                        entered_display = pd.DataFrame({
                            'Chain': entered_chains['chain'].values,
                            'Locations': entered_chains['location_count'].values
                        })
                        st.dataframe(entered_display, use_container_width=True, hide_index=True)
                    else:
                        st.info(f"No chains entered {segment} this month")
                
                with col2:
                    st.markdown(f"**📉 Chains that Exited {segment}**")
                    if not exited_chains.empty:
                        exited_display = pd.DataFrame({
                            'Chain': exited_chains['chain'].values,
                            'Locations': exited_chains['location_count'].values
                        })
                        st.dataframe(exited_display, use_container_width=True, hide_index=True)
                    else:
                        st.info(f"No chains exited {segment} this month")
            else:
                st.info(f"No chain movement detected for {segment}")
    else:
        st.info(f"No data available for {segment}")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data source: BigQuery")