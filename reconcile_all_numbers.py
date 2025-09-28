"""
Comprehensive reconciliation of all dashboard numbers
Verifies calculations across overall, platform, and segment views
"""

import pandas as pd
import pandas_gbq
import os
from datetime import date
# Simple color codes for terminal output
class Colors:
    CYAN = '\033[96m'
    YELLOW = '\033[93m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'

# Configuration
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
PROJECT_ID = 'arboreal-vision-339901'

# Date setup
today = date.today()
current_month_start = date(today.year, today.month, 1)
current_month_end = today

print("="*100)
print(Colors.CYAN + "COMPREHENSIVE DATA RECONCILIATION - WEEKLY SCORECARD")
print(Colors.CYAN + f"Period: {current_month_start} to {current_month_end}")
print("="*100)

# 1. OVERALL TOTALS
print(Colors.YELLOW + "\n1. OVERALL TOTALS (Raw Data)")
print("-"*80)

overall_query = f"""
WITH monthly_data AS (
    SELECT 
        sm.chain,
        sm.slug,
        sm.b_name_id,
        cs.platform,
        cs.external_status,
        COALESCE(cs.enabled_won_disputes, 0) as won_amount,
        CASE 
            WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
            THEN COALESCE(cs.enabled_customer_refunds, 0)
            ELSE 0
        END as settled_amount,
        CASE 
            WHEN cs.external_status = 'ACCEPTED' THEN 'won'
            WHEN cs.external_status = 'DENIED' THEN 'lost'
            ELSE 'pending'
        END as dispute_status
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
    WHERE cs.chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND sm.chain IS NOT NULL
        AND sm.chain != ''
)
SELECT 
    COUNT(DISTINCT chain) as unique_chains,
    COUNT(DISTINCT b_name_id) as unique_locations,
    COUNT(DISTINCT slug) as unique_slugs,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Doordash' THEN slug END) as dd_slugs,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'UberEats' THEN slug END) as ue_slugs,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Grubhub' THEN slug END) as gh_slugs,
    COUNT(*) as total_disputes,
    SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as won_count,
    SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as lost_count,
    SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as pending_count,
    SUM(won_amount) as total_recovered,
    SUM(settled_amount) as total_settled,
    ROUND(SAFE_DIVIDE(SUM(won_amount), NULLIF(SUM(settled_amount), 0)) * 100, 2) as win_rate
FROM monthly_data
"""

overall_df = pandas_gbq.read_gbq(overall_query, project_id=PROJECT_ID, credentials=None, auth_local_webserver=False)
overall = overall_df.iloc[0]

print(f"Unique Chains: {overall['unique_chains']:,}")
print(f"Unique Locations (b_name_id): {overall['unique_locations']:,}")
print(f"Unique Slugs (all platforms): {overall['unique_slugs']:,}")
print(f"  - DoorDash slugs: {overall['dd_slugs']:,}")
print(f"  - UberEats slugs: {overall['ue_slugs']:,}")
print(f"  - Grubhub slugs: {overall['gh_slugs']:,}")
print(f"Total Disputes: {overall['total_disputes']:,}")
print(f"  - Won: {overall['won_count']:,}")
print(f"  - Lost: {overall['lost_count']:,}")
print(f"  - Pending: {overall['pending_count']:,}")
print(f"Total Recovered: ${overall['total_recovered']:,.2f}")
print(f"Total Settled: ${overall['total_settled']:,.2f}")
print(f"Win Rate: {overall['win_rate']:.2f}%")
print(f"$/Location: ${overall['total_recovered']/overall['unique_locations']:.2f}")

# 2. PLATFORM BREAKDOWN
print(Colors.YELLOW + "\n2. PLATFORM BREAKDOWN")
print("-"*80)

platform_query = f"""
WITH monthly_data AS (
    SELECT 
        cs.platform,
        sm.b_name_id,
        sm.slug,
        cs.external_status,
        COALESCE(cs.enabled_won_disputes, 0) as won_amount,
        CASE 
            WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
            THEN COALESCE(cs.enabled_customer_refunds, 0)
            ELSE 0
        END as settled_amount,
        CASE 
            WHEN cs.external_status = 'ACCEPTED' THEN 'won'
            WHEN cs.external_status = 'DENIED' THEN 'lost'
            ELSE 'pending'
        END as dispute_status
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
    WHERE cs.chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND sm.chain IS NOT NULL
        AND sm.chain != ''
)
SELECT 
    TRIM(platform) as platform,
    COUNT(DISTINCT b_name_id) as unique_locations,
    COUNT(DISTINCT slug) as slug_count,
    COUNT(*) as total_disputed,
    SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as won,
    SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as lost,
    SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as pending,
    SUM(won_amount) as recovered,
    SUM(settled_amount) as settled
FROM monthly_data
GROUP BY platform
ORDER BY platform
"""

platform_df = pandas_gbq.read_gbq(platform_query, project_id=PROJECT_ID, credentials=None, auth_local_webserver=False)

for _, row in platform_df.iterrows():
    print(f"\n{row['platform']}:")
    print(f"  Unique Locations (b_name_id): {row['unique_locations']:,}")
    print(f"  Slugs: {row['slug_count']:,}")
    print(f"  Disputes: {row['total_disputed']:,} (Won: {row['won']:,}, Lost: {row['lost']:,}, Pending: {row['pending']:,})")
    print(f"  Recovered: ${row['recovered']:,.2f}")
    print(f"  $/Location: ${row['recovered']/row['unique_locations']:.2f}")

# Platform totals
platform_totals = {
    'disputes': platform_df['total_disputed'].sum(),
    'won': platform_df['won'].sum(),
    'lost': platform_df['lost'].sum(),
    'pending': platform_df['pending'].sum(),
    'recovered': platform_df['recovered'].sum(),
    'settled': platform_df['settled'].sum()
}

# 3. SEGMENT BREAKDOWN
print(Colors.YELLOW + "\n3. SEGMENT BREAKDOWN (P0-P4)")
print("-"*80)

segment_query = f"""
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
        sm.b_name_id,
        sm.slug,
        cs.platform,
        sc.segment,
        cs.external_status,
        COALESCE(cs.enabled_won_disputes, 0) as won_amount,
        CASE 
            WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
            THEN COALESCE(cs.enabled_customer_refunds, 0)
            ELSE 0
        END as settled_amount,
        CASE 
            WHEN cs.external_status = 'ACCEPTED' THEN 'won'
            WHEN cs.external_status = 'DENIED' THEN 'lost'
            ELSE 'pending'
        END as dispute_status
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
    JOIN segmented_chains sc ON sm.chain = sc.chain
    WHERE cs.chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}'
)
SELECT 
    segment,
    COUNT(DISTINCT chain) as chains,
    COUNT(DISTINCT b_name_id) as unique_locations,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Doordash' THEN slug END) as dd_slugs,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'UberEats' THEN slug END) as ue_slugs,
    COUNT(DISTINCT CASE WHEN TRIM(platform) = 'Grubhub' THEN slug END) as gh_slugs,
    COUNT(*) as disputes,
    SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as won,
    SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as lost,
    SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as pending,
    SUM(won_amount) as recovered,
    SUM(settled_amount) as settled
FROM monthly_data
GROUP BY segment
ORDER BY segment
"""

segment_df = pandas_gbq.read_gbq(segment_query, project_id=PROJECT_ID, credentials=None, auth_local_webserver=False)

for _, row in segment_df.iterrows():
    print(f"\n{row['segment']}:")
    print(f"  Chains: {row['chains']:,}")
    print(f"  Unique Locations (b_name_id): {row['unique_locations']:,}")
    print(f"  Platform Slugs - DD: {row['dd_slugs']:,}, UE: {row['ue_slugs']:,}, GH: {row['gh_slugs']:,}")
    print(f"  Disputes: {row['disputes']:,} (Won: {row['won']:,}, Lost: {row['lost']:,}, Pending: {row['pending']:,})")
    print(f"  Recovered: ${row['recovered']:,.2f}")
    if row['unique_locations'] > 0:
        print(f"  $/Location: ${row['recovered']/row['unique_locations']:.2f}")

# Segment totals
segment_totals = {
    'chains': segment_df['chains'].sum(),
    'unique_locations': segment_df['unique_locations'].sum(),
    'dd_slugs': segment_df['dd_slugs'].sum(),
    'ue_slugs': segment_df['ue_slugs'].sum(),
    'gh_slugs': segment_df['gh_slugs'].sum(),
    'disputes': segment_df['disputes'].sum(),
    'won': segment_df['won'].sum(),
    'lost': segment_df['lost'].sum(),
    'pending': segment_df['pending'].sum(),
    'recovered': segment_df['recovered'].sum(),
    'settled': segment_df['settled'].sum()
}

# 4. RECONCILIATION
print(Colors.YELLOW + "\n4. RECONCILIATION CHECK")
print("="*80)

def check_match(name, overall_val, calculated_val, tolerance=1):
    """Check if values match within tolerance"""
    diff = abs(overall_val - calculated_val)
    match = diff <= tolerance
    status = Colors.GREEN + "✓ MATCH" if match else Colors.RED + f"✗ MISMATCH (diff: {diff:,.2f})"
    print(f"{name:30} Overall: {overall_val:15,.2f} | Calculated: {calculated_val:15,.2f} | {status}")
    return match

print(Colors.CYAN + "\nPlatform Totals vs Overall:")
print("-"*80)
all_match = True
all_match &= check_match("Total Disputes", overall['total_disputes'], platform_totals['disputes'])
all_match &= check_match("Won Count", overall['won_count'], platform_totals['won'])
all_match &= check_match("Lost Count", overall['lost_count'], platform_totals['lost'])
all_match &= check_match("Pending Count", overall['pending_count'], platform_totals['pending'])
all_match &= check_match("Total Recovered", overall['total_recovered'], platform_totals['recovered'])
all_match &= check_match("Total Settled", overall['total_settled'], platform_totals['settled'])

print(Colors.CYAN + "\nSegment Totals vs Overall:")
print("-"*80)
all_match &= check_match("Total Disputes", overall['total_disputes'], segment_totals['disputes'])
all_match &= check_match("Won Count", overall['won_count'], segment_totals['won'])
all_match &= check_match("Lost Count", overall['lost_count'], segment_totals['lost'])
all_match &= check_match("Pending Count", overall['pending_count'], segment_totals['pending'])
all_match &= check_match("Total Recovered", overall['total_recovered'], segment_totals['recovered'])
all_match &= check_match("Total Settled", overall['total_settled'], segment_totals['settled'])
all_match &= check_match("DD Slugs", overall['dd_slugs'], segment_totals['dd_slugs'])
all_match &= check_match("UE Slugs", overall['ue_slugs'], segment_totals['ue_slugs'])
all_match &= check_match("GH Slugs", overall['gh_slugs'], segment_totals['gh_slugs'])

# Note about chains - they won't sum correctly because of unique counting
print(Colors.YELLOW + f"\nNote: Segment chains sum ({segment_totals['chains']}) may differ from overall ({overall['unique_chains']}) due to unique counting across segments")

# 5. KEY METRICS VERIFICATION
print(Colors.YELLOW + "\n5. KEY METRICS VERIFICATION")
print("="*80)

# Win Rate Calculation
calculated_win_rate = (overall['total_recovered'] / overall['total_settled'] * 100) if overall['total_settled'] > 0 else 0
print(f"\nWin Rate Calculation:")
print(f"  Formula: (Total Recovered / Total Settled) * 100")
print(f"  = ({overall['total_recovered']:,.2f} / {overall['total_settled']:,.2f}) * 100")
print(f"  = {calculated_win_rate:.2f}%")
print(f"  Stored Win Rate: {overall['win_rate']:.2f}%")
win_rate_match = abs(calculated_win_rate - overall['win_rate']) < 0.01
print(f"  Status: {Colors.GREEN + 'MATCH' if win_rate_match else Colors.RED + 'MISMATCH'}")

# Location vs Slug Verification
print(f"\nLocation Count Verification:")
print(f"  Unique b_name_id (physical locations): {overall['unique_locations']:,}")
print(f"  Total unique slugs: {overall['unique_slugs']:,}")
print(f"  Ratio (slugs/location): {overall['unique_slugs']/overall['unique_locations']:.2f}")
print(f"  This means on average each physical location has ~{overall['unique_slugs']/overall['unique_locations']:.1f} platform presences")

# Platform slug sum check
platform_slug_sum = overall['dd_slugs'] + overall['ue_slugs'] + overall['gh_slugs']
print(f"\nPlatform Slug Verification:")
print(f"  Sum of platform slugs: {platform_slug_sum:,}")
print(f"  Total unique slugs: {overall['unique_slugs']:,}")
print(f"  Difference: {overall['unique_slugs'] - platform_slug_sum:,}")
print(f"  Note: Difference indicates slugs appearing on multiple platforms")

# 6. FINAL STATUS
print("\n" + "="*100)
if all_match and win_rate_match:
    print(Colors.GREEN + "✅ ALL RECONCILIATION CHECKS PASSED - DATA IS CONSISTENT!")
else:
    print(Colors.RED + "⚠️ SOME RECONCILIATION CHECKS FAILED - REVIEW DATA!")
print("="*100)

# 7. SUMMARY INSIGHTS
print(Colors.CYAN + "\n7. SUMMARY INSIGHTS")
print("="*80)
print(f"• Total unique physical locations: {overall['unique_locations']:,}")
print(f"• Average platforms per location: {overall['unique_slugs']/overall['unique_locations']:.2f}")
print(f"• Recovery efficiency: ${overall['total_recovered']/overall['unique_locations']:.2f} per location")
print(f"• Overall win rate: {overall['win_rate']:.2f}%")
print(f"• Pending resolution: {overall['pending_count']:,} disputes ({overall['pending_count']/overall['total_disputes']*100:.1f}%)")