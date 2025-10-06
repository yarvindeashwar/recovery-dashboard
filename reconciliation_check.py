#!/usr/bin/env python3
"""
Reconciliation and Triangulation Checks for Weekly Scorecard Dashboard
This script verifies that all calculations are correct and segments sum to totals
"""

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import date, timedelta
import json
import sys

# Project configuration
PROJECT_ID = 'arboreal-vision-339901'

# Load credentials
def load_credentials():
    """Load BigQuery credentials from environment or service account"""
    try:
        # Try loading from default credentials
        from google.auth import default
        credentials, project = default()
        return credentials
    except:
        try:
            # Try loading from service account file
            credentials = service_account.Credentials.from_service_account_file(
                'service_account_key.json',
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            return credentials
        except:
            print("Warning: Using default credentials")
            return None

credentials = load_credentials()

# Date ranges
today = date.today()
last_30_start = today - timedelta(days=30)
last_30_end = today

print("=" * 80)
print("WEEKLY SCORECARD RECONCILIATION CHECK")
print(f"Date Range: {last_30_start} to {last_30_end}")
print("=" * 80)

# 1. Check Overall Totals vs Sum of Segments
print("\n1. CHECKING OVERALL TOTALS VS SEGMENT SUMS")
print("-" * 40)

overall_query = f"""
WITH monthly_data AS (
    SELECT
        sm.chain,
        cs.external_status,
        cs.error_category,
        COALESCE(cs.enabled_won_disputes, 0) as won_amount,
        CASE
            WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
            THEN COALESCE(cs.enabled_customer_refunds, 0)
            ELSE 0
        END as settled_amount
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
    WHERE cs.chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
        AND sm.chain IS NOT NULL
        AND sm.chain != ''
)
SELECT
    COUNT(*) as total_disputes,
    SUM(won_amount) as total_won,
    SUM(settled_amount) as total_settled,
    ROUND(SAFE_DIVIDE(SUM(won_amount), NULLIF(SUM(settled_amount), 0)) * 100, 2) as win_rate
FROM monthly_data
"""

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
        sc.segment,
        cs.external_status,
        cs.error_category,
        COALESCE(cs.enabled_won_disputes, 0) as won_amount,
        CASE
            WHEN cs.external_status IN ('ACCEPTED', 'DENIED')
                AND UPPER(COALESCE(cs.error_category, '')) LIKE '%INACCURATE%'
            THEN COALESCE(cs.enabled_customer_refunds, 0)
            ELSE 0
        END as settled_amount
    FROM `merchant_portal_export.chargeback_split_summary` cs
    JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
    JOIN segmented_chains sc ON sm.chain = sc.chain
    WHERE cs.chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
)
SELECT
    segment,
    COUNT(*) as disputes,
    SUM(won_amount) as won,
    SUM(settled_amount) as settled,
    ROUND(SAFE_DIVIDE(SUM(won_amount), NULLIF(SUM(settled_amount), 0)) * 100, 2) as win_rate
FROM monthly_data
GROUP BY segment
ORDER BY segment
"""

try:
    overall_df = pandas_gbq.read_gbq(overall_query, project_id=PROJECT_ID, credentials=credentials, location='US')
    segment_df = pandas_gbq.read_gbq(segment_query, project_id=PROJECT_ID, credentials=credentials, location='US')

    print("Overall Totals:")
    print(f"  Total Disputes: {overall_df['total_disputes'].iloc[0]:,}")
    print(f"  Total Won: ${overall_df['total_won'].iloc[0]:,.2f}")
    print(f"  Total Settled: ${overall_df['total_settled'].iloc[0]:,.2f}")
    print(f"  Win Rate: {overall_df['win_rate'].iloc[0]:.2f}%")

    print("\nSegment Breakdown:")
    for _, row in segment_df.iterrows():
        print(f"  {row['segment']}: {row['disputes']:,} disputes, ${row['won']:,.2f} won, Win Rate: {row['win_rate']:.2f}%")

    # Sum check
    segment_sum_disputes = segment_df['disputes'].sum()
    segment_sum_won = segment_df['won'].sum()
    segment_sum_settled = segment_df['settled'].sum()

    print("\nSegment Totals:")
    print(f"  Sum of Disputes: {segment_sum_disputes:,}")
    print(f"  Sum of Won: ${segment_sum_won:,.2f}")
    print(f"  Sum of Settled: ${segment_sum_settled:,.2f}")

    print("\nReconciliation:")
    dispute_diff = overall_df['total_disputes'].iloc[0] - segment_sum_disputes
    won_diff = overall_df['total_won'].iloc[0] - segment_sum_won
    settled_diff = overall_df['total_settled'].iloc[0] - segment_sum_settled

    print(f"  Dispute Difference: {dispute_diff:,} ({dispute_diff/overall_df['total_disputes'].iloc[0]*100:.2f}%)")
    print(f"  Won Difference: ${won_diff:.2f} ({won_diff/overall_df['total_won'].iloc[0]*100:.2f}%)")
    print(f"  Settled Difference: ${settled_diff:.2f} ({settled_diff/overall_df['total_settled'].iloc[0]*100:.2f}%)")

    if abs(dispute_diff) < 10 and abs(won_diff) < 100 and abs(settled_diff) < 100:
        print("  ✅ PASSED: Differences are within acceptable tolerance")
    else:
        print("  ❌ FAILED: Significant differences detected")

except Exception as e:
    print(f"Error running reconciliation: {e}")

# 2. Check Status Counts
print("\n2. CHECKING STATUS COUNTS")
print("-" * 40)

status_query = f"""
SELECT
    external_status,
    COUNT(*) as count,
    SUM(COALESCE(enabled_won_disputes, 0)) as won_amount,
    SUM(CASE
        WHEN external_status IN ('ACCEPTED', 'DENIED')
            AND UPPER(COALESCE(error_category, '')) LIKE '%INACCURATE%'
        THEN COALESCE(enabled_customer_refunds, 0)
        ELSE 0
    END) as settled_amount
FROM `merchant_portal_export.chargeback_split_summary`
WHERE chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
GROUP BY external_status
ORDER BY count DESC
"""

try:
    status_df = pandas_gbq.read_gbq(status_query, project_id=PROJECT_ID, credentials=credentials, location='US')

    print("Status Distribution:")
    for _, row in status_df.iterrows():
        print(f"  {row['external_status']}: {row['count']:,} disputes, ${row['won_amount']:,.2f} won")

    # Verify pending calculation
    pending_count = status_df[status_df['external_status'].isin(['IN_PROGRESS', 'TO_BE_RAISED'])]['count'].sum()
    expired_count = status_df[status_df['external_status'] == 'EXPIRED']['count'].sum() if 'EXPIRED' in status_df['external_status'].values else 0

    print(f"\nPending Calculation:")
    print(f"  IN_PROGRESS + TO_BE_RAISED = {pending_count:,}")
    print(f"  EXPIRED (not in pending) = {expired_count:,}")

except Exception as e:
    print(f"Error checking status counts: {e}")

# 3. Check Win Rate Formula
print("\n3. VERIFYING WIN RATE FORMULA")
print("-" * 40)

formula_query = f"""
WITH calculations AS (
    SELECT
        -- Numerator: ALL categories
        SUM(COALESCE(enabled_won_disputes, 0)) as total_won_all_categories,

        -- Denominator: INACCURATE only with ACCEPTED/DENIED
        SUM(CASE
            WHEN external_status IN ('ACCEPTED', 'DENIED')
                AND UPPER(COALESCE(error_category, '')) LIKE '%INACCURATE%'
            THEN COALESCE(enabled_customer_refunds, 0)
            ELSE 0
        END) as settled_inaccurate_only,

        -- Wrong calculation (for comparison)
        SUM(CASE
            WHEN external_status IN ('ACCEPTED', 'DENIED')
            THEN COALESCE(enabled_customer_refunds, 0)
            ELSE 0
        END) as settled_all_categories

    FROM `merchant_portal_export.chargeback_split_summary`
    WHERE chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
)
SELECT
    total_won_all_categories,
    settled_inaccurate_only,
    settled_all_categories,
    ROUND(SAFE_DIVIDE(total_won_all_categories, NULLIF(settled_inaccurate_only, 0)) * 100, 2) as correct_win_rate,
    ROUND(SAFE_DIVIDE(total_won_all_categories, NULLIF(settled_all_categories, 0)) * 100, 2) as wrong_win_rate
FROM calculations
"""

try:
    formula_df = pandas_gbq.read_gbq(formula_query, project_id=PROJECT_ID, credentials=credentials, location='US')

    print("Win Rate Formula Verification:")
    print(f"  Numerator (Won - ALL categories): ${formula_df['total_won_all_categories'].iloc[0]:,.2f}")
    print(f"  Denominator (Settled - INACCURATE only): ${formula_df['settled_inaccurate_only'].iloc[0]:,.2f}")
    print(f"  ✅ CORRECT Win Rate: {formula_df['correct_win_rate'].iloc[0]:.2f}%")
    print(f"  ")
    print(f"  If using ALL categories in denominator:")
    print(f"  Denominator (Settled - ALL categories): ${formula_df['settled_all_categories'].iloc[0]:,.2f}")
    print(f"  ❌ WRONG Win Rate: {formula_df['wrong_win_rate'].iloc[0]:.2f}%")

except Exception as e:
    print(f"Error verifying formula: {e}")

# 4. Check Platform Totals
print("\n4. CHECKING PLATFORM TOTALS")
print("-" * 40)

platform_query = f"""
SELECT
    platform,
    COUNT(*) as disputes,
    SUM(COALESCE(enabled_won_disputes, 0)) as won,
    SUM(CASE
        WHEN external_status IN ('ACCEPTED', 'DENIED')
            AND UPPER(COALESCE(error_category, '')) LIKE '%INACCURATE%'
        THEN COALESCE(enabled_customer_refunds, 0)
        ELSE 0
    END) as settled
FROM `merchant_portal_export.chargeback_split_summary`
WHERE chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
GROUP BY platform
"""

try:
    platform_df = pandas_gbq.read_gbq(platform_query, project_id=PROJECT_ID, credentials=credentials, location='US')

    print("Platform Distribution:")
    total_platform_disputes = 0
    total_platform_won = 0
    total_platform_settled = 0

    for _, row in platform_df.iterrows():
        print(f"  {row['platform']}: {row['disputes']:,} disputes, ${row['won']:,.2f} won")
        total_platform_disputes += row['disputes']
        total_platform_won += row['won']
        total_platform_settled += row['settled']

    print(f"\nPlatform Totals:")
    print(f"  Total Disputes: {total_platform_disputes:,}")
    print(f"  Total Won: ${total_platform_won:,.2f}")
    print(f"  Total Settled: ${total_platform_settled:,.2f}")

except Exception as e:
    print(f"Error checking platforms: {e}")

# 5. Check Location Counts
print("\n5. CHECKING LOCATION COUNTS")
print("-" * 40)

location_query1 = f"""
SELECT
    COUNT(DISTINCT CONCAT(chain, b_name)) as locations_from_enriched
FROM `merchant_portal_export.chargeback_orders_enriched`
WHERE order_date BETWEEN '{last_30_start}' AND '{last_30_end}'
    AND is_loop_enabled = true
    AND loop_raised_timestamp IS NOT NULL
"""

location_query2 = f"""
SELECT
    COUNT(DISTINCT sm.slug) as unique_slugs,
    COUNT(DISTINCT CONCAT(sm.chain, sm.b_name)) as unique_chain_bname
FROM `merchant_portal_export.chargeback_split_summary` cs
JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
WHERE cs.chargeback_date BETWEEN '{last_30_start}' AND '{last_30_end}'
"""

try:
    loc1_df = pandas_gbq.read_gbq(location_query1, project_id=PROJECT_ID, credentials=credentials, location='US')
    loc2_df = pandas_gbq.read_gbq(location_query2, project_id=PROJECT_ID, credentials=credentials, location='US')

    print("Location Count Methods:")
    print(f"  From chargeback_orders_enriched: {loc1_df['locations_from_enriched'].iloc[0]:,}")
    print(f"  From chargeback_split_summary (slugs): {loc2_df['unique_slugs'].iloc[0]:,}")
    print(f"  From chargeback_split_summary (chain+b_name): {loc2_df['unique_chain_bname'].iloc[0]:,}")

except Exception as e:
    print(f"Error checking locations: {e}")

print("\n" + "=" * 80)
print("RECONCILIATION CHECK COMPLETE")
print("=" * 80)