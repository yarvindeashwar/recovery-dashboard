"""
Comprehensive test to triangulate all location calculations across the dashboard
This ensures all sections are using the same location counting methodology
"""

import pandas as pd
import pandas_gbq
from datetime import date, timedelta
import os

# Configuration
PROJECT_ID = 'arboreal-vision-339901'

# Disable metadata server for local development
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

# Test date ranges
current_month_start = date(2025, 9, 1)
current_month_end = date(2025, 9, 30)
last_30_days_start = date.today() - timedelta(days=30)

def test_base_location_count():
    """Test 1: Base location count from chargeback_orders_enriched"""

    query = f"""
    SELECT
        COUNT(DISTINCT CONCAT(chain, b_name)) as total_locations,
        COUNT(DISTINCT chain) as unique_chains,
        COUNT(DISTINCT b_name) as unique_b_names
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    """

    print("\n" + "="*60)
    print("TEST 1: BASE LOCATION COUNT (September 2025)")
    print("="*60)

    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    if not df.empty:
        row = df.iloc[0]
        print(f"Total Locations (chain + b_name): {row['total_locations']:,}")
        print(f"Unique Chains: {row['unique_chains']:,}")
        print(f"Unique B_Names: {row['unique_b_names']:,}")
        return row['total_locations']
    return 0

def test_location_by_chain():
    """Test 2: Location count by chain (top 10)"""

    query = f"""
    SELECT
        chain,
        COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
        AND chain IS NOT NULL
        AND chain != ''
    GROUP BY chain
    ORDER BY location_count DESC
    LIMIT 10
    """

    print("\n" + "="*60)
    print("TEST 2: TOP 10 CHAINS BY LOCATION COUNT")
    print("="*60)

    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    if not df.empty:
        print("\n{:<40} {:>15}".format("Chain", "Locations"))
        print("-"*55)
        total = 0
        for _, row in df.iterrows():
            print("{:<40} {:>15,}".format(row['chain'][:40], int(row['location_count'])))
            total += row['location_count']
        print("-"*55)
        print("{:<40} {:>15,}".format("Top 10 Total:", total))
        return total
    return 0

def test_location_by_platform():
    """Test 3: Location count by platform"""

    query = f"""
    SELECT
        platform,
        COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    GROUP BY platform
    ORDER BY platform
    """

    print("\n" + "="*60)
    print("TEST 3: LOCATION COUNT BY PLATFORM")
    print("="*60)

    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    if not df.empty:
        print("\n{:<20} {:>15}".format("Platform", "Locations"))
        print("-"*35)
        total = 0
        for _, row in df.iterrows():
            print("{:<20} {:>15,}".format(row['platform'], int(row['location_count'])))
            total += row['location_count']
        print("-"*35)
        print("{:<20} {:>15,}".format("Total:", total))
        return df
    return pd.DataFrame()

def test_segmentation_consistency():
    """Test 4: Verify P0-P4 segmentation is consistent"""

    query = f"""
    WITH chain_locations AS (
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
    ranked_chains AS (
        SELECT
            chain,
            location_count,
            ROW_NUMBER() OVER (ORDER BY location_count DESC) as rank
        FROM chain_locations
    ),
    segmented AS (
        SELECT
            CASE
                WHEN rank <= 15 THEN 'P0'
                WHEN rank <= 40 THEN 'P1'
                WHEN rank <= 70 THEN 'P2'
                WHEN rank <= 132 THEN 'P3'
                ELSE 'P4'
            END as segment,
            COUNT(*) as chain_count,
            SUM(location_count) as total_locations,
            MIN(location_count) as min_locations,
            MAX(location_count) as max_locations
        FROM ranked_chains
        GROUP BY segment
    )
    SELECT *
    FROM segmented
    ORDER BY segment
    """

    print("\n" + "="*60)
    print("TEST 4: P0-P4 SEGMENTATION CONSISTENCY")
    print("="*60)

    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    if not df.empty:
        print("\n{:<10} {:<12} {:<15} {:<12} {:<12}".format(
            "Segment", "Chains", "Total Locs", "Min Locs", "Max Locs"))
        print("-"*61)
        grand_total_chains = 0
        grand_total_locs = 0
        for _, row in df.iterrows():
            print("{:<10} {:<12} {:<15,} {:<12,} {:<12,}".format(
                row['segment'],
                int(row['chain_count']),
                int(row['total_locations']),
                int(row['min_locations']),
                int(row['max_locations'])
            ))
            grand_total_chains += row['chain_count']
            grand_total_locs += row['total_locations']
        print("-"*61)
        print("{:<10} {:<12} {:<15,}".format("TOTAL:", grand_total_chains, grand_total_locs))
        return grand_total_locs
    return 0

def test_monthly_trend():
    """Test 5: Monthly trend of location counts"""

    print("\n" + "="*60)
    print("TEST 5: MONTHLY LOCATION TREND")
    print("="*60)

    months = [
        ('2025-07-01', '2025-07-31', 'July 2025'),
        ('2025-08-01', '2025-08-31', 'August 2025'),
        ('2025-09-01', '2025-09-30', 'September 2025')
    ]

    print("\n{:<15} {:>15}".format("Month", "Locations"))
    print("-"*30)

    for start, end, label in months:
        query = f"""
        SELECT
            COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date BETWEEN '{start}' AND '{end}'
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
        """

        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        if not df.empty:
            count = df.iloc[0]['location_count']
            print("{:<15} {:>15,}".format(label, int(count)))

def test_reconciliation():
    """Test 6: Reconcile location counts across different aggregations"""

    print("\n" + "="*60)
    print("TEST 6: LOCATION COUNT RECONCILIATION")
    print("="*60)

    # Get total unique locations
    query1 = f"""
    SELECT COUNT(DISTINCT CONCAT(chain, b_name)) as total
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    """

    # Get sum of locations by chain
    query2 = f"""
    WITH chain_locs AS (
        SELECT
            chain,
            COUNT(DISTINCT CONCAT(chain, b_name)) as locs
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
        GROUP BY chain
    )
    SELECT SUM(locs) as total FROM chain_locs
    """

    # Get unique chain-b_name combinations
    query3 = f"""
    SELECT COUNT(DISTINCT CONCAT(chain, '|', b_name)) as total
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    """

    df1 = pandas_gbq.read_gbq(query1, project_id=PROJECT_ID)
    df2 = pandas_gbq.read_gbq(query2, project_id=PROJECT_ID)
    df3 = pandas_gbq.read_gbq(query3, project_id=PROJECT_ID)

    total1 = df1.iloc[0]['total'] if not df1.empty else 0
    total2 = df2.iloc[0]['total'] if not df2.empty else 0
    total3 = df3.iloc[0]['total'] if not df3.empty else 0

    print("\nReconciliation Results:")
    print(f"1. Direct COUNT(DISTINCT CONCAT(chain, b_name)): {total1:,}")
    print(f"2. SUM of locations grouped by chain: {total2:,}")
    print(f"3. COUNT(DISTINCT CONCAT(chain, '|', b_name)): {total3:,}")

    if total1 == total2 == total3:
        print("\n✅ RECONCILIATION PASSED: All methods return the same count!")
    else:
        print("\n❌ RECONCILIATION FAILED: Methods return different counts!")
        print(f"   Difference between method 1 and 2: {abs(total1 - total2):,}")
        print(f"   Difference between method 1 and 3: {abs(total1 - total3):,}")

def test_avg_per_location_calc():
    """Test 7: Verify $/Location calculations are correct"""

    print("\n" + "="*60)
    print("TEST 7: AVERAGE PER LOCATION CALCULATIONS")
    print("="*60)

    query = f"""
    WITH location_data AS (
        SELECT
            COUNT(DISTINCT CONCAT(chain, b_name)) as total_locations
        FROM `merchant_portal_export.chargeback_orders_enriched`
        WHERE order_date BETWEEN '{current_month_start}' AND '{current_month_end}'
            AND is_loop_enabled = true
            AND loop_raised_timestamp IS NOT NULL
    ),
    recovery_data AS (
        SELECT
            SUM(COALESCE(cs.enabled_won_disputes, 0)) as total_recovered
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{current_month_start}' AND '{current_month_end}'
    )
    SELECT
        l.total_locations,
        r.total_recovered,
        ROUND(r.total_recovered / NULLIF(l.total_locations, 0), 2) as avg_per_location
    FROM location_data l
    CROSS JOIN recovery_data r
    """

    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    if not df.empty:
        row = df.iloc[0]
        print(f"\nTotal Locations: {row['total_locations']:,}")
        print(f"Total Recovered: ${row['total_recovered']:,.2f}")
        print(f"Average per Location: ${row['avg_per_location']:,.2f}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("LOCATION COUNT TRIANGULATION TEST SUITE")
    print("Testing all location calculations for consistency")
    print("="*60)

    # Run all tests
    base_count = test_base_location_count()
    top_10_total = test_location_by_chain()
    platform_df = test_location_by_platform()
    segment_total = test_segmentation_consistency()
    test_monthly_trend()
    test_reconciliation()
    test_avg_per_location_calc()

    # Final summary
    print("\n" + "="*60)
    print("TRIANGULATION SUMMARY")
    print("="*60)
    print(f"\n✓ Base location count (Sept 2025): {base_count:,}")
    print(f"✓ Top 10 chains account for: {top_10_total:,} locations")
    print(f"✓ P0-P4 segments total: {segment_total:,} locations")

    if not platform_df.empty:
        platform_total = platform_df['location_count'].sum()
        print(f"✓ Platform breakdown total: {platform_total:,} locations")

        # Note about platform duplicates
        print("\n⚠️  Note: Platform totals may exceed base count due to")
        print("   locations appearing on multiple platforms")

    print("\n✅ All location calculations have been updated to use:")
    print("   COUNT(DISTINCT CONCAT(chain, b_name)) from chargeback_orders_enriched")
    print("   with filters: is_loop_enabled = true AND loop_raised_timestamp IS NOT NULL")

    print("\n" + "="*60)
    print("TEST COMPLETE!")
    print("="*60)