"""
Test script to verify location count calculation is fixed
"""

import pandas as pd
import pandas_gbq
from datetime import date
import os

# Configuration
PROJECT_ID = 'arboreal-vision-339901'

# Disable metadata server for local development
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

def test_location_count():
    """Test the location count with the correct query"""

    # Test date range
    start_date = '2025-08-01'
    end_date = '2025-09-30'

    # The correct query provided by the user
    query = f"""
    SELECT
        COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    """

    print(f"Testing location count for period: {start_date} to {end_date}")
    print("Using query:")
    print(query)
    print("\n" + "="*50)

    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        if not df.empty:
            location_count = df.iloc[0]['location_count']
            print(f"✓ Location count: {location_count:,}")
            return location_count
        else:
            print("✗ No data returned")
            return None
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def compare_old_vs_new():
    """Compare old method vs new method"""

    start_date = '2025-09-01'
    end_date = '2025-09-30'

    # Old method (counting b_name_id from chargeback_split_summary)
    old_query = f"""
    WITH monthly_data AS (
        SELECT
            sm.b_name_id
        FROM `merchant_portal_export.chargeback_split_summary` cs
        JOIN `restaurant_aggregate_metrics.slug_am_mapping` sm ON cs.slug = sm.slug
        WHERE cs.chargeback_date BETWEEN '{start_date}' AND '{end_date}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
    )
    SELECT COUNT(DISTINCT b_name_id) as location_count
    FROM monthly_data
    """

    # New method (your correct query)
    new_query = f"""
    SELECT
        COUNT(DISTINCT CONCAT(chain, b_name)) as location_count
    FROM `merchant_portal_export.chargeback_orders_enriched`
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        AND is_loop_enabled = true
        AND loop_raised_timestamp IS NOT NULL
    """

    print(f"\nComparing methods for September 2025:")
    print("="*50)

    # Run old query
    print("\n1. OLD METHOD (b_name_id from chargeback_split_summary):")
    try:
        df_old = pandas_gbq.read_gbq(old_query, project_id=PROJECT_ID)
        old_count = df_old.iloc[0]['location_count'] if not df_old.empty else 0
        print(f"   Count: {old_count:,}")
    except Exception as e:
        print(f"   Error: {e}")
        old_count = None

    # Run new query
    print("\n2. NEW METHOD (chain + b_name from chargeback_orders_enriched):")
    try:
        df_new = pandas_gbq.read_gbq(new_query, project_id=PROJECT_ID)
        new_count = df_new.iloc[0]['location_count'] if not df_new.empty else 0
        print(f"   Count: {new_count:,}")
    except Exception as e:
        print(f"   Error: {e}")
        new_count = None

    # Show difference
    if old_count is not None and new_count is not None:
        diff = new_count - old_count
        pct_change = (diff / old_count * 100) if old_count > 0 else 0
        print(f"\n3. DIFFERENCE:")
        print(f"   Absolute: {diff:+,}")
        print(f"   Percentage: {pct_change:+.1f}%")

if __name__ == "__main__":
    print("LOCATION COUNT TEST")
    print("="*50)

    # Test the new location count
    test_location_count()

    # Compare old vs new methods
    compare_old_vs_new()

    print("\n" + "="*50)
    print("Test complete!")