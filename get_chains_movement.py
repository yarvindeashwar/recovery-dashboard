"""
Function to track chains entering and exiting Recover
"""

import pandas as pd
import pandas_gbq
import os
from datetime import date, timedelta

# Configuration
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
PROJECT_ID = 'arboreal-vision-339901'

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
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=None, auth_local_webserver=False)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Test
if __name__ == "__main__":
    today = date.today()
    current_month_start = date(today.year, today.month, 1)
    
    # Previous month
    if today.month == 1:
        prev_month_start = date(today.year - 1, 12, 1)
        prev_month_end = date(today.year - 1, 12, 31)
    else:
        prev_month_start = date(today.year, today.month - 1, 1)
        # Last day of previous month
        prev_month_end = current_month_start - timedelta(days=1)
    
    result = get_chains_movement(current_month_start, today, prev_month_start, prev_month_end)
    print(result)