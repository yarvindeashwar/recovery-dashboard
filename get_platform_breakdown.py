"""
Function to get platform-specific breakdown for weekly scorecard
"""

import pandas as pd
import pandas_gbq
import os

# Configuration
os.environ['GOOGLE_AUTH_DISABLE_METADATA_SERVER'] = 'True'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''
PROJECT_ID = 'arboreal-vision-339901'

def get_platform_breakdown(start_date, end_date, month_label):
    """Get platform-specific metrics"""
    
    query = f"""
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
        WHERE cs.chargeback_date BETWEEN '{start_date}' AND '{end_date}'
            AND sm.chain IS NOT NULL
            AND sm.chain != ''
    )
    SELECT 
        TRIM(platform) as platform,
        COUNT(DISTINCT b_name_id) as unique_locations,
        COUNT(DISTINCT slug) as slug_count,
        COUNT(*) as total_disputed,
        SUM(CASE WHEN dispute_status = 'won' THEN 1 ELSE 0 END) as disputes_won,
        SUM(CASE WHEN dispute_status = 'lost' THEN 1 ELSE 0 END) as disputes_lost,
        SUM(CASE WHEN dispute_status = 'pending' THEN 1 ELSE 0 END) as disputes_pending,
        SUM(won_amount) as total_recovered,
        SUM(settled_amount) as total_settled,
        ROUND(SAFE_DIVIDE(SUM(won_amount), NULLIF(SUM(settled_amount), 0)) * 100, 2) as win_rate
    FROM monthly_data
    GROUP BY platform
    ORDER BY platform
    """
    
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=None, auth_local_webserver=False)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Test
if __name__ == "__main__":
    from datetime import date
    today = date.today()
    current_month_start = date(today.year, today.month, 1)
    result = get_platform_breakdown(current_month_start, today, "MTD")
    print(result)