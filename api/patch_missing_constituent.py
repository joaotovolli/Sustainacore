# -*- coding: utf-8 -*-
"""
Created on Wed May 21 11:24:48 2025

@author: joaot
"""

import pandas as pd
from Connect_apexRI4X6 import get_connection

# Step 1: Connect to Oracle
conn = get_connection()
cursor = conn.cursor()

# Step 2: Define rebalance date and index
rebalance_date_str = '01-APR-2025'
index_name = 'TECH100_AI_GOV'
base_index_level = 1000
equal_weight = 0.04

# Step 3: Find missing ticker
query_missing = f"""
SELECT TICKER
FROM (
    SELECT TICKER, AIGES_COMPOSITE_AVERAGE
    FROM TECH11_AI_GOV_ETH_INDEX
    WHERE PORT_DATE = TO_DATE('{rebalance_date_str}', 'DD-MON-YYYY')
    ORDER BY AIGES_COMPOSITE_AVERAGE DESC
    FETCH FIRST 25 ROWS ONLY
)
MINUS
SELECT TICKER
FROM INDEX_CONSTITUENTS
WHERE INDEX_DATE = TO_DATE('{rebalance_date_str}', 'DD-MON-YYYY')
"""
df_missing = pd.read_sql(query_missing, con=conn)

if not df_missing.empty:
    missing_ticker = df_missing["TICKER"].iloc[0]

    # Step 4: Get company row and price
    query_constituent = f"""
    SELECT * FROM TECH11_AI_GOV_ETH_INDEX
    WHERE PORT_DATE = TO_DATE('{rebalance_date_str}', 'DD-MON-YYYY')
    AND TICKER = '{missing_ticker}'
    """
    row = pd.read_sql(query_constituent, con=conn).iloc[0]

    query_price = f"""
    SELECT ADJ_CLOSE FROM INDEX_STOCK_PRICES
    WHERE PRICE_DATE = TO_DATE('{rebalance_date_str}', 'DD-MON-YYYY')
    AND TICKER = '{missing_ticker}'
    """
    price_row = pd.read_sql(query_price, con=conn)

    if not price_row.empty:
        base_price = price_row["ADJ_CLOSE"].iloc[0]
        shares_held = (equal_weight * base_index_level) / base_price

        # Step 5: Insert into INDEX_CONSTITUENTS
        insert_sql = """
        INSERT INTO INDEX_CONSTITUENTS (
            INDEX_DATE, TICKER, INDEX_NAME, WEIGHT_PERCENT, BASE_PRICE,
            SHARES_HELD, AIGES_SCORE, INCLUDED_FLAG
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
        """
        cursor.execute(insert_sql, (
            pd.to_datetime(rebalance_date_str),
            missing_ticker,
            index_name,
            equal_weight * 100,
            round(base_price, 4),
            round(shares_held, 8),
            row["AIGES_COMPOSITE_AVERAGE"],
            'Y'
        ))
        conn.commit()

cursor.close()
conn.close()
