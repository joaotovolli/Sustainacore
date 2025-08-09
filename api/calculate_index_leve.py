# -*- coding: utf-8 -*-
"""
Created on Wed May 21 11:21:34 2025

@author: joaot
"""

import pandas as pd
from Connect_apexRI4X6 import get_connection

# Step 1: Connect to Oracle
conn = get_connection()
cursor = conn.cursor()

# Step 2: Get all price dates
query_price_dates = """
SELECT DISTINCT PRICE_DATE FROM INDEX_STOCK_PRICES ORDER BY PRICE_DATE
"""
df_price_dates = pd.read_sql(query_price_dates, con=conn)
price_dates = pd.to_datetime(df_price_dates["PRICE_DATE"])

# Step 3: Get all rebalance dates
query_rebalance_dates = """
SELECT DISTINCT INDEX_DATE FROM INDEX_CONSTITUENTS ORDER BY INDEX_DATE
"""
df_rebalance_dates = pd.read_sql(query_rebalance_dates, con=conn)
rebalance_dates = pd.to_datetime(df_rebalance_dates["INDEX_DATE"])

# Step 4: Build date-to-rebalance mapping
rebalance_dates = sorted(rebalance_dates)
date_to_rebalance = {}
current_rebalance = None
for date in price_dates:
    for rebalance in reversed(rebalance_dates):
        if date >= rebalance:
            current_rebalance = rebalance
            break
    if current_rebalance:
        date_to_rebalance[date] = current_rebalance

# Step 5: Prepare inserts
records_levels = []
records_constituents = []

for date, rebalance_date in date_to_rebalance.items():
    # Get constituents
    q_const = f"""
    SELECT TICKER, SHARES_HELD FROM INDEX_CONSTITUENTS
    WHERE INDEX_DATE = TO_DATE('{rebalance_date.strftime("%Y-%m-%d")}', 'YYYY-MM-DD')
    """
    df_const = pd.read_sql(q_const, con=conn)

    # Get prices
    tickers = "','".join(df_const["TICKER"])
    q_price = f"""
    SELECT TICKER, ADJ_CLOSE FROM INDEX_STOCK_PRICES
    WHERE PRICE_DATE = TO_DATE('{date.strftime("%Y-%m-%d")}', 'YYYY-MM-DD')
    AND TICKER IN ('{tickers}')
    """
    df_price = pd.read_sql(q_price, con=conn)

    # Calculate contributions
    merged = pd.merge(df_const, df_price, on="TICKER", how="inner")
    if merged.empty:
        continue
    merged["CONTRIBUTION"] = merged["SHARES_HELD"] * merged["ADJ_CLOSE"]
    index_level = merged["CONTRIBUTION"].sum()

    # Add index level record
    records_levels.append((
        date,
        'TECH100_AI_GOV',
        round(index_level, 4),
        'Y' if date == rebalance_date else 'N',
        round(index_level, 8),  # First rebalance sets divisor = level
        None
    ))

    # Add per-constituent record
    for _, row in merged.iterrows():
        records_constituents.append((
            date,
            row["TICKER"],
            'TECH100_AI_GOV',
            None,  # CLOSE_PRICE not used
            round(row["ADJ_CLOSE"], 4),
            round(row["CONTRIBUTION"], 6),
            round(index_level, 4)
        ))

# Step 6: Insert into Oracle
sql_levels = """
INSERT INTO INDEX_LEVELS (
    INDEX_DATE, INDEX_NAME, INDEX_LEVEL, REBALANCE_FLAG, DIVISOR, INDEX_NOTE
) VALUES (:1, :2, :3, :4, :5, :6)
"""
sql_constituents = """
INSERT INTO INDEX_CONSTITUENT_PRICES (
    PRICE_DATE, TICKER, INDEX_NAME, CLOSE_PRICE, ADJ_CLOSE_PRICE, CONTRIBUTION, INDEX_LEVEL
) VALUES (:1, :2, :3, :4, :5, :6, :7)
"""

cursor.executemany(sql_levels, records_levels)
cursor.executemany(sql_constituents, records_constituents)
conn.commit()
cursor.close()
conn.close()
