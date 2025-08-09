import pandas as pd
from Connect_apexRI4X6 import get_connection

# Step 1: Connect to Oracle
conn = get_connection()
cursor = conn.cursor()

# Step 2: Get the latest date in INDEX_LEVELS
query_latest_date = "SELECT MAX(INDEX_DATE) AS LAST_DATE FROM INDEX_LEVELS"
df_latest = pd.read_sql(query_latest_date, con=conn)
last_index_date = df_latest["LAST_DATE"].iloc[0] if not df_latest.empty else None

# Step 3: Get available price dates
query_price_dates = """
SELECT DISTINCT PRICE_DATE FROM INDEX_STOCK_PRICES
ORDER BY PRICE_DATE
"""
df_price_dates = pd.read_sql(query_price_dates, con=conn)
all_price_dates = pd.to_datetime(df_price_dates["PRICE_DATE"].tolist())

# Step 4: Get all unique rebalance dates
query_rebalance_dates = """
SELECT DISTINCT PORT_DATE FROM TECH11_AI_GOV_ETH_INDEX
ORDER BY PORT_DATE
"""
df_rebalance_dates = pd.read_sql(query_rebalance_dates, con=conn)
rebalance_dates = pd.to_datetime(df_rebalance_dates["PORT_DATE"].tolist())

# Step 5: Loop through each rebalance period
base_index_level = 1000
equal_weight = 0.04
inserted_dates = []

for rebalance_date in rebalance_dates:
    # Skip if already inserted
    check_query = f"""
    SELECT COUNT(*) FROM INDEX_CONSTITUENTS
    WHERE INDEX_DATE = TO_DATE('{rebalance_date.strftime("%Y-%m-%d")}', 'YYYY-MM-DD')
    """
    if pd.read_sql(check_query, con=conn).iloc[0, 0] > 0:
        continue

    # Get top 25 tickers
    query_top_constituents = f"""
    SELECT * FROM TECH11_AI_GOV_ETH_INDEX
    WHERE PORT_DATE = TO_DATE('{rebalance_date.strftime("%Y-%m-%d")}', 'YYYY-MM-DD')
    ORDER BY AIGES_COMPOSITE_AVERAGE DESC
    FETCH FIRST 25 ROWS ONLY
    """
    df_constituents = pd.read_sql(query_top_constituents, con=conn)
    tickers = df_constituents["TICKER"].tolist()

    # Get base prices from INDEX_STOCK_PRICES
    query_prices = f"""
    SELECT TICKER, ADJ_CLOSE
    FROM INDEX_STOCK_PRICES
    WHERE PRICE_DATE = TO_DATE('{rebalance_date.strftime("%Y-%m-%d")}', 'YYYY-MM-DD')
    AND TICKER IN ({','.join("'" + t + "'" for t in tickers)})
    """
    df_prices = pd.read_sql(query_prices, con=conn)
    price_map = dict(zip(df_prices["TICKER"], df_prices["ADJ_CLOSE"]))

    # Prepare rows to insert
    records = []
    for _, row in df_constituents.iterrows():
        ticker = row["TICKER"]
        base_price = price_map.get(ticker)
        if base_price and base_price > 0:
            shares_held = (equal_weight * base_index_level) / base_price
            records.append((
                rebalance_date,
                ticker,
                'TECH100_AI_GOV',
                equal_weight * 100,
                round(base_price, 4),
                round(shares_held, 8),
                row["AIGES_COMPOSITE_AVERAGE"],
                'Y'
            ))

    # Insert into INDEX_CONSTITUENTS
    if records:
        insert_sql = """
        INSERT INTO INDEX_CONSTITUENTS (
            INDEX_DATE, TICKER, INDEX_NAME, WEIGHT_PERCENT, BASE_PRICE,
            SHARES_HELD, AIGES_SCORE, INCLUDED_FLAG
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
        """
        cursor.executemany(insert_sql, records)
        conn.commit()
        inserted_dates.append(rebalance_date.strftime("%Y-%m-%d"))

# Cleanup
cursor.close()
conn.close()

# Show result
print("Rebalance dates inserted:")
print(inserted_dates)

