import pandas as pd
import yfinance as yf
from Connect_apexRI4X6 import get_connection
from datetime import datetime

# Step 1: Connect to Oracle
conn = get_connection()
cursor = conn.cursor()

# Step 2: Load tickers from the view
query = 'SELECT TICKER FROM "WKSP_ESGAPEX"."TECH11_AI_GOV_ETH_PERF_00"'
tickers_df = pd.read_sql(query, con=conn)
tickers = tickers_df["TICKER"].dropna().unique().tolist()

# Step 3: Ensure tickers exist in INDEX_COMPANIES
existing_tickers_df = pd.read_sql("SELECT TICKER FROM INDEX_COMPANIES", con=conn)
existing_tickers = existing_tickers_df["TICKER"].tolist()

missing_tickers = [t for t in tickers if t not in existing_tickers]

if missing_tickers:
    insert_sql = "INSERT INTO INDEX_COMPANIES (TICKER) VALUES (:1)"
    cursor.executemany(insert_sql, [(t,) for t in missing_tickers])
    conn.commit()
    print(f"Inserted {len(missing_tickers)} missing tickers into INDEX_COMPANIES.")

# Step 4: Download data from Yahoo Finance
start_date = "2024-12-31"
yf_data = yf.download(tickers, start=start_date, auto_adjust=False, group_by='ticker')

# Step 5: Prepare price data
records = []

for ticker in tickers:
    try:
        df = yf_data[ticker].copy()
        df["TICKER"] = ticker
        df["PRICE_DATE"] = pd.to_datetime(df.index)
        df = df.rename(columns={
            "Open": "OPEN",
            "High": "HIGH",
            "Low": "LOW",
            "Close": "CLOSE",
            "Adj Close": "ADJ_CLOSE",
            "Volume": "VOLUME"
        })
        df = df[["TICKER", "PRICE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

        for row in df.itertuples(index=False):
            records.append((
                row.TICKER,
                row.PRICE_DATE.to_pydatetime(),
                float(row.OPEN),
                float(row.HIGH),
                float(row.LOW),
                float(row.CLOSE),
                float(row.ADJ_CLOSE),
                int(row.VOLUME) if not pd.isna(row.VOLUME) else None
            ))

    except Exception as e:
        print(f"Skipping {ticker}: {e}")

# Step 6: Upload to Oracle
if records:
    insert_sql = """
    INSERT INTO INDEX_STOCK_PRICES (
        TICKER, PRICE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8
    )
    """
    cursor.executemany(insert_sql, records)
    conn.commit()
    print(f"Uploaded {len(records)} rows to INDEX_STOCK_PRICES.")
else:
    print("No data to upload.")

# Cleanup
cursor.close()
conn.close()
