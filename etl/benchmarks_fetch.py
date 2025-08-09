
import os, sys, datetime as dt
import pandas as pd

# pip install yfinance cx_Oracle
import yfinance as yf
import cx_Oracle

DB_DSN = os.getenv("DB_DSN")
DB_USER = os.getenv("DB_USER", "ESG")
DB_PASS = os.getenv("DB_PASS", "")

def db_conn():
    return cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN, encoding="UTF-8")

def load_benchmarks():
    with db_conn() as con:
        df = pd.read_sql("SELECT bench_id, code, symbol FROM ESG_BENCHMARKS", con)
    return df

def upsert_prices(bench_id, df):
    # df columns: Date, Adj Close
    rows = [(bench_id, d.date(), float(v)) for d, v in zip(df.index, df["Adj Close"]) if pd.notna(v)]
    if not rows:
        return 0
    sql = "MERGE INTO ESG_BENCHMARK_PRICES t USING (SELECT :1 bench_id, :2 trade_date, :3 close_adj FROM dual) s ON (t.bench_id=s.bench_id AND t.trade_date=s.trade_date) WHEN MATCHED THEN UPDATE SET t.close_adj=s.close_adj WHEN NOT MATCHED THEN INSERT (bench_id, trade_date, close_adj) VALUES (s.bench_id, s.trade_date, s.close_adj)"
    with db_conn() as con:
        cur = con.cursor()
        cur.executemany(sql, rows)
        con.commit()
    return len(rows)

def main():
    bens = load_benchmarks()
    start = "2010-01-01"
    end = dt.date.today().isoformat()
    total = 0
    for _, row in bens.iterrows():
        sym = row["SYMBOL"]
        data = yf.download(sym, start=start, end=end, progress=False, auto_adjust=False)
        if data.empty:
            print(f"No data for {row['CODE']} ({sym})")
            continue
        n = upsert_prices(int(row["BENCH_ID"]), data)
        total += n
        print(f"Upserted {n} rows for {row['CODE']}")
    print("Done. Total rows:", total)

if __name__ == "__main__":
    main()
