# 01_upload_prices.py  ────────────────────────────────────────────────────────
import pandas as pd
import datetime as dt
import yfinance as yf

from utils.log import logger
import utils.db as db
import config

# ─── helpers ────────────────────────────────────────────────────────────────
def _get_all_tickers() -> list[str]:
    sql = f"""
        SELECT DISTINCT TICKER
        FROM {config.TABLE_CONSTITUENTS}
    """
    return db.query_df(sql)["TICKER"].tolist()


def _get_last_price_date():
    sql = f"SELECT MAX(PRICE_DATE) AS LAST_DT FROM {config.TABLE_PRICES}"
    return db.query_df(sql).at[0, "LAST_DT"]  # may be None / NaT


def _download_prices(tickers: list[str],
                     start: dt.date,
                     end: dt.date) -> pd.DataFrame:
    """
    Download **Adj Close** prices.
    Yahoo’s `end` argument is *exclusive*, so pass `end+1day` from caller.
    Returns tidy df: PRICE_DATE | TICKER | PRICE
    """
    logger.info(
        "Downloading %d tickers from Yahoo Finance %s → %s",
        len(tickers), start, end - dt.timedelta(days=1)
    )

    raw = yf.download(
        tickers,
        start=start,
        end=end,
        actions=False,
        progress=False,
        auto_adjust=False       # <── keeps “Adj Close” column
    )["Adj Close"]

    # Ensure 2-D dataframe even for a single ticker
    if isinstance(raw, pd.Series):
        raw = raw.to_frame(tickers[0])

    tidy = (
        raw.reset_index()
           .melt(id_vars="Date", var_name="TICKER", value_name="PRICE")
           .dropna(subset=["PRICE"])
           .rename(columns={"Date": "PRICE_DATE"})
    )
    return tidy


def _upsert_prices(df: pd.DataFrame) -> None:
    rows = list(df[["PRICE_DATE", "TICKER", "PRICE"]].itertuples(index=False))
    sql = f"""
        INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({config.TABLE_PRICES}(PRICE_DATE,TICKER)) */
        INTO {config.TABLE_PRICES} (PRICE_DATE, TICKER, PRICE)
        VALUES (:1, :2, :3)
    """
    db.execute_many(sql, rows)
    logger.info("→ Upserted %d price rows", len(rows))


# ─── main entry ─────────────────────────────────────────────────────────────
def run() -> None:
    tickers = _get_all_tickers()
    if not tickers:
        logger.warning("No tickers found in constituents table – aborting Step 1.")
        return

    last_dt = _get_last_price_date()
    if last_dt is not None and not pd.isna(last_dt):
        start = pd.Timestamp(last_dt).date() + dt.timedelta(days=1)
    else:
        start = config.BASE_DATE - dt.timedelta(days=config.LOOKBACK_DAYS)

    end = dt.date.today()                       # inclusive-1 for Yahoo
    if start >= end:
        logger.info("No new dates to fetch – Step 1 done.")
        return

    df = _download_prices(tickers, start, end + dt.timedelta(days=1))
    if df.empty:
        logger.warning("Yahoo returned no data – check tickers.")
        return

    _upsert_prices(df)
    logger.info("Step 1 finished.")
