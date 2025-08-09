import pandas as pd, datetime
from utils.log import logger
import utils.db as db
import config

def run():
    # Get trading calendar from existing price dates
    calendar_sql = f"""SELECT DISTINCT PRICE_DATE FROM {config.TABLE_PRICES}"""
    cal = db.query_df(calendar_sql)['PRICE_DATE'].sort_values()
    if cal.empty:
        logger.warning("No price data yet – skipping Step 2.")
        return
    # Get list of current constituents (latest rebalance <= today)
    tick_sql = f"""
        SELECT TICKER
        FROM {config.TABLE_CONSTITUENTS}
        WHERE INDEX_DATE = (
            SELECT MAX(INDEX_DATE)
            FROM {config.TABLE_CONSTITUENTS}
            WHERE INDEX_DATE <= SYSDATE
        )
    """
                    FROM {config.TABLE_CONSTITUENTS}
                    WHERE INDEX_DATE = (SELECT MAX(INDEX_DATE)
                                            FROM {config.TABLE_CONSTITUENTS}
                                            WHERE INDEX_DATE <= SYSDATE)"""
    ticks = db.query_df(tick_sql)["TICKER"].tolist()
    if not ticks:
        logger.warning("No constituents found – skipping Step 2.")
        return
    # All combinations
    full = pd.MultiIndex.from_product([cal, ticks], names=['PRICE_DATE','TICKER']).to_frame(index=False)
    # Existing
    exist_sql = f"""SELECT PRICE_DATE,TICKER FROM {config.TABLE_PRICES}"""
    exist = db.query_df(exist_sql)
    missing = full.merge(exist, on=['PRICE_DATE','TICKER'], how='left', indicator=True)
    missing = missing[missing['_merge']=='left_only'].drop(columns=['_merge'])
    if missing.empty:
        logger.info("No gaps to fill – Step 2 done.")
        return
    logger.info("Filling %d missing price cells", len(missing))
    # Forward fill prices per ticker
    price_sql = f"""SELECT PRICE_DATE,TICKER,PRICE
                     FROM {config.TABLE_PRICES}"""
    prices = db.query_df(price_sql)
    prices = prices.sort_values(['TICKER','PRICE_DATE'])
    prices['PRICE_FF'] = prices.groupby('TICKER')['PRICE'].ffill()
    merged = missing.merge(prices[['PRICE_DATE','TICKER','PRICE_FF']],
                           on=['PRICE_DATE','TICKER'], how='left')
    merged = merged.rename(columns={'PRICE_FF':'PRICE'}).dropna()
    if merged.empty:
        logger.warning("Could not forward fill any gaps – Step 2 finished.")
        return
    rows = [tuple(r) for r in merged[['PRICE_DATE','TICKER','PRICE']].itertuples(index=False)]
    sql = f"""INSERT INTO {config.TABLE_PRICES} (PRICE_DATE,TICKER,PRICE)
              VALUES (:1,:2,:3)"""
    db.execute_many(sql, rows)
    logger.info("Inserted %d forward‑filled rows", len(rows))