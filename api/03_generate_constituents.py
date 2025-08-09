from utils.log import logger
import utils.db as db
import config
import pandas as pd, datetime

def _latest_rebalance():
    sql = f"""SELECT MAX(INDEX_DATE) D FROM {config.TABLE_CONSTITUENTS}"""
    df = db.query_df(sql)
    return df.at[0, 'D']
def _insert_equal_weights(rebal_date):
    sql_const = f"""SELECT TICKER FROM {config.TABLE_CONSTITUENTS}
                    WHERE INDEX_DATE = :d"""
    existing = db.query_df(sql_const, {'d': rebal_date})
    if not existing.empty and existing['TICKER'].nunique()==25:
        logger.info("Constituent set for %s already has 25 tickers – skipping Step 3.", rebal_date)
        return
    # Get tickers from some selection logic – for now graze previous set
    prev_sql = f"""SELECT TICKER
                    FROM {config.TABLE_CONSTITUENTS}
                    WHERE INDEX_DATE = (SELECT MAX(INDEX_DATE)
                                             FROM {config.TABLE_CONSTITUENTS}
                                             WHERE INDEX_DATE < :d)"""
    prev = db.query_df(prev_sql, {'d': rebal_date})
    tickers = prev['TICKER'].tolist()[:25]  # take first 25 if more
    weight = round(1/25,6)
    rows = [(rebal_date, t, weight) for t in tickers]
    ins = f"""INSERT INTO {config.TABLE_CONSTITUENTS}(INDEX_DATE,TICKER,WEIGHT_PERCENT)
              VALUES (:1,:2,:3)"""
    db.execute_many(ins, rows)
    logger.info("Inserted %d equal‑weight rows for rebalance %s", len(rows), rebal_date)

def run():
    latest = _latest_rebalance()
    if not latest:
        logger.warning("No rebalance dates – Step 3 skipped.")
        return
    _insert_equal_weights(latest)