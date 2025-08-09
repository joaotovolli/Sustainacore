import pandas as pd, numpy as np
from utils.log import logger
import utils.db as db
import config

def run():
    # Index returns
    lvl_sql = f"""SELECT INDEX_DATE, INDEX_LEVEL FROM {config.TABLE_LEVELS}"""
    lvl = db.query_df(lvl_sql).sort_values('INDEX_DATE')
    if lvl.empty:
        logger.warning("No index levels – Step 5 aborted.")
        return
    lvl['RET'] = lvl['INDEX_LEVEL'].pct_change()
    idx_rows = [tuple(x) for x in lvl.dropna()[['INDEX_DATE','RET']].itertuples(index=False)]
    # Stock returns
    price_sql = f"""SELECT PRICE_DATE,TICKER,PRICE
                     FROM {config.TABLE_PRICES}"""
    pr = db.query_df(price_sql)
    pr = pr.sort_values(['TICKER','PRICE_DATE'])
    pr['RET'] = pr.groupby('TICKER')['PRICE'].pct_change()
    pr = pr.dropna(subset=['RET'])
    stock_rows = [tuple(x) for x in pr[['PRICE_DATE','TICKER','RET']].itertuples(index=False)]

    # Upsert
    sql_idx = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({config.TABLE_RETURNS}(INDEX_DATE,TICKER)) */
               INTO {config.TABLE_RETURNS} (INDEX_DATE,TICKER,DAILY_RETURN)
               VALUES (:1,'_INDEX_',:2)"""
    db.execute_many(sql_idx, idx_rows)
    sql_stk = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({config.TABLE_RETURNS}(INDEX_DATE,TICKER)) */
               INTO {config.TABLE_RETURNS} (INDEX_DATE,TICKER,DAILY_RETURN)
               VALUES (:1,:2,:3)"""
    db.execute_many(sql_stk, stock_rows)
    logger.info("Inserted/updated %d index + %d stock returns", len(idx_rows), len(stock_rows))