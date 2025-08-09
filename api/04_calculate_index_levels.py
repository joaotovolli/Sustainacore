import pandas as pd, numpy as np, datetime
from utils.log import logger
import utils.db as db
import config

def run():
    # Load price and weight data
    price_sql = f"""SELECT p.PRICE_DATE, p.TICKER, p.PRICE
                     FROM {config.TABLE_PRICES} p"""
    prices = db.query_df(price_sql)
    if prices.empty:
        logger.warning("No prices yet – Step 4 aborted.")
        return
        weight_sql = f"""SELECT INDEX_DATE,TICKER,WEIGHT_PERCENT
                          FROM {config.TABLE_CONSTITUENTS}"""
                      FROM {config.TABLE_CONSTITUENTS}"""
    weights = db.query_df(weight_sql)
    if weights.empty:
        logger.warning("No constituents weights – Step 4 aborted.")
        return
    # Expand weights daily
    weights = weights.sort_values('INDEX_DATE')
    weights['NEXT_REBAL'] = weights.groupby('TICKER')['INDEX_DATE'].shift(-1)
    all_dates = prices['PRICE_DATE'].unique()
    dfs = []
    for _, row in weights.iterrows():
        start = row['INDEX_DATE']
        end   = row['NEXT_REBAL'] - datetime.timedelta(days=1) if pd.notna(row['NEXT_REBAL']) else prices['PRICE_DATE'].max()
        mask = (all_dates>=start) & (all_dates<=end)
        dts = pd.Series(all_dates[mask])
        tmp = pd.DataFrame({
            'PRICE_DATE': dts,
            'TICKER': row['TICKER'],
            'WEIGHT_PERCENT': row['WEIGHT_PERCENT']
        })
        dfs.append(tmp)
    daily_w = pd.concat(dfs, ignore_index=True)
    # Merge with prices
    merged = prices.merge(daily_w, on=['PRICE_DATE','TICKER'], how='inner')
    # Calculate level
    merged['WPRICE'] = merged['PRICE']*merged['WEIGHT_PERCENT']
    daily = merged.groupby('PRICE_DATE')['WPRICE'].sum().sort_index()
    daily = daily.reset_index().rename(columns={'WPRICE':'NUMERATOR'})
    # Calculate divisor
    base_num = daily.loc[daily['PRICE_DATE']==config.BASE_DATE, 'NUMERATOR'].iloc[0]
    divisor = base_num / config.BASE_VALUE
    daily['INDEX_LEVEL'] = daily['NUMERATOR']/divisor
    # Re‑adjust divisor at every rebalance
    rebals = weights['INDEX_DATE'].unique()
    for r in sorted(rebals[rebals>config.BASE_DATE]):
        num_r = daily.loc[daily['PRICE_DATE']==r, 'NUMERATOR'].iloc[0]
        lvl_prev = daily.loc[daily['PRICE_DATE']==r, 'INDEX_LEVEL'].iloc[0]
        divisor *= num_r / lvl_prev
        daily.loc[daily['PRICE_DATE']>=r, 'INDEX_LEVEL'] = daily.loc[daily['PRICE_DATE']>=r, 'NUMERATOR']/divisor
    # Upsert
    rows = [tuple(x) for x in daily[['PRICE_DATE','INDEX_LEVEL']].itertuples(index=False)]
    sql = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX({config.TABLE_LEVELS}(INDEX_DATE)) */
             INTO {config.TABLE_LEVELS} (INDEX_DATE,INDEX_LEVEL)
             VALUES (:1,:2)"""
    db.execute_many(sql, rows)
    logger.info("Inserted/updated %d index level rows", len(rows))