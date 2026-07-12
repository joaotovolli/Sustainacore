#!/usr/bin/env python3
"""Verify TECH100 reconstruction invariants without changing Oracle."""

from __future__ import annotations
import argparse,datetime as dt,os,sys
from pathlib import Path
ROOT=Path(os.getenv('SC_IDX_REPO_ROOT') or Path(__file__).resolve().parents[2]);sys.path[:0]=[str(ROOT),str(ROOT/'app')]
from tools.oracle.env_bootstrap import load_env_files
from db_helper import get_connection

def main(argv=None)->int:
    p=argparse.ArgumentParser();p.add_argument('--tolerance',type=float,default=1e-6);p.add_argument('--allow-known-contamination',action='store_true');args=p.parse_args(argv)
    load_env_files()
    with get_connection() as conn:
        cur=conn.cursor()
        cur.execute("SELECT MAX(ABS(s.ret_1d-NVL(x.contrib,0))) FROM SC_IDX_STATS_DAILY s LEFT JOIN (SELECT trade_date,SUM(contribution) contrib FROM SC_IDX_CONTRIBUTION_DAILY GROUP BY trade_date) x ON x.trade_date=s.trade_date WHERE s.ret_1d IS NOT NULL")
        contrib=float(cur.fetchone()[0] or 0)
        cur.execute("SELECT MAX(ABS(market_value-shares*price_used)) FROM SC_IDX_CONSTITUENT_DAILY")
        mv=float(cur.fetchone()[0] or 0)
        cur.execute("SELECT ret_1d,contribution FROM SC_IDX_CONTRIBUTION_DAILY WHERE ticker='CRWD' AND trade_date=DATE '2026-07-02'")
        row=cur.fetchone();split_ret=float(row[0]) if row else 0;split_contrib=float(row[1]) if row else 0
        cur.execute("SELECT MAX(ABS(ret_1d)) FROM SC_IDX_STATS_DAILY")
        max_return=float(cur.fetchone()[0] or 0)
        maxima=[]
        for table in ('SC_IDX_LEVELS','SC_IDX_CONSTITUENT_DAILY','SC_IDX_CONTRIBUTION_DAILY','SC_IDX_STATS_DAILY','SC_IDX_PORTFOLIO_ANALYTICS_DAILY','SC_IDX_PORTFOLIO_POSITION_DAILY'):
            cur.execute(f'SELECT MAX(trade_date) FROM {table}');value=cur.fetchone()[0];maxima.append(value.date() if isinstance(value,dt.datetime) else value)
    print(f'contribution_max_residual={contrib:.12g}')
    print(f'market_value_max_residual={mv:.12g}')
    print(f'crwd_2026_07_02_return={split_ret:.12g}')
    print(f'crwd_2026_07_02_contribution={split_contrib:.12g}')
    print(f'max_abs_index_return={max_return:.12g}')
    print(f'calc_and_portfolio_freshness_equal={len(set(maxima))==1}')
    failed=contrib>args.tolerance or mv>args.tolerance or abs(split_ret)>.20 or len(set(maxima))!=1
    if failed and not args.allow_known_contamination:return 2
    return 0

if __name__=='__main__':raise SystemExit(main())
