#!/usr/bin/env python3
"""Back up, repair, rebuild, or roll back TECH100 corporate-action history."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "app"
for path in (ROOT, APP):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files
from db_helper import get_connection
from index_engine.corporate_actions import earliest_material_change

BASE_DATE = dt.date(2025, 1, 2)
BACKUP_OBJECTS = {
    "PC": "SC_IDX_PRICES_CANON",
    "HD": "SC_IDX_HOLDINGS",
    "DV": "SC_IDX_DIVISOR",
    "LV": "SC_IDX_LEVELS",
    "CD": "SC_IDX_CONSTITUENT_DAILY",
    "CO": "SC_IDX_CONTRIBUTION_DAILY",
    "ST": "SC_IDX_STATS_DAILY",
    "PA": "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
    "PP": "SC_IDX_PORTFOLIO_POSITION_DAILY",
    "PO": "SC_IDX_PORTFOLIO_OPT_INPUTS",
    "CA": "SC_IDX_CORPORATE_ACTIONS",
}


def backup_name(tag: str, code: str) -> str:
    clean = re.sub(r"[^A-Z0-9]", "", tag.upper())[:12]
    if not clean:
        raise ValueError("invalid_backup_tag")
    return f"SCB_CA_{clean}_{code}"


def load_adjusted_prices(path: Path, ticker: str) -> dict[dt.date, float]:
    rows: dict[dt.date, float] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"ticker", "trade_date", "adjusted_close"}
        if not reader.fieldnames or not required.issubset({x.lower() for x in reader.fieldnames}):
            raise ValueError("price_csv_columns_required:ticker,trade_date,adjusted_close")
        keys = {x.lower(): x for x in reader.fieldnames}
        for row in reader:
            if str(row[keys["ticker"]]).strip().upper() != ticker:
                continue
            day = dt.date.fromisoformat(str(row[keys["trade_date"]])[:10])
            value = float(row[keys["adjusted_close"]])
            if value <= 0:
                raise ValueError(f"invalid_adjusted_price:{day.isoformat()}")
            rows[day] = value
    if not rows:
        raise ValueError("no_adjusted_prices_for_ticker")
    return rows


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Repair TECH100 history after a confirmed corporate action")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--rollback-tag")
    p.add_argument("--ticker")
    p.add_argument("--effective-date", type=dt.date.fromisoformat)
    p.add_argument("--ratio", type=float)
    p.add_argument("--action-type", choices=("FORWARD_SPLIT", "REVERSE_SPLIT"), default="FORWARD_SPLIT")
    p.add_argument("--source-type", default="REGULATORY_FILING")
    p.add_argument("--source-reference")
    p.add_argument("--adjusted-price-csv", type=Path)
    p.add_argument("--refresh-adjusted-history", action="store_true")
    p.add_argument("--max-refresh-calls", type=int, default=500)
    p.add_argument("--start", type=dt.date.fromisoformat, default=BASE_DATE)
    p.add_argument("--end", type=dt.date.fromisoformat)
    p.add_argument("--backup-tag")
    p.add_argument("--report", type=Path)
    return p


def _date_column(table: str) -> str | None:
    return {
        "SC_IDX_PRICES_CANON": "TRADE_DATE", "SC_IDX_HOLDINGS": "REBALANCE_DATE",
        "SC_IDX_DIVISOR": "EFFECTIVE_DATE", "SC_IDX_LEVELS": "TRADE_DATE",
        "SC_IDX_CONSTITUENT_DAILY": "TRADE_DATE", "SC_IDX_CONTRIBUTION_DAILY": "TRADE_DATE",
        "SC_IDX_STATS_DAILY": "TRADE_DATE", "SC_IDX_PORTFOLIO_ANALYTICS_DAILY": "TRADE_DATE",
        "SC_IDX_PORTFOLIO_POSITION_DAILY": "TRADE_DATE", "SC_IDX_PORTFOLIO_OPT_INPUTS": "TRADE_DATE",
        "SC_IDX_CORPORATE_ACTIONS": "EFFECTIVE_DATE",
    }.get(table)


def _write_report(path: Path | None, lines: list[str]) -> None:
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def dry_run(conn, args: argparse.Namespace) -> tuple[dt.date, list[str]]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS")
    max_day = args.end or cur.fetchone()[0]
    if isinstance(max_day, dt.datetime): max_day = max_day.date()
    cur.execute("""
      WITH p AS (
        SELECT ticker,trade_date,canon_adj_close_px,
               LAG(canon_adj_close_px) OVER(PARTITION BY ticker ORDER BY trade_date) previous_px
        FROM SC_IDX_PRICES_CANON
      )
      SELECT COUNT(*),MIN(trade_date),MAX(trade_date) FROM p
      WHERE previous_px>0 AND ABS(canon_adj_close_px/previous_px-1)>.20
    """)
    candidate_count,candidate_min,candidate_max=cur.fetchone()
    lines=["mode=DRY_RUN",f"rebuild_start={args.start.isoformat()}",f"rebuild_end={max_day.isoformat()}",
           f"price_candidates={candidate_count}",f"candidate_min={candidate_min.date().isoformat() if candidate_min else ''}",
           f"candidate_max={candidate_max.date().isoformat() if candidate_max else ''}"]
    for table in BACKUP_OBJECTS.values():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            lines.append(f"{table}_rows={cur.fetchone()[0]}")
        except Exception as exc:
            if "ORA-00942" in str(exc): lines.append(f"{table}_rows=UNAVAILABLE")
            else: raise
    return max_day,lines


def create_backups(conn, tag: str, start: dt.date, end: dt.date) -> list[str]:
    cur=conn.cursor();created=[]
    for code,table in BACKUP_OBJECTS.items():
        name=backup_name(tag,code); dc=_date_column(table)
        try:
            sql=f"CREATE TABLE {name} AS SELECT * FROM {table}"
            binds={}
            if dc:
                sql+=f" WHERE {dc} BETWEEN :start_date AND :end_date";binds={"start_date":start,"end_date":end}
            cur.execute(sql,binds);created.append(name)
        except Exception as exc:
            if "ORA-00955" in str(exc): created.append(name)
            elif "ORA-00942" in str(exc) and table=="SC_IDX_CORPORATE_ACTIONS": continue
            else: raise
    return created


def ensure_action_schema(conn) -> None:
    blocks=[];current=[]
    for line in (ROOT/'oracle_scripts/sc_idx_corporate_actions.sql').read_text(encoding='utf-8').splitlines():
        if line.strip()=='/':
            if current: blocks.append('\n'.join(current));current=[]
        else: current.append(line)
    if current: blocks.append('\n'.join(current))
    cur=conn.cursor()
    for block in blocks: cur.execute(block)


def apply_prices(conn, ticker: str, prices: dict[dt.date,float]) -> int:
    cur=conn.cursor();rows=[{"ticker":ticker,"trade_date":d,"price":p} for d,p in sorted(prices.items())]
    cur.executemany("UPDATE SC_IDX_PRICES_CANON SET canon_adj_close_px=:price,computed_at=SYSTIMESTAMP WHERE ticker=:ticker AND trade_date=:trade_date",rows)
    return cur.rowcount


def upsert_action(conn,args:argparse.Namespace,start:dt.date,run_id:str)->None:
    conn.cursor().execute("""
      MERGE INTO SC_IDX_CORPORATE_ACTIONS d
      USING (SELECT 'TECH100' index_code,:ticker ticker,:action_type action_type,:effective_date effective_date FROM dual) s
      ON (d.index_code=s.index_code AND d.ticker=s.ticker AND d.action_type=s.action_type AND d.effective_date=s.effective_date)
      WHEN MATCHED THEN UPDATE SET ratio=:ratio,confirmation_status='CONFIRMED',source_type=:source_type,source_reference=:source_reference,
        confirmed_at=NVL(d.confirmed_at,SYSTIMESTAMP),affected_start_date=:affected_start_date,processing_run_id=:run_id
      WHEN NOT MATCHED THEN INSERT (index_code,ticker,action_type,effective_date,ratio,confirmation_status,source_type,source_reference,confirmed_at,processing_method,applied_at,affected_start_date,processing_run_id)
        VALUES ('TECH100',:ticker,:action_type,:effective_date,:ratio,'CONFIRMED',:source_type,:source_reference,SYSTIMESTAMP,'REFRESH_ADJUSTED_HISTORY',NULL,:affected_start_date,:run_id)
    """,{"ticker":args.ticker,"action_type":args.action_type,"effective_date":args.effective_date,"ratio":args.ratio,
          "source_type":args.source_type,"source_reference":args.source_reference,"affected_start_date":start,"run_id":run_id})


def mark_action_applied(conn,args:argparse.Namespace,run_id:str)->None:
    conn.cursor().execute("UPDATE SC_IDX_CORPORATE_ACTIONS SET confirmation_status='APPLIED',applied_at=SYSTIMESTAMP WHERE index_code='TECH100' AND ticker=:ticker AND action_type=:action_type AND effective_date=:effective_date AND processing_run_id=:run_id",
                          {"ticker":args.ticker,"action_type":args.action_type,"effective_date":args.effective_date,"run_id":run_id})
    conn.commit()


def rebuild(start:dt.date,end:dt.date)->None:
    subprocess.run([sys.executable,str(ROOT/'tools/index_engine/calc_index.py'),'--start',start.isoformat(),'--end',end.isoformat(),'--rebuild','--strict','--no-preflight-self-heal'],check=True,cwd=ROOT)
    subprocess.run([sys.executable,str(ROOT/'tools/index_engine/build_portfolio_analytics.py'),'--start',start.isoformat(),'--end',end.isoformat()],check=True,cwd=ROOT)


def rollback(conn,tag:str,start:dt.date,end:dt.date)->list[str]:
    cur=conn.cursor();restored=[]
    for code,table in reversed(list(BACKUP_OBJECTS.items())):
        name=backup_name(tag,code);dc=_date_column(table)
        try:
            if dc: cur.execute(f"DELETE FROM {table} WHERE {dc} BETWEEN :s AND :e",{"s":start,"e":end})
            else: cur.execute(f"DELETE FROM {table}")
            cur.execute(f"INSERT INTO {table} SELECT * FROM {name}");restored.append(table)
        except Exception as exc:
            if "ORA-00942" in str(exc): continue
            raise
    conn.commit();return restored


def main(argv:list[str]|None=None)->int:
    args=parser().parse_args(argv);load_env_files()
    with get_connection() as conn:
        max_day,lines=dry_run(conn,args)
        if args.rollback_tag:
            restored=rollback(conn,args.rollback_tag,args.start,max_day);lines += ["mode=ROLLBACK",f"restored_objects={len(restored)}"]
        elif args.apply:
            if not all((args.ticker,args.effective_date,args.ratio,args.source_reference)):
                raise SystemExit("apply_requires_ticker_effective_date_ratio_source_reference")
            if bool(args.adjusted_price_csv)==bool(args.refresh_adjusted_history):
                raise SystemExit("apply_requires_exactly_one_adjusted_history_source")
            ticker=args.ticker.strip().upper();args.ticker=ticker
            tag=args.backup_tag or dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M')
            ensure_action_schema(conn)
            created=create_backups(conn,tag,args.start,max_day);run_id=f"ca-{uuid.uuid4()}"
            if args.refresh_adjusted_history:
                subprocess.run([sys.executable,str(ROOT/'tools/index_engine/ingest_prices.py'),'--start',args.start.isoformat(),'--end',args.effective_date.isoformat(),'--backfill','--tickers',ticker,'--max-provider-calls',str(args.max_refresh_calls)],check=True,cwd=ROOT)
                prices={}
            else:
                prices=load_adjusted_prices(args.adjusted_price_csv,ticker)
                cur=conn.cursor();cur.execute("SELECT trade_date,canon_adj_close_px FROM SC_IDX_PRICES_CANON WHERE ticker=:t",{"t":ticker})
                stored={(d.date() if isinstance(d,dt.datetime) else d):float(v) for d,v in cur if v is not None}
                changed=earliest_material_change(stored,prices)
                if changed is None: raise SystemExit("no_material_adjusted_price_change")
                args.start=min(args.start,changed);apply_prices(conn,ticker,prices)
            upsert_action(conn,args,args.start,run_id);conn.commit();rebuild(args.start,max_day);mark_action_applied(conn,args,run_id)
            lines += ["mode=APPLY",f"backup_tag={tag}",f"backup_objects={len(created)}",f"rebuild_start={args.start.isoformat()}"]
        _write_report(args.report,lines)
        for line in lines: print(line)
    return 0


if __name__=='__main__': raise SystemExit(main())
