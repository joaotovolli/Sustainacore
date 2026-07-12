import datetime as dt
from pathlib import Path

import pytest

from tools.db_migrations import repair_sc_idx_corporate_actions as repair


def test_backup_names_are_deterministic_and_oracle_safe() -> None:
    name=repair.backup_name("20260712-1200","PC")
    assert name=="SCB_CA_202607121200_PC"
    assert len(name)<=30


def test_price_file_is_ticker_scoped(tmp_path: Path) -> None:
    path=tmp_path/'prices.csv'
    path.write_text('ticker,trade_date,adjusted_close\nAAA,2026-07-01,100.25\nBBB,2026-07-01,9\n',encoding='utf-8')
    assert repair.load_adjusted_prices(path,'AAA')=={dt.date(2026,7,1):100.25}


def test_price_file_rejects_nonpositive_values(tmp_path: Path) -> None:
    path=tmp_path/'prices.csv'
    path.write_text('ticker,trade_date,adjusted_close\nAAA,2026-07-01,0\n',encoding='utf-8')
    with pytest.raises(ValueError,match='invalid_adjusted_price'):
        repair.load_adjusted_prices(path,'AAA')


class _Cursor:
    def __init__(self): self.sql=[];self.result=[]
    def execute(self,sql,binds=None):
        self.sql.append(sql.strip().upper())
        if 'WITH P AS' in sql.upper(): self.result=[(2,dt.datetime(2025,11,4),dt.datetime(2026,7,2))]
        elif 'MAX(TRADE_DATE)' in sql.upper(): self.result=[(dt.date(2026,7,10),)]
        else:self.result=[(10,)]
        return self
    def fetchone(self):return self.result.pop(0)


class _Conn:
    def __init__(self):self.cur=_Cursor();self.commits=0
    def cursor(self):return self.cur
    def commit(self):self.commits+=1


def test_dry_run_performs_selects_only() -> None:
    conn=_Conn();args=repair.parser().parse_args([])
    end,lines=repair.dry_run(conn,args)
    assert end==dt.date(2026,7,10)
    assert all(sql.startswith(('SELECT','WITH')) for sql in conn.cur.sql)
    assert conn.commits==0
    assert 'mode=DRY_RUN' in lines
