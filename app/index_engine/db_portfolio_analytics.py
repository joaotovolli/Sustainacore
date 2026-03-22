"""Oracle helpers for TECH100 portfolio analytics tables."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Iterable

from db_helper import get_connection

from .portfolio_analytics_v1 import (
    MetadataRow,
    OfficialDailyRow,
    OfficialPositionRow,
    PriceRow,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = REPO_ROOT / "oracle_scripts" / "sc_idx_portfolio_analytics_v1.sql"
DROP_DDL_PATH = REPO_ROOT / "oracle_scripts" / "sc_idx_portfolio_analytics_v1_drop.sql"


def _coerce_date(value: object) -> _dt.date | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return None


def _is_missing_object_error(exc: Exception) -> bool:
    text = str(exc)
    if "ORA-00942" in text or "ORA-04043" in text:
        return True
    if exc.args:
        first = exc.args[0]
        code = getattr(first, "code", None)
        if code in {942, 4043}:
            return True
    return False


def _script_blocks(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: list[str] = []
    current: list[str] = []
    for line in lines:
        if line.strip() == "/":
            block = "\n".join(current).strip()
            if block:
                blocks.append(block)
            current = []
            continue
        current.append(line)
    tail = "\n".join(current).strip()
    if tail:
        blocks.append(tail)
    return blocks


def apply_ddl(path: Path = DDL_PATH) -> None:
    blocks = _script_blocks(path)
    with get_connection() as conn:
        cur = conn.cursor()
        for block in blocks:
            cur.execute(block)
        conn.commit()


def fetch_trade_date_bounds() -> tuple[_dt.date | None, _dt.date | None]:
    sql = (
        "SELECT MIN(trade_date), MAX(trade_date) "
        "FROM SC_IDX_LEVELS WHERE index_code = :index_code"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": "TECH100"})
        row = cur.fetchone()
        if not row:
            return None, None
        return _coerce_date(row[0]), _coerce_date(row[1])


def fetch_official_daily_rows() -> list[OfficialDailyRow]:
    sql = (
        "SELECT l.trade_date, l.level_tr, s.ret_1d, s.ret_5d, s.ret_20d, s.vol_20d, "
        "       s.max_drawdown_252d, s.n_constituents, s.n_imputed, s.top5_weight, s.herfindahl "
        "FROM SC_IDX_LEVELS l "
        "LEFT JOIN SC_IDX_STATS_DAILY s ON s.trade_date = l.trade_date "
        "WHERE l.index_code = :index_code "
        "ORDER BY l.trade_date"
    )
    rows: list[OfficialDailyRow] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": "TECH100"})
        for record in cur.fetchall():
            trade_date = _coerce_date(record[0])
            level_tr = float(record[1]) if record[1] is not None else None
            if trade_date is None or level_tr is None:
                continue
            rows.append(
                OfficialDailyRow(
                    trade_date=trade_date,
                    level_tr=level_tr,
                    ret_1d=float(record[2]) if record[2] is not None else None,
                    ret_5d=float(record[3]) if record[3] is not None else None,
                    ret_20d=float(record[4]) if record[4] is not None else None,
                    vol_20d=float(record[5]) if record[5] is not None else None,
                    max_drawdown_252d=float(record[6]) if record[6] is not None else None,
                    n_constituents=int(record[7]) if record[7] is not None else None,
                    n_imputed=int(record[8]) if record[8] is not None else None,
                    top5_weight=float(record[9]) if record[9] is not None else None,
                    herfindahl=float(record[10]) if record[10] is not None else None,
                )
            )
    return rows


def fetch_official_position_rows() -> list[OfficialPositionRow]:
    sql = (
        "SELECT c.trade_date, c.rebalance_date, c.ticker, c.weight, c.price_quality, "
        "       d.ret_1d, d.contribution "
        "FROM SC_IDX_CONSTITUENT_DAILY c "
        "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
        "  ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
        "ORDER BY c.trade_date, c.ticker"
    )
    rows: list[OfficialPositionRow] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        for record in cur.fetchall():
            trade_date = _coerce_date(record[0])
            rebalance_date = _coerce_date(record[1])
            ticker = str(record[2]).strip().upper() if record[2] is not None else ""
            weight = float(record[3]) if record[3] is not None else None
            if trade_date is None or rebalance_date is None or not ticker or weight is None:
                continue
            rows.append(
                OfficialPositionRow(
                    trade_date=trade_date,
                    rebalance_date=rebalance_date,
                    ticker=ticker,
                    weight=weight,
                    price_quality=str(record[4]).strip().upper() if record[4] is not None else None,
                    ret_1d=float(record[5]) if record[5] is not None else None,
                    contribution_1d=float(record[6]) if record[6] is not None else None,
                )
            )
    return rows


def fetch_metadata_rows() -> list[MetadataRow]:
    sql = (
        "SELECT port_date, ticker, company_name, gics_sector, aiges_composite_average, "
        "       transparency, ethical_principles, governance_structure, "
        "       regulatory_alignment, stakeholder_engagement "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker IS NOT NULL "
        "ORDER BY port_date, rank_index, ticker"
    )
    rows: list[MetadataRow] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        for record in cur.fetchall():
            port_date = _coerce_date(record[0])
            ticker = str(record[1]).strip().upper() if record[1] is not None else ""
            if port_date is None or not ticker:
                continue
            rows.append(
                MetadataRow(
                    port_date=port_date,
                    ticker=ticker,
                    company_name=str(record[2]).strip() if record[2] is not None else None,
                    sector=str(record[3]).strip() if record[3] is not None else None,
                    governance_score=float(record[4]) if record[4] is not None else None,
                    transparency=float(record[5]) if record[5] is not None else None,
                    ethical_principles=float(record[6]) if record[6] is not None else None,
                    governance_structure=float(record[7]) if record[7] is not None else None,
                    regulatory_alignment=float(record[8]) if record[8] is not None else None,
                    stakeholder_engagement=float(record[9]) if record[9] is not None else None,
                )
            )
    return rows


def fetch_price_rows(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
) -> list[PriceRow]:
    sql = (
        "SELECT c.trade_date, c.ticker, NVL(c.canon_adj_close_px, c.canon_close_px) AS price "
        "FROM SC_IDX_PRICES_CANON c "
        "WHERE c.trade_date BETWEEN :start_date AND :end_date "
        "AND c.ticker IN (SELECT DISTINCT ticker FROM tech11_ai_gov_eth_index WHERE ticker IS NOT NULL) "
        "AND NVL(c.canon_adj_close_px, c.canon_close_px) IS NOT NULL "
        "ORDER BY c.trade_date, c.ticker"
    )
    rows: list[PriceRow] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start_date, "end_date": end_date})
        for record in cur.fetchall():
            trade_date = _coerce_date(record[0])
            ticker = str(record[1]).strip().upper() if record[1] is not None else ""
            price = float(record[2]) if record[2] is not None else None
            if trade_date is None or not ticker or price is None:
                continue
            rows.append(PriceRow(trade_date=trade_date, ticker=ticker, price=price))
    return rows


def fetch_portfolio_analytics_max_date() -> _dt.date | None:
    sql = "SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY"
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except Exception as exc:
            if _is_missing_object_error(exc):
                return None
            raise
        row = cur.fetchone()
        return _coerce_date(row[0]) if row else None


def persist_outputs(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    analytics_rows: Sequence[dict[str, object]],
    position_rows: Sequence[dict[str, object]],
    optimizer_rows: Sequence[dict[str, object]],
    constraint_rows: Sequence[dict[str, object]],
) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass

        for table_name in (
            "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
            "SC_IDX_PORTFOLIO_POSITION_DAILY",
            "SC_IDX_PORTFOLIO_OPT_INPUTS",
        ):
            cur.execute(
                f"DELETE FROM {table_name} WHERE trade_date BETWEEN :start_date AND :end_date",
                {"start_date": start_date, "end_date": end_date},
            )

        cur.execute("DELETE FROM SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS")

        if analytics_rows:
            cur.executemany(
                "INSERT INTO SC_IDX_PORTFOLIO_ANALYTICS_DAILY ("
                "  model_code, trade_date, model_name, rebalance_date, level_tr, ret_1d, ret_5d, ret_20d, "
                "  ret_mtd, ret_ytd, vol_20d, vol_60d, drawdown_to_date, max_drawdown_252d, "
                "  n_constituents, n_imputed, top1_weight, top5_weight, herfindahl, avg_governance_score, "
                "  avg_momentum_20d, avg_low_vol_60d, sector_count, factor_governance_exposure, "
                "  factor_momentum_exposure, factor_low_vol_exposure, factor_sector_tilt_abs, factor_concentration"
                ") VALUES ("
                "  :model_code, :trade_date, :model_name, :rebalance_date, :level_tr, :ret_1d, :ret_5d, :ret_20d, "
                "  :ret_mtd, :ret_ytd, :vol_20d, :vol_60d, :drawdown_to_date, :max_drawdown_252d, "
                "  :n_constituents, :n_imputed, :top1_weight, :top5_weight, :herfindahl, :avg_governance_score, "
                "  :avg_momentum_20d, :avg_low_vol_60d, :sector_count, :factor_governance_exposure, "
                "  :factor_momentum_exposure, :factor_low_vol_exposure, :factor_sector_tilt_abs, :factor_concentration"
                ")",
                analytics_rows,
            )

        if position_rows:
            cur.executemany(
                "INSERT INTO SC_IDX_PORTFOLIO_POSITION_DAILY ("
                "  model_code, trade_date, ticker, model_name, rebalance_date, port_date, company_name, sector, "
                "  model_weight, benchmark_weight, active_weight, price_quality, ret_1d, contrib_1d, contrib_5d, "
                "  contrib_20d, contrib_mtd, contrib_ytd, governance_score, transparency, ethical_principles, "
                "  governance_structure, regulatory_alignment, stakeholder_engagement, momentum_20d, low_vol_60d"
                ") VALUES ("
                "  :model_code, :trade_date, :ticker, :model_name, :rebalance_date, :port_date, :company_name, :sector, "
                "  :model_weight, :benchmark_weight, :active_weight, :price_quality, :ret_1d, :contrib_1d, :contrib_5d, "
                "  :contrib_20d, :contrib_mtd, :contrib_ytd, :governance_score, :transparency, :ethical_principles, "
                "  :governance_structure, :regulatory_alignment, :stakeholder_engagement, :momentum_20d, :low_vol_60d"
                ")",
                position_rows,
            )

        if optimizer_rows:
            cur.executemany(
                "INSERT INTO SC_IDX_PORTFOLIO_OPT_INPUTS ("
                "  trade_date, port_date, ticker, company_name, sector, benchmark_weight, governance_score, "
                "  momentum_20d, low_vol_60d, governance_rank, momentum_rank, low_vol_rank, hybrid_rank, "
                "  price_quality, eligible_flag"
                ") VALUES ("
                "  :trade_date, :port_date, :ticker, :company_name, :sector, :benchmark_weight, :governance_score, "
                "  :momentum_20d, :low_vol_60d, :governance_rank, :momentum_rank, :low_vol_rank, :hybrid_rank, "
                "  :price_quality, :eligible_flag"
                ")",
                optimizer_rows,
            )

        if constraint_rows:
            cur.executemany(
                "INSERT INTO SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS ("
                "  model_code, constraint_key, constraint_type, constraint_value"
                ") VALUES ("
                "  :model_code, :constraint_key, :constraint_type, :constraint_value"
                ")",
                constraint_rows,
            )

        conn.commit()


__all__ = [
    "DDL_PATH",
    "DROP_DDL_PATH",
    "apply_ddl",
    "fetch_metadata_rows",
    "fetch_official_daily_rows",
    "fetch_official_position_rows",
    "fetch_portfolio_analytics_max_date",
    "fetch_price_rows",
    "fetch_trade_date_bounds",
    "persist_outputs",
]
