"""Oracle helpers for TECH100 portfolio analytics tables."""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from db_helper import get_connection as _get_connection

from .oracle_runtime import configure_reconstruction_connection_if_enabled

from .portfolio_analytics_v1 import (
    MetadataRow,
    OfficialDailyRow,
    OfficialPositionRow,
    PriceRow,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = REPO_ROOT / "oracle_scripts" / "sc_idx_portfolio_analytics_v1.sql"
DROP_DDL_PATH = REPO_ROOT / "oracle_scripts" / "sc_idx_portfolio_analytics_v1_drop.sql"
DEFAULT_FETCH_SIZE = 500
DEFAULT_WRITE_BATCH_SIZE = 250


def get_connection():
    """Return a connection bounded by the reconstruction statement timeout."""
    return configure_reconstruction_connection_if_enabled(_get_connection())


def _fetchmany(cur, size: int = DEFAULT_FETCH_SIZE) -> Iterator[tuple[object, ...]]:
    cur.arraysize = size
    while True:
        rows = cur.fetchmany(size)
        if not rows:
            return
        yield from rows


def _batches(rows: Sequence[dict[str, object]], size: int) -> Iterator[Sequence[dict[str, object]]]:
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


def _coerce_date(value: object) -> _dt.date | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return None


def _completion_floor(*dates: _dt.date | None) -> _dt.date | None:
    present = [day for day in dates if day is not None]
    return min(present) if present else None


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
        for record in _fetchmany(cur):
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
        for record in _fetchmany(cur):
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
        for record in _fetchmany(cur):
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
        for record in _fetchmany(cur):
            trade_date = _coerce_date(record[0])
            ticker = str(record[1]).strip().upper() if record[1] is not None else ""
            price = float(record[2]) if record[2] is not None else None
            if trade_date is None or not ticker or price is None:
                continue
            rows.append(PriceRow(trade_date=trade_date, ticker=ticker, price=price))
    return rows


def fetch_portfolio_analytics_max_date() -> _dt.date | None:
    return _fetch_max_trade_date("SC_IDX_PORTFOLIO_ANALYTICS_DAILY")


def fetch_portfolio_position_max_date() -> _dt.date | None:
    return _fetch_max_trade_date("SC_IDX_PORTFOLIO_POSITION_DAILY")


def fetch_portfolio_opt_inputs_max_date() -> _dt.date | None:
    return _fetch_max_trade_date("SC_IDX_PORTFOLIO_OPT_INPUTS")


def fetch_latest_required_portfolio_opt_inputs_date(
    end_date: _dt.date | None = None,
) -> _dt.date | None:
    sql = (
        "SELECT MAX(rebalance_date) "
        "FROM SC_IDX_CONSTITUENT_DAILY "
        "WHERE rebalance_date IS NOT NULL "
    )
    params: dict[str, object] = {}
    if end_date is not None:
        sql += "AND trade_date <= :end_date"
        params["end_date"] = end_date
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
        except Exception as exc:
            if _is_missing_object_error(exc):
                return None
            raise
        row = cur.fetchone()
        return _coerce_date(row[0]) if row else None


def fetch_portfolio_completion_max_date() -> _dt.date | None:
    analytics_max = fetch_portfolio_analytics_max_date()
    position_max = fetch_portfolio_position_max_date()
    daily_floor = _completion_floor(analytics_max, position_max)
    if daily_floor is None:
        return None

    required_opt_inputs_date = fetch_latest_required_portfolio_opt_inputs_date(daily_floor)
    if required_opt_inputs_date is None:
        return daily_floor

    opt_inputs_max = fetch_portfolio_opt_inputs_max_date()
    if opt_inputs_max is not None and opt_inputs_max >= required_opt_inputs_date:
        return daily_floor
    return opt_inputs_max


def _fetch_max_trade_date(table_name: str) -> _dt.date | None:
    sql = f"SELECT MAX(trade_date) FROM {table_name}"
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
    batch_size = max(1, int(os.getenv("SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE)))
    statements = (
        (
            "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
            analytics_rows,
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
        ),
        (
            "SC_IDX_PORTFOLIO_POSITION_DAILY",
            position_rows,
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
        ),
        (
            "SC_IDX_PORTFOLIO_OPT_INPUTS",
            optimizer_rows,
            "INSERT INTO SC_IDX_PORTFOLIO_OPT_INPUTS ("
                "  trade_date, port_date, ticker, company_name, sector, benchmark_weight, governance_score, "
                "  momentum_20d, low_vol_60d, governance_rank, momentum_rank, low_vol_rank, hybrid_rank, "
                "  price_quality, eligible_flag"
                ") VALUES ("
                "  :trade_date, :port_date, :ticker, :company_name, :sector, :benchmark_weight, :governance_score, "
                "  :momentum_20d, :low_vol_60d, :governance_rank, :momentum_rank, :low_vol_rank, :hybrid_rank, "
                "  :price_quality, :eligible_flag"
                ")",
        ),
    )
    with get_connection() as conn:
        if os.getenv("SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS") is None:
            conn.call_timeout = 120_000
        cur = conn.cursor()
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
        for table_name, rows, sql in statements:
            cur.execute(
                f"DELETE FROM {table_name} WHERE trade_date BETWEEN :start_date AND :end_date",
                {"start_date": start_date, "end_date": end_date},
            )
            written = 0
            for batch in _batches(rows, batch_size):
                cur.executemany(sql, batch)
                written += len(batch)
            print(
                f"portfolio_persist:table={table_name} rows={written} batch_size={batch_size}",
                flush=True,
            )
        cur.execute("DELETE FROM SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS")
        constraint_sql = (
            "INSERT INTO SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS ("
            "  model_code, constraint_key, constraint_type, constraint_value"
            ") VALUES ("
            "  :model_code, :constraint_key, :constraint_type, :constraint_value"
            ")"
        )
        for batch in _batches(constraint_rows, batch_size):
            cur.executemany(constraint_sql, batch)
        conn.commit()


def reset_output_window(*, start_date: _dt.date, end_date: _dt.date) -> None:
    """Commit an empty protected window before bounded manifest-backed inserts."""
    with get_connection() as conn:
        cur = conn.cursor()
        for table_name in (
            "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
            "SC_IDX_PORTFOLIO_POSITION_DAILY",
            "SC_IDX_PORTFOLIO_OPT_INPUTS",
        ):
            cur.execute(
                f"DELETE FROM {table_name} WHERE trade_date BETWEEN :start_date AND :end_date",
                {"start_date": start_date, "end_date": end_date},
            )
        conn.commit()


def _normalized_constraint_rows(
    rows: Iterable[dict[str, object] | Sequence[object]],
) -> list[tuple[object, object, object, object]]:
    normalized: list[tuple[object, object, object, object]] = []
    for row in rows:
        if isinstance(row, dict):
            values = (
                row.get("model_code"),
                row.get("constraint_key"),
                row.get("constraint_type"),
                row.get("constraint_value"),
            )
        else:
            values = tuple(row[:4])
        normalized.append(tuple(None if value is None else str(value) for value in values))
    return sorted(normalized)


def fetch_constraint_rows() -> list[tuple[object, object, object, object]]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT model_code,constraint_key,constraint_type,constraint_value "
            "FROM SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS "
            "ORDER BY model_code,constraint_key"
        )
        return [tuple(row) for row in _fetchmany(cur)]


def validate_static_constraints(expected_rows: Sequence[dict[str, object]]) -> None:
    """Fail before output mutation unless immutable constraints match exactly."""
    expected = _normalized_constraint_rows(expected_rows)
    actual = _normalized_constraint_rows(fetch_constraint_rows())
    if actual != expected:
        raise RuntimeError(
            "model_portfolio_constraints_mismatch:"
            f"expected_rows={len(expected)}:actual_rows={len(actual)}"
        )


def persist_model_output_batch(
    *,
    analytics_rows: Sequence[dict[str, object]],
    position_rows: Sequence[dict[str, object]],
) -> tuple[int, int]:
    """Persist one model at a time so six model paths are never retained together."""
    batch_size = max(1, int(os.getenv("SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE)))
    analytics_sql = (
        "INSERT INTO SC_IDX_PORTFOLIO_ANALYTICS_DAILY ("
        "model_code,trade_date,model_name,rebalance_date,level_tr,ret_1d,ret_5d,ret_20d,ret_mtd,ret_ytd,"
        "vol_20d,vol_60d,drawdown_to_date,max_drawdown_252d,n_constituents,n_imputed,top1_weight,top5_weight,"
        "herfindahl,avg_governance_score,avg_momentum_20d,avg_low_vol_60d,sector_count,factor_governance_exposure,"
        "factor_momentum_exposure,factor_low_vol_exposure,factor_sector_tilt_abs,factor_concentration) VALUES ("
        ":model_code,:trade_date,:model_name,:rebalance_date,:level_tr,:ret_1d,:ret_5d,:ret_20d,:ret_mtd,:ret_ytd,"
        ":vol_20d,:vol_60d,:drawdown_to_date,:max_drawdown_252d,:n_constituents,:n_imputed,:top1_weight,:top5_weight,"
        ":herfindahl,:avg_governance_score,:avg_momentum_20d,:avg_low_vol_60d,:sector_count,:factor_governance_exposure,"
        ":factor_momentum_exposure,:factor_low_vol_exposure,:factor_sector_tilt_abs,:factor_concentration)"
    )
    position_sql = (
        "INSERT INTO SC_IDX_PORTFOLIO_POSITION_DAILY ("
        "model_code,trade_date,ticker,model_name,rebalance_date,port_date,company_name,sector,model_weight,"
        "benchmark_weight,active_weight,price_quality,ret_1d,contrib_1d,contrib_5d,contrib_20d,contrib_mtd,"
        "contrib_ytd,governance_score,transparency,ethical_principles,governance_structure,regulatory_alignment,"
        "stakeholder_engagement,momentum_20d,low_vol_60d) VALUES ("
        ":model_code,:trade_date,:ticker,:model_name,:rebalance_date,:port_date,:company_name,:sector,:model_weight,"
        ":benchmark_weight,:active_weight,:price_quality,:ret_1d,:contrib_1d,:contrib_5d,:contrib_20d,:contrib_mtd,"
        ":contrib_ytd,:governance_score,:transparency,:ethical_principles,:governance_structure,:regulatory_alignment,"
        ":stakeholder_engagement,:momentum_20d,:low_vol_60d)"
    )
    for rows, sql in ((analytics_rows, analytics_sql), (position_rows, position_sql)):
        with get_connection() as conn:
            cur = conn.cursor()
            for batch in _batches(rows, batch_size):
                cur.executemany(sql, batch)
            conn.commit()
    return len(analytics_rows), len(position_rows)


def persist_optimizer_with_static_constraints(
    *,
    optimizer_rows: Sequence[dict[str, object]],
    constraint_rows: Sequence[dict[str, object]],
) -> int:
    """Persist optimizer rows while leaving validated static constraints unchanged."""
    validate_static_constraints(constraint_rows)
    batch_size = max(1, int(os.getenv("SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE)))
    optimizer_sql = (
        "INSERT INTO SC_IDX_PORTFOLIO_OPT_INPUTS (trade_date,port_date,ticker,company_name,sector,benchmark_weight,"
        "governance_score,momentum_20d,low_vol_60d,governance_rank,momentum_rank,low_vol_rank,hybrid_rank,"
        "price_quality,eligible_flag) VALUES (:trade_date,:port_date,:ticker,:company_name,:sector,:benchmark_weight,"
        ":governance_score,:momentum_20d,:low_vol_60d,:governance_rank,:momentum_rank,:low_vol_rank,:hybrid_rank,"
        ":price_quality,:eligible_flag)"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        for batch in _batches(optimizer_rows, batch_size):
            cur.executemany(optimizer_sql, batch)
        conn.commit()
    return len(optimizer_rows)


__all__ = [
    "DDL_PATH",
    "DROP_DDL_PATH",
    "apply_ddl",
    "fetch_metadata_rows",
    "fetch_official_daily_rows",
    "fetch_official_position_rows",
    "fetch_portfolio_analytics_max_date",
    "fetch_portfolio_position_max_date",
    "fetch_portfolio_opt_inputs_max_date",
    "fetch_latest_required_portfolio_opt_inputs_date",
    "fetch_portfolio_completion_max_date",
    "fetch_price_rows",
    "fetch_trade_date_bounds",
    "persist_outputs",
    "persist_model_output_batch",
    "persist_optimizer_with_static_constraints",
    "reset_output_window",
    "fetch_constraint_rows",
    "validate_static_constraints",
]
