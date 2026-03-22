"""Pure-Python helpers for TECH100 portfolio analytics and model portfolios."""

from __future__ import annotations

import datetime as _dt
import math
import statistics
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Sequence

INDEX_CODE = "TECH100"
MOMENTUM_WINDOW = 20
LOW_VOL_WINDOW = 60
MAX_DRAWDOWN_WINDOW = 252
TILT_MIN_MULTIPLIER = 0.5
TILT_MAX_MULTIPLIER = 1.5
_TICKER_ALIASES = {"FI": "FISV"}


@dataclass(frozen=True)
class ModelSpec:
    code: str
    name: str


@dataclass(frozen=True)
class OfficialDailyRow:
    trade_date: _dt.date
    level_tr: float
    ret_1d: float | None = None
    ret_5d: float | None = None
    ret_20d: float | None = None
    vol_20d: float | None = None
    max_drawdown_252d: float | None = None
    n_constituents: int | None = None
    n_imputed: int | None = None
    top5_weight: float | None = None
    herfindahl: float | None = None


@dataclass(frozen=True)
class OfficialPositionRow:
    trade_date: _dt.date
    rebalance_date: _dt.date
    ticker: str
    weight: float
    price_quality: str | None = None
    ret_1d: float | None = None
    contribution_1d: float | None = None


@dataclass(frozen=True)
class MetadataRow:
    port_date: _dt.date
    ticker: str
    company_name: str | None = None
    sector: str | None = None
    governance_score: float | None = None
    transparency: float | None = None
    ethical_principles: float | None = None
    governance_structure: float | None = None
    regulatory_alignment: float | None = None
    stakeholder_engagement: float | None = None


@dataclass(frozen=True)
class PriceRow:
    trade_date: _dt.date
    ticker: str
    price: float


@dataclass(frozen=True)
class FactorPoint:
    momentum_20d: float | None
    low_vol_60d: float | None


DEFAULT_MODEL_SPECS = (
    ModelSpec(code=INDEX_CODE, name="Official TECH100"),
    ModelSpec(code="TECH100_EQ", name="TECH100 Equal Weight"),
    ModelSpec(code="TECH100_GOV", name="TECH100 Governance Tilt"),
    ModelSpec(code="TECH100_MOM", name="TECH100 Momentum Tilt"),
    ModelSpec(code="TECH100_LOWVOL", name="TECH100 Low Volatility Tilt"),
    ModelSpec(code="TECH100_GOV_MOM", name="TECH100 Governance + Momentum"),
)


def _normalize_ticker(value: str) -> str:
    cleaned = (value or "").strip().upper()
    return _TICKER_ALIASES.get(cleaned, cleaned)


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _metadata_to_dict(rows: Iterable[MetadataRow]) -> dict[_dt.date, dict[str, MetadataRow]]:
    by_port_date: dict[_dt.date, dict[str, MetadataRow]] = {}
    for row in rows:
        by_port_date.setdefault(row.port_date, {})[_normalize_ticker(row.ticker)] = row
    return by_port_date


def _price_to_dict(rows: Iterable[PriceRow]) -> dict[_dt.date, dict[str, float]]:
    by_date: dict[_dt.date, dict[str, float]] = {}
    for row in rows:
        by_date.setdefault(row.trade_date, {})[_normalize_ticker(row.ticker)] = float(row.price)
    return by_date


def _official_positions_to_dict(
    rows: Iterable[OfficialPositionRow],
) -> tuple[
    dict[_dt.date, dict[str, OfficialPositionRow]],
    dict[_dt.date, _dt.date],
]:
    by_date: dict[_dt.date, dict[str, OfficialPositionRow]] = {}
    rebalance_by_date: dict[_dt.date, _dt.date] = {}
    for row in rows:
        ticker = _normalize_ticker(row.ticker)
        by_date.setdefault(row.trade_date, {})[ticker] = OfficialPositionRow(
            trade_date=row.trade_date,
            rebalance_date=row.rebalance_date,
            ticker=ticker,
            weight=float(row.weight),
            price_quality=row.price_quality,
            ret_1d=_safe_float(row.ret_1d),
            contribution_1d=_safe_float(row.contribution_1d),
        )
        rebalance_by_date[row.trade_date] = row.rebalance_date
    return by_date, rebalance_by_date


def _rank_positions(
    values_by_ticker: Mapping[str, float | None],
    *,
    higher_is_better: bool,
) -> tuple[dict[str, float], dict[str, int]]:
    available = [(ticker, value) for ticker, value in values_by_ticker.items() if value is not None]
    if not available:
        return {ticker: 1.0 for ticker in values_by_ticker}, {ticker: 1 for ticker in values_by_ticker}

    ordered = sorted(available, key=lambda item: (item[1], item[0]), reverse=higher_is_better)
    if len(ordered) == 1:
        only_ticker = ordered[0][0]
        return (
            {ticker: 1.0 for ticker in values_by_ticker},
            {ticker: 1 if ticker == only_ticker else 2 for ticker in values_by_ticker},
        )

    span = TILT_MAX_MULTIPLIER - TILT_MIN_MULTIPLIER
    multipliers: dict[str, float] = {}
    ranks: dict[str, int] = {}
    denom = max(len(ordered) - 1, 1)
    for idx, (ticker, _value) in enumerate(ordered):
        pct = 1.0 - (idx / denom)
        multipliers[ticker] = TILT_MIN_MULTIPLIER + pct * span
        ranks[ticker] = idx + 1

    for ticker in values_by_ticker:
        multipliers.setdefault(ticker, 1.0)
        ranks.setdefault(ticker, len(ordered) + 1)
    return multipliers, ranks


def build_model_target_weights(
    *,
    model_code: str,
    benchmark_weights: Mapping[str, float],
    governance_scores: Mapping[str, float | None],
    momentum_scores: Mapping[str, float | None],
    low_vol_scores: Mapping[str, float | None],
) -> dict[str, float]:
    tickers = [ticker for ticker, weight in benchmark_weights.items() if weight and weight > 0]
    if not tickers:
        return {}

    if model_code == INDEX_CODE:
        return normalize_weights({ticker: benchmark_weights[ticker] for ticker in tickers})
    if model_code == "TECH100_EQ":
        equal_weight = 1.0 / len(tickers)
        return {ticker: equal_weight for ticker in tickers}

    governance_mult, _gov_ranks = _rank_positions(governance_scores, higher_is_better=True)
    momentum_mult, _mom_ranks = _rank_positions(momentum_scores, higher_is_better=True)
    low_vol_mult, _low_ranks = _rank_positions(low_vol_scores, higher_is_better=False)

    raw_weights: dict[str, float] = {}
    for ticker in tickers:
        base_weight = max(float(benchmark_weights.get(ticker) or 0.0), 0.0)
        if model_code == "TECH100_GOV":
            multiplier = governance_mult.get(ticker, 1.0)
        elif model_code == "TECH100_MOM":
            multiplier = momentum_mult.get(ticker, 1.0)
        elif model_code == "TECH100_LOWVOL":
            multiplier = low_vol_mult.get(ticker, 1.0)
        elif model_code == "TECH100_GOV_MOM":
            multiplier = governance_mult.get(ticker, 1.0) * momentum_mult.get(ticker, 1.0)
        else:
            multiplier = 1.0
        raw_weights[ticker] = base_weight * multiplier
    return normalize_weights(raw_weights)


def normalize_weights(raw_weights: Mapping[str, float]) -> dict[str, float]:
    cleaned = {ticker: max(float(weight), 0.0) for ticker, weight in raw_weights.items() if weight is not None}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {ticker: weight / total for ticker, weight in cleaned.items() if weight > 0}


def compute_factor_history(
    trade_days: Sequence[_dt.date],
    price_by_date: Mapping[_dt.date, Mapping[str, float]],
    tickers: Iterable[str],
    *,
    momentum_window: int = MOMENTUM_WINDOW,
    low_vol_window: int = LOW_VOL_WINDOW,
) -> dict[_dt.date, dict[str, FactorPoint]]:
    tickers_list = sorted({_normalize_ticker(ticker) for ticker in tickers if ticker})
    history: dict[_dt.date, dict[str, FactorPoint]] = {}

    for idx, trade_date in enumerate(trade_days):
        daily: dict[str, FactorPoint] = {}
        for ticker in tickers_list:
            today_price = _safe_float(price_by_date.get(trade_date, {}).get(ticker))
            momentum = None
            if idx >= momentum_window and today_price not in (None, 0):
                anchor_price = _safe_float(price_by_date.get(trade_days[idx - momentum_window], {}).get(ticker))
                if anchor_price not in (None, 0):
                    momentum = today_price / anchor_price - 1.0

            vol = None
            if idx >= low_vol_window:
                returns: list[float] = []
                for j in range(idx - low_vol_window + 1, idx + 1):
                    prev_date = trade_days[j - 1]
                    current_date = trade_days[j]
                    prev_price = _safe_float(price_by_date.get(prev_date, {}).get(ticker))
                    current_price = _safe_float(price_by_date.get(current_date, {}).get(ticker))
                    if prev_price in (None, 0) or current_price is None:
                        returns = []
                        break
                    returns.append(current_price / prev_price - 1.0)
                if len(returns) == low_vol_window:
                    vol = statistics.pstdev(returns) * math.sqrt(252.0)
            daily[ticker] = FactorPoint(momentum_20d=momentum, low_vol_60d=vol)
        history[trade_date] = daily
    return history


def build_portfolio_outputs(
    *,
    official_daily_rows: Sequence[OfficialDailyRow],
    official_position_rows: Sequence[OfficialPositionRow],
    metadata_rows: Sequence[MetadataRow],
    price_rows: Sequence[PriceRow],
    model_specs: Sequence[ModelSpec] = DEFAULT_MODEL_SPECS,
) -> dict[str, list[dict[str, object]]]:
    if not official_daily_rows:
        return {"analytics": [], "positions": [], "optimizer_inputs": [], "constraints": []}

    trade_days = sorted(row.trade_date for row in official_daily_rows)
    official_daily_by_date = {row.trade_date: row for row in official_daily_rows}
    official_positions_by_date, official_rebalance_by_date = _official_positions_to_dict(official_position_rows)
    metadata_by_port_date = _metadata_to_dict(metadata_rows)
    port_dates = sorted(metadata_by_port_date)
    price_by_date = _price_to_dict(price_rows)

    all_tickers = {
        ticker
        for by_date in official_positions_by_date.values()
        for ticker in by_date
    }
    all_tickers.update(
        ticker
        for by_port in metadata_by_port_date.values()
        for ticker in by_port
    )
    factor_history = compute_factor_history(trade_days, price_by_date, all_tickers)
    active_port_by_trade = _active_port_dates(trade_days, port_dates)
    rebalance_days = sorted({row.rebalance_date for row in official_position_rows})

    analytics_rows: list[dict[str, object]] = []
    position_rows: list[dict[str, object]] = []
    optimizer_rows: list[dict[str, object]] = []
    constraints_rows = build_constraint_rows(model_specs)

    benchmark_levels = {date: official_daily_by_date[date].level_tr for date in trade_days}
    benchmark_weights_by_date = {
        date: {ticker: row.weight for ticker, row in positions.items()}
        for date, positions in official_positions_by_date.items()
    }
    price_quality_by_date = {
        date: {ticker: (row.price_quality or "UNKNOWN") for ticker, row in positions.items()}
        for date, positions in official_positions_by_date.items()
    }
    official_contrib_by_date = {
        date: {
            ticker: _safe_float(row.contribution_1d) or 0.0
            for ticker, row in positions.items()
        }
        for date, positions in official_positions_by_date.items()
    }
    official_ret_1d_by_date = {
        date: {
            ticker: _safe_float(row.ret_1d)
            for ticker, row in positions.items()
        }
        for date, positions in official_positions_by_date.items()
    }

    optimizer_rows.extend(
        build_optimizer_inputs(
            rebalance_days=rebalance_days,
            trade_days=trade_days,
            benchmark_weights_by_date=benchmark_weights_by_date,
            active_port_by_trade=active_port_by_trade,
            metadata_by_port_date=metadata_by_port_date,
            factor_history=factor_history,
            price_quality_by_date=price_quality_by_date,
        )
    )

    for spec in model_specs:
        if spec.code == INDEX_CODE:
            model_levels = benchmark_levels
            model_weights_by_date = benchmark_weights_by_date
            model_rebalance_by_date = official_rebalance_by_date
            model_contrib_by_date = official_contrib_by_date
            model_ret_1d_by_date = official_ret_1d_by_date
        else:
            (
                model_levels,
                model_weights_by_date,
                model_rebalance_by_date,
                model_contrib_by_date,
                model_ret_1d_by_date,
            ) = build_model_path(
                model_code=spec.code,
                trade_days=trade_days,
                rebalance_days=rebalance_days,
                benchmark_weights_by_date=benchmark_weights_by_date,
                benchmark_levels=benchmark_levels,
                active_port_by_trade=active_port_by_trade,
                metadata_by_port_date=metadata_by_port_date,
                factor_history=factor_history,
                price_by_date=price_by_date,
            )

        model_positions = build_position_rows(
            model_code=spec.code,
            model_name=spec.name,
            trade_days=trade_days,
            levels_by_date=model_levels,
            weights_by_date=model_weights_by_date,
            rebalance_by_date=model_rebalance_by_date,
            benchmark_weights_by_date=benchmark_weights_by_date,
            contrib_by_date=model_contrib_by_date,
            ret_1d_by_date=model_ret_1d_by_date,
            active_port_by_trade=active_port_by_trade,
            metadata_by_port_date=metadata_by_port_date,
            factor_history=factor_history,
            price_quality_by_date=price_quality_by_date,
        )
        position_rows.extend(model_positions)
        analytics_rows.extend(
            build_analytics_rows(
                model_code=spec.code,
                model_name=spec.name,
                trade_days=trade_days,
                levels_by_date=model_levels,
                weights_by_date=model_weights_by_date,
                rebalance_by_date=model_rebalance_by_date,
                benchmark_weights_by_date=benchmark_weights_by_date,
                contrib_by_date=model_contrib_by_date,
                active_port_by_trade=active_port_by_trade,
                metadata_by_port_date=metadata_by_port_date,
                factor_history=factor_history,
                price_quality_by_date=price_quality_by_date,
                official_daily_by_date=official_daily_by_date if spec.code == INDEX_CODE else None,
            )
        )

    return {
        "analytics": analytics_rows,
        "positions": position_rows,
        "optimizer_inputs": optimizer_rows,
        "constraints": constraints_rows,
    }


def build_constraint_rows(model_specs: Sequence[ModelSpec]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    base_constraints = [
        ("LONG_ONLY", "BOOLEAN", "TRUE"),
        ("FULLY_INVESTED", "BOOLEAN", "TRUE"),
        ("LIVE_OPTIMIZER", "BOOLEAN", "FALSE"),
        ("UNIVERSE_SOURCE", "TEXT", "TECH100_TOP25_ACTIVE"),
        ("REBALANCE_SOURCE", "TEXT", "TECH100_REBALANCE_DATES"),
        ("MAX_CONSTITUENTS", "NUMBER", "25"),
        ("SOLVER_MODE", "TEXT", "PRECOMPUTED_ONLY"),
    ]
    tilt_constraints = {
        INDEX_CODE: "OFFICIAL_BENCHMARK",
        "TECH100_EQ": "EQUAL_WEIGHT",
        "TECH100_GOV": "BENCHMARK_X_GOVERNANCE_RANK",
        "TECH100_MOM": "BENCHMARK_X_MOMENTUM_RANK",
        "TECH100_LOWVOL": "BENCHMARK_X_LOW_VOL_RANK",
        "TECH100_GOV_MOM": "BENCHMARK_X_GOVERNANCE_RANK_X_MOMENTUM_RANK",
    }
    for spec in model_specs:
        for key, value_type, value in base_constraints:
            rows.append(
                {
                    "model_code": spec.code,
                    "constraint_key": key,
                    "constraint_type": value_type,
                    "constraint_value": value,
                }
            )
        rows.append(
            {
                "model_code": spec.code,
                "constraint_key": "WEIGHT_CONSTRUCTION",
                "constraint_type": "TEXT",
                "constraint_value": tilt_constraints.get(spec.code, "UNKNOWN"),
            }
        )
    return rows


def _active_port_dates(
    trade_days: Sequence[_dt.date],
    port_dates: Sequence[_dt.date],
) -> dict[_dt.date, _dt.date | None]:
    active_by_trade: dict[_dt.date, _dt.date | None] = {}
    current: _dt.date | None = None
    port_iter = iter(sorted(port_dates))
    next_port = next(port_iter, None)
    for trade_date in trade_days:
        while next_port is not None and next_port <= trade_date:
            current = next_port
            next_port = next(port_iter, None)
        active_by_trade[trade_date] = current
    return active_by_trade


def build_optimizer_inputs(
    *,
    rebalance_days: Sequence[_dt.date],
    trade_days: Sequence[_dt.date],
    benchmark_weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    active_port_by_trade: Mapping[_dt.date, _dt.date | None],
    metadata_by_port_date: Mapping[_dt.date, Mapping[str, MetadataRow]],
    factor_history: Mapping[_dt.date, Mapping[str, FactorPoint]],
    price_quality_by_date: Mapping[_dt.date, Mapping[str, str]],
) -> list[dict[str, object]]:
    trade_index = {trade_date: idx for idx, trade_date in enumerate(trade_days)}
    rows: list[dict[str, object]] = []

    for rebalance_date in rebalance_days:
        port_date = active_port_by_trade.get(rebalance_date)
        benchmark_weights = benchmark_weights_by_date.get(rebalance_date, {})
        signal_idx = max(trade_index.get(rebalance_date, 0) - 1, 0)
        signal_date = trade_days[signal_idx]
        metadata_map = metadata_by_port_date.get(port_date or rebalance_date, {})
        governance_values = {
            ticker: _safe_float(metadata_map.get(ticker).governance_score) if metadata_map.get(ticker) else None
            for ticker in benchmark_weights
        }
        momentum_values = {
            ticker: factor_history.get(signal_date, {}).get(ticker, FactorPoint(None, None)).momentum_20d
            for ticker in benchmark_weights
        }
        low_vol_values = {
            ticker: factor_history.get(signal_date, {}).get(ticker, FactorPoint(None, None)).low_vol_60d
            for ticker in benchmark_weights
        }
        _gov_mult, gov_ranks = _rank_positions(governance_values, higher_is_better=True)
        _mom_mult, mom_ranks = _rank_positions(momentum_values, higher_is_better=True)
        _low_mult, low_ranks = _rank_positions(low_vol_values, higher_is_better=False)
        hybrid_values = {
            ticker: (governance_values.get(ticker) or 0.0) + (momentum_values.get(ticker) or 0.0)
            for ticker in benchmark_weights
        }
        _hyb_mult, hybrid_ranks = _rank_positions(hybrid_values, higher_is_better=True)

        for ticker, benchmark_weight in benchmark_weights.items():
            metadata = metadata_map.get(ticker)
            factor_point = factor_history.get(signal_date, {}).get(ticker, FactorPoint(None, None))
            rows.append(
                {
                    "trade_date": rebalance_date,
                    "port_date": port_date,
                    "ticker": ticker,
                    "company_name": metadata.company_name if metadata else None,
                    "sector": metadata.sector if metadata else None,
                    "benchmark_weight": benchmark_weight,
                    "governance_score": _safe_float(metadata.governance_score) if metadata else None,
                    "momentum_20d": factor_point.momentum_20d,
                    "low_vol_60d": factor_point.low_vol_60d,
                    "governance_rank": gov_ranks.get(ticker),
                    "momentum_rank": mom_ranks.get(ticker),
                    "low_vol_rank": low_ranks.get(ticker),
                    "hybrid_rank": hybrid_ranks.get(ticker),
                    "price_quality": price_quality_by_date.get(rebalance_date, {}).get(ticker),
                    "eligible_flag": "Y",
                }
            )
    return rows


def build_model_path(
    *,
    model_code: str,
    trade_days: Sequence[_dt.date],
    rebalance_days: Sequence[_dt.date],
    benchmark_weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    benchmark_levels: Mapping[_dt.date, float],
    active_port_by_trade: Mapping[_dt.date, _dt.date | None],
    metadata_by_port_date: Mapping[_dt.date, Mapping[str, MetadataRow]],
    factor_history: Mapping[_dt.date, Mapping[str, FactorPoint]],
    price_by_date: Mapping[_dt.date, Mapping[str, float]],
) -> tuple[
    dict[_dt.date, float],
    dict[_dt.date, dict[str, float]],
    dict[_dt.date, _dt.date],
    dict[_dt.date, dict[str, float]],
    dict[_dt.date, dict[str, float | None]],
]:
    rebalance_set = set(rebalance_days)
    trade_index = {trade_date: idx for idx, trade_date in enumerate(trade_days)}

    levels_by_date: dict[_dt.date, float] = {}
    weights_by_date: dict[_dt.date, dict[str, float]] = {}
    rebalance_by_date: dict[_dt.date, _dt.date] = {}
    contrib_by_date: dict[_dt.date, dict[str, float]] = {}
    ret_1d_by_date: dict[_dt.date, dict[str, float | None]] = {}
    current_shares: dict[str, float] = {}
    current_rebalance: _dt.date | None = None
    prev_trade: _dt.date | None = None
    prev_weights: dict[str, float] = {}
    prev_prices: dict[str, float] = {}

    for trade_date in trade_days:
        if not current_shares or trade_date in rebalance_set:
            benchmark_weights = benchmark_weights_by_date.get(trade_date, {})
            signal_idx = max(trade_index.get(trade_date, 0) - 1, 0)
            signal_date = trade_days[signal_idx]
            port_date = active_port_by_trade.get(trade_date)
            metadata_map = metadata_by_port_date.get(port_date or trade_date, {})
            governance_scores = {
                ticker: _safe_float(metadata_map.get(ticker).governance_score) if metadata_map.get(ticker) else None
                for ticker in benchmark_weights
            }
            momentum_scores = {
                ticker: factor_history.get(signal_date, {}).get(ticker, FactorPoint(None, None)).momentum_20d
                for ticker in benchmark_weights
            }
            low_vol_scores = {
                ticker: factor_history.get(signal_date, {}).get(ticker, FactorPoint(None, None)).low_vol_60d
                for ticker in benchmark_weights
            }
            target_weights = build_model_target_weights(
                model_code=model_code,
                benchmark_weights=benchmark_weights,
                governance_scores=governance_scores,
                momentum_scores=momentum_scores,
                low_vol_scores=low_vol_scores,
            )
            anchor_date = prev_trade or trade_date
            anchor_level = levels_by_date.get(prev_trade, benchmark_levels.get(anchor_date, benchmark_levels[trade_days[0]]))
            anchor_prices = price_by_date.get(anchor_date, {})
            current_shares = {
                ticker: anchor_level * weight / anchor_prices[ticker]
                for ticker, weight in target_weights.items()
                if anchor_prices.get(ticker) not in (None, 0)
            }
            current_rebalance = trade_date

        prices_now = price_by_date.get(trade_date, {})
        market_values = {
            ticker: shares * prices_now[ticker]
            for ticker, shares in current_shares.items()
            if prices_now.get(ticker) not in (None, 0)
        }
        total_market_value = sum(market_values.values())
        if total_market_value <= 0:
            continue

        levels_by_date[trade_date] = total_market_value
        weights_now = {ticker: value / total_market_value for ticker, value in market_values.items()}
        weights_by_date[trade_date] = weights_now
        if current_rebalance is not None:
            rebalance_by_date[trade_date] = current_rebalance

        daily_contrib: dict[str, float] = {}
        daily_ret: dict[str, float | None] = {}
        for ticker, weight_prev in prev_weights.items():
            start_price = _safe_float(prev_prices.get(ticker))
            end_price = _safe_float(prices_now.get(ticker))
            if start_price in (None, 0) or end_price is None:
                daily_ret[ticker] = None
                continue
            ret = end_price / start_price - 1.0
            daily_ret[ticker] = ret
            daily_contrib[ticker] = weight_prev * ret
        contrib_by_date[trade_date] = daily_contrib
        ret_1d_by_date[trade_date] = daily_ret

        prev_trade = trade_date
        prev_weights = weights_now
        prev_prices = {
            ticker: prices_now[ticker]
            for ticker in weights_now
            if prices_now.get(ticker) is not None
        }

    return levels_by_date, weights_by_date, rebalance_by_date, contrib_by_date, ret_1d_by_date


def build_position_rows(
    *,
    model_code: str,
    model_name: str,
    trade_days: Sequence[_dt.date],
    levels_by_date: Mapping[_dt.date, float],
    weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    rebalance_by_date: Mapping[_dt.date, _dt.date],
    benchmark_weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    contrib_by_date: Mapping[_dt.date, Mapping[str, float]],
    ret_1d_by_date: Mapping[_dt.date, Mapping[str, float | None]],
    active_port_by_trade: Mapping[_dt.date, _dt.date | None],
    metadata_by_port_date: Mapping[_dt.date, Mapping[str, MetadataRow]],
    factor_history: Mapping[_dt.date, Mapping[str, FactorPoint]],
    price_quality_by_date: Mapping[_dt.date, Mapping[str, str]],
) -> list[dict[str, object]]:
    ticker_series = _ticker_series(trade_days, weights_by_date, contrib_by_date)
    month_starts, year_starts = _period_starts(trade_days)
    rows: list[dict[str, object]] = []

    for idx, trade_date in enumerate(trade_days):
        weights = weights_by_date.get(trade_date, {})
        benchmark_weights = benchmark_weights_by_date.get(trade_date, {})
        port_date = active_port_by_trade.get(trade_date)
        metadata_map = metadata_by_port_date.get(port_date or trade_date, {})
        for ticker, model_weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
            metadata = metadata_map.get(ticker)
            factor_point = factor_history.get(trade_date, {}).get(ticker, FactorPoint(None, None))
            contrib_series = ticker_series.get(ticker, [0.0] * len(trade_days))
            rows.append(
                {
                    "model_code": model_code,
                    "model_name": model_name,
                    "trade_date": trade_date,
                    "rebalance_date": rebalance_by_date.get(trade_date),
                    "port_date": port_date,
                    "ticker": ticker,
                    "company_name": metadata.company_name if metadata else None,
                    "sector": metadata.sector if metadata and metadata.sector else "Unclassified",
                    "model_weight": model_weight,
                    "benchmark_weight": benchmark_weights.get(ticker),
                    "active_weight": model_weight - float(benchmark_weights.get(ticker) or 0.0),
                    "price_quality": price_quality_by_date.get(trade_date, {}).get(ticker),
                    "ret_1d": ret_1d_by_date.get(trade_date, {}).get(ticker),
                    "contrib_1d": contrib_by_date.get(trade_date, {}).get(ticker, 0.0),
                    "contrib_5d": _window_sum(contrib_series, idx, 5),
                    "contrib_20d": _window_sum(contrib_series, idx, 20),
                    "contrib_mtd": _window_sum(contrib_series, idx, idx - month_starts[idx] + 1),
                    "contrib_ytd": _window_sum(contrib_series, idx, idx - year_starts[idx] + 1),
                    "governance_score": _safe_float(metadata.governance_score) if metadata else None,
                    "transparency": _safe_float(metadata.transparency) if metadata else None,
                    "ethical_principles": _safe_float(metadata.ethical_principles) if metadata else None,
                    "governance_structure": _safe_float(metadata.governance_structure) if metadata else None,
                    "regulatory_alignment": _safe_float(metadata.regulatory_alignment) if metadata else None,
                    "stakeholder_engagement": _safe_float(metadata.stakeholder_engagement) if metadata else None,
                    "momentum_20d": factor_point.momentum_20d,
                    "low_vol_60d": factor_point.low_vol_60d,
                }
            )
    return rows


def build_analytics_rows(
    *,
    model_code: str,
    model_name: str,
    trade_days: Sequence[_dt.date],
    levels_by_date: Mapping[_dt.date, float],
    weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    rebalance_by_date: Mapping[_dt.date, _dt.date],
    benchmark_weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    contrib_by_date: Mapping[_dt.date, Mapping[str, float]],
    active_port_by_trade: Mapping[_dt.date, _dt.date | None],
    metadata_by_port_date: Mapping[_dt.date, Mapping[str, MetadataRow]],
    factor_history: Mapping[_dt.date, Mapping[str, FactorPoint]],
    price_quality_by_date: Mapping[_dt.date, Mapping[str, str]],
    official_daily_by_date: Mapping[_dt.date, OfficialDailyRow] | None,
) -> list[dict[str, object]]:
    month_starts, year_starts = _period_starts(trade_days)
    level_series = [_safe_float(levels_by_date.get(trade_date)) or 0.0 for trade_date in trade_days]
    return_series = _returns_from_levels(level_series)
    running_drawdowns = _drawdown_series(level_series)

    rows: list[dict[str, object]] = []
    for idx, trade_date in enumerate(trade_days):
        level = levels_by_date.get(trade_date)
        if level is None:
            continue
        weights = weights_by_date.get(trade_date, {})
        benchmark_weights = benchmark_weights_by_date.get(trade_date, {})
        port_date = active_port_by_trade.get(trade_date)
        metadata_map = metadata_by_port_date.get(port_date or trade_date, {})
        price_quality = price_quality_by_date.get(trade_date, {})

        sector_weights = _aggregate_sector_weights(weights, metadata_map)
        benchmark_sector_weights = _aggregate_sector_weights(benchmark_weights, metadata_map)
        governance_values = {
            ticker: _safe_float(metadata_map.get(ticker).governance_score) if metadata_map.get(ticker) else None
            for ticker in weights
        }
        momentum_values = {
            ticker: factor_history.get(trade_date, {}).get(ticker, FactorPoint(None, None)).momentum_20d
            for ticker in weights
        }
        low_vol_values = {
            ticker: factor_history.get(trade_date, {}).get(ticker, FactorPoint(None, None)).low_vol_60d
            for ticker in weights
        }

        row = {
            "model_code": model_code,
            "model_name": model_name,
            "trade_date": trade_date,
            "rebalance_date": rebalance_by_date.get(trade_date),
            "level_tr": level,
            "ret_1d": return_series[idx],
            "ret_5d": _level_return(level_series, idx, 5),
            "ret_20d": _level_return(level_series, idx, 20),
            "ret_mtd": _level_return(level_series, idx, idx - month_starts[idx]),
            "ret_ytd": _level_return(level_series, idx, idx - year_starts[idx]),
            "vol_20d": _rolling_vol(return_series, idx, 20),
            "vol_60d": _rolling_vol(return_series, idx, 60),
            "drawdown_to_date": running_drawdowns[idx],
            "max_drawdown_252d": _max_drawdown(level_series, idx, MAX_DRAWDOWN_WINDOW),
            "n_constituents": len(weights),
            "n_imputed": sum(1 for ticker in weights if price_quality.get(ticker) == "IMPUTED"),
            "top1_weight": sum(sorted(weights.values(), reverse=True)[:1]) if weights else 0.0,
            "top5_weight": sum(sorted(weights.values(), reverse=True)[:5]) if weights else 0.0,
            "herfindahl": sum(weight * weight for weight in weights.values()) if weights else 0.0,
            "avg_governance_score": _weighted_average(weights, governance_values),
            "avg_momentum_20d": _weighted_average(weights, momentum_values),
            "avg_low_vol_60d": _weighted_average(weights, low_vol_values),
            "sector_count": len([sector for sector, weight in sector_weights.items() if weight > 0]),
            "factor_governance_exposure": _active_weighted_exposure(weights, benchmark_weights, governance_values),
            "factor_momentum_exposure": _active_weighted_exposure(weights, benchmark_weights, momentum_values),
            "factor_low_vol_exposure": _active_weighted_exposure(
                weights,
                benchmark_weights,
                {ticker: (1.0 / value) if value not in (None, 0) else None for ticker, value in low_vol_values.items()},
            ),
            "factor_sector_tilt_abs": _sector_tilt_abs(sector_weights, benchmark_sector_weights),
            "factor_concentration": sum(weight * weight for weight in weights.values()) if weights else 0.0,
        }

        if official_daily_by_date is not None and trade_date in official_daily_by_date:
            official = official_daily_by_date[trade_date]
            row.update(
                {
                    "ret_1d": official.ret_1d if official.ret_1d is not None else row["ret_1d"],
                    "ret_5d": official.ret_5d if official.ret_5d is not None else row["ret_5d"],
                    "ret_20d": official.ret_20d if official.ret_20d is not None else row["ret_20d"],
                    "vol_20d": official.vol_20d if official.vol_20d is not None else row["vol_20d"],
                    "max_drawdown_252d": official.max_drawdown_252d
                    if official.max_drawdown_252d is not None
                    else row["max_drawdown_252d"],
                    "n_constituents": official.n_constituents
                    if official.n_constituents is not None
                    else row["n_constituents"],
                    "n_imputed": official.n_imputed if official.n_imputed is not None else row["n_imputed"],
                    "top5_weight": official.top5_weight
                    if official.top5_weight is not None
                    else row["top5_weight"],
                    "herfindahl": official.herfindahl
                    if official.herfindahl is not None
                    else row["herfindahl"],
                }
            )

        rows.append(row)
    return rows


def _ticker_series(
    trade_days: Sequence[_dt.date],
    weights_by_date: Mapping[_dt.date, Mapping[str, float]],
    contrib_by_date: Mapping[_dt.date, Mapping[str, float]],
) -> dict[str, list[float]]:
    tickers = {
        ticker
        for by_date in weights_by_date.values()
        for ticker in by_date
    }
    tickers.update(
        ticker
        for by_date in contrib_by_date.values()
        for ticker in by_date
    )
    series = {ticker: [0.0] * len(trade_days) for ticker in tickers}
    date_index = {trade_date: idx for idx, trade_date in enumerate(trade_days)}
    for trade_date, contribs in contrib_by_date.items():
        idx = date_index.get(trade_date)
        if idx is None:
            continue
        for ticker, value in contribs.items():
            series.setdefault(ticker, [0.0] * len(trade_days))[idx] = float(value)
    return series


def _period_starts(trade_days: Sequence[_dt.date]) -> tuple[list[int], list[int]]:
    month_starts: list[int] = []
    year_starts: list[int] = []
    last_month = None
    last_year = None
    month_start_idx = 0
    year_start_idx = 0
    for idx, trade_date in enumerate(trade_days):
        month_key = (trade_date.year, trade_date.month)
        year_key = trade_date.year
        if month_key != last_month:
            month_start_idx = idx
            last_month = month_key
        if year_key != last_year:
            year_start_idx = idx
            last_year = year_key
        month_starts.append(month_start_idx)
        year_starts.append(year_start_idx)
    return month_starts, year_starts


def _window_sum(series: Sequence[float], idx: int, window: int) -> float:
    if not series:
        return 0.0
    start = max(0, idx - window + 1)
    return float(sum(series[start : idx + 1]))


def _returns_from_levels(level_series: Sequence[float]) -> list[float | None]:
    returns: list[float | None] = [None] * len(level_series)
    for idx in range(1, len(level_series)):
        prev = level_series[idx - 1]
        current = level_series[idx]
        if prev == 0:
            continue
        returns[idx] = current / prev - 1.0
    return returns


def _level_return(level_series: Sequence[float], idx: int, lookback: int) -> float | None:
    if idx <= 0 or lookback <= 0 or idx - lookback < 0:
        return None
    start_level = level_series[idx - lookback]
    end_level = level_series[idx]
    if start_level == 0:
        return None
    return end_level / start_level - 1.0


def _rolling_vol(returns: Sequence[float | None], idx: int, window: int) -> float | None:
    if idx < window:
        return None
    sample = [value for value in returns[idx - window + 1 : idx + 1] if value is not None]
    if len(sample) < window:
        return None
    return statistics.pstdev(sample) * math.sqrt(252.0)


def _drawdown_series(level_series: Sequence[float]) -> list[float]:
    peak = 0.0
    drawdowns: list[float] = []
    for level in level_series:
        peak = max(peak, level)
        if peak == 0:
            drawdowns.append(0.0)
        else:
            drawdowns.append(level / peak - 1.0)
    return drawdowns


def _max_drawdown(level_series: Sequence[float], idx: int, window: int) -> float | None:
    if not level_series:
        return None
    start = max(0, idx - window + 1)
    peak = 0.0
    max_drawdown = 0.0
    for level in level_series[start : idx + 1]:
        peak = max(peak, level)
        if peak:
            max_drawdown = min(max_drawdown, level / peak - 1.0)
    return max_drawdown


def _aggregate_sector_weights(
    weights: Mapping[str, float],
    metadata_map: Mapping[str, MetadataRow],
) -> dict[str, float]:
    totals: dict[str, float] = {}
    for ticker, weight in weights.items():
        sector = "Unclassified"
        metadata = metadata_map.get(ticker)
        if metadata and metadata.sector and str(metadata.sector).strip():
            sector = str(metadata.sector).strip()
        totals[sector] = totals.get(sector, 0.0) + float(weight)
    return totals


def _weighted_average(
    weights: Mapping[str, float],
    values: Mapping[str, float | None],
) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for ticker, weight in weights.items():
        value = values.get(ticker)
        if value is None:
            continue
        numerator += float(weight) * float(value)
        denominator += float(weight)
    if denominator <= 0:
        return None
    return numerator / denominator


def _active_weighted_exposure(
    weights: Mapping[str, float],
    benchmark_weights: Mapping[str, float],
    values: Mapping[str, float | None],
) -> float | None:
    available = [value for value in values.values() if value is not None]
    if not available:
        return None
    mean = sum(available) / len(available)
    variance = sum((value - mean) ** 2 for value in available) / max(len(available), 1)
    stdev = math.sqrt(variance)
    if stdev == 0:
        return 0.0

    exposure = 0.0
    has_value = False
    for ticker, value in values.items():
        if value is None:
            continue
        has_value = True
        z_score = (value - mean) / stdev
        active_weight = float(weights.get(ticker, 0.0)) - float(benchmark_weights.get(ticker, 0.0))
        exposure += active_weight * z_score
    return exposure if has_value else None


def _sector_tilt_abs(
    sector_weights: Mapping[str, float],
    benchmark_sector_weights: Mapping[str, float],
) -> float:
    sectors = set(sector_weights) | set(benchmark_sector_weights)
    return 0.5 * sum(
        abs(float(sector_weights.get(sector, 0.0)) - float(benchmark_sector_weights.get(sector, 0.0)))
        for sector in sectors
    )


__all__ = [
    "DEFAULT_MODEL_SPECS",
    "INDEX_CODE",
    "LOW_VOL_WINDOW",
    "MAX_DRAWDOWN_WINDOW",
    "MOMENTUM_WINDOW",
    "MetadataRow",
    "ModelSpec",
    "OfficialDailyRow",
    "OfficialPositionRow",
    "PriceRow",
    "build_constraint_rows",
    "build_model_path",
    "build_model_target_weights",
    "build_optimizer_inputs",
    "build_portfolio_outputs",
    "compute_factor_history",
    "normalize_weights",
]
