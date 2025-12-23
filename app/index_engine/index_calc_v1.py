"""Pure-python index calculation helpers for TECH100 v1."""
from __future__ import annotations

import datetime as _dt
import math
from typing import Dict, Sequence


def build_rebalance_schedule(
    trading_days: Sequence[_dt.date],
    port_dates: Sequence[_dt.date],
) -> list[_dt.date]:
    """Return trading days that represent a rebalance (when port_date changes)."""
    if not trading_days or not port_dates:
        return []
    rebalance_dates: list[_dt.date] = []
    port_dates_sorted = sorted(port_dates)
    current_port: _dt.date | None = None
    port_idx = 0

    for trade_date in sorted(trading_days):
        while port_idx < len(port_dates_sorted) and port_dates_sorted[port_idx] <= trade_date:
            current_port = port_dates_sorted[port_idx]
            port_idx += 1
        if current_port is None:
            continue
        if not rebalance_dates or rebalance_dates[-1] != trade_date and current_port != _latest_port_date_before(
            port_dates_sorted, trade_date, exclude_current=True
        ):
            rebalance_dates.append(trade_date)
    return rebalance_dates


def _latest_port_date_before(
    port_dates_sorted: Sequence[_dt.date],
    trade_date: _dt.date,
    *,
    exclude_current: bool,
) -> _dt.date | None:
    latest: _dt.date | None = None
    for pd in port_dates_sorted:
        if pd > trade_date:
            break
        if exclude_current and pd == trade_date:
            continue
        latest = pd
    return latest


def compute_holdings_at_rebalance(
    *,
    tickers: Sequence[str],
    prices_prev: Dict[str, float],
    level_prev: float,
    divisor_prev: float,
) -> tuple[Dict[str, float], float]:
    """Compute equal-weighted shares and divisor for a rebalance date."""
    if not tickers:
        raise ValueError("no_tickers")
    n = len(tickers)
    target_weight = 1.0 / n
    mv_prev = level_prev * divisor_prev

    shares: Dict[str, float] = {}
    for ticker in tickers:
        price = prices_prev.get(ticker)
        if price is None or price <= 0:
            raise ValueError(f"missing_price_prev:{ticker}")
        shares[ticker] = (target_weight * mv_prev) / price

    mv_new = sum(shares[ticker] * prices_prev[ticker] for ticker in tickers)
    if mv_new <= 0:
        raise ValueError("invalid_mv_new")
    divisor = mv_new / level_prev
    return shares, divisor


def compute_levels(
    *,
    trading_days: Sequence[_dt.date],
    holdings_by_rebalance: Dict[_dt.date, Dict[str, float]],
    divisors_by_rebalance: Dict[_dt.date, float],
    prices_by_date: Dict[_dt.date, Dict[str, float]],
) -> Dict[_dt.date, float]:
    """Compute level_tr for each trading day."""
    levels: Dict[_dt.date, float] = {}
    rebalance_dates = sorted(holdings_by_rebalance.keys())
    current_reb: _dt.date | None = None
    for trade_date in sorted(trading_days):
        for reb_date in rebalance_dates:
            if reb_date <= trade_date:
                current_reb = reb_date
            else:
                break
        if current_reb is None:
            continue
        shares = holdings_by_rebalance[current_reb]
        divisor = divisors_by_rebalance[current_reb]
        prices = prices_by_date.get(trade_date, {})
        mv = sum(shares[ticker] * prices[ticker] for ticker in shares if ticker in prices)
        levels[trade_date] = mv / divisor if divisor else 0.0
    return levels


def compute_constituent_daily(
    *,
    trading_days: Sequence[_dt.date],
    holdings_by_rebalance: Dict[_dt.date, Dict[str, float]],
    prices_by_date: Dict[_dt.date, Dict[str, float]],
) -> Dict[_dt.date, Dict[str, float]]:
    """Compute weights per day from holdings and prices."""
    weights_by_date: Dict[_dt.date, Dict[str, float]] = {}
    rebalance_dates = sorted(holdings_by_rebalance.keys())
    current_reb: _dt.date | None = None
    for trade_date in sorted(trading_days):
        for reb_date in rebalance_dates:
            if reb_date <= trade_date:
                current_reb = reb_date
            else:
                break
        if current_reb is None:
            continue
        shares = holdings_by_rebalance[current_reb]
        prices = prices_by_date.get(trade_date, {})
        mv_by_ticker = {t: shares[t] * prices[t] for t in shares if t in prices}
        total_mv = sum(mv_by_ticker.values())
        if total_mv <= 0:
            continue
        weights_by_date[trade_date] = {t: mv / total_mv for t, mv in mv_by_ticker.items()}
    return weights_by_date


def compute_contributions(
    *,
    trading_days: Sequence[_dt.date],
    weights_by_date: Dict[_dt.date, Dict[str, float]],
    prices_by_date: Dict[_dt.date, Dict[str, float]],
) -> Dict[_dt.date, Dict[str, float]]:
    """Compute per-ticker contribution based on prev-day weights and returns."""
    contributions: Dict[_dt.date, Dict[str, float]] = {}
    ordered = sorted(trading_days)
    for idx in range(1, len(ordered)):
        prev_date = ordered[idx - 1]
        trade_date = ordered[idx]
        weights_prev = weights_by_date.get(prev_date, {})
        prices_prev = prices_by_date.get(prev_date, {})
        prices_now = prices_by_date.get(trade_date, {})
        daily: Dict[str, float] = {}
        for ticker, weight in weights_prev.items():
            p0 = prices_prev.get(ticker)
            p1 = prices_now.get(ticker)
            if p0 is None or p1 is None or p0 == 0:
                continue
            ret = p1 / p0 - 1.0
            daily[ticker] = weight * ret
        contributions[trade_date] = daily
    return contributions


def compute_stats(
    *,
    trading_days: Sequence[_dt.date],
    levels: Dict[_dt.date, float],
    weights_by_date: Dict[_dt.date, Dict[str, float]],
    returns_1d: Dict[_dt.date, float],
) -> Dict[_dt.date, Dict[str, float]]:
    """Compute rolling stats for each day."""
    stats: Dict[_dt.date, Dict[str, float]] = {}
    ordered = sorted(trading_days)
    for idx, trade_date in enumerate(ordered):
        level = levels.get(trade_date)
        ret_1d = returns_1d.get(trade_date)
        ret_5d = _rolling_return(levels, ordered, idx, 5)
        ret_20d = _rolling_return(levels, ordered, idx, 20)
        vol_20d = _rolling_vol(returns_1d, ordered, idx, 20)
        weights = weights_by_date.get(trade_date, {})
        n_const = len(weights)
        top5_weight = sum(sorted(weights.values(), reverse=True)[:5]) if weights else 0.0
        herfindahl = sum(w * w for w in weights.values()) if weights else 0.0
        stats[trade_date] = {
            "level_tr": level or 0.0,
            "ret_1d": ret_1d,
            "ret_5d": ret_5d,
            "ret_20d": ret_20d,
            "vol_20d": vol_20d,
            "n_constituents": n_const,
            "top5_weight": top5_weight,
            "herfindahl": herfindahl,
        }
    return stats


def _rolling_return(levels: Dict[_dt.date, float], ordered: list[_dt.date], idx: int, window: int) -> float | None:
    if idx < window:
        return None
    prev = levels.get(ordered[idx - window])
    current = levels.get(ordered[idx])
    if prev in (None, 0) or current is None:
        return None
    return current / prev - 1.0


def _rolling_vol(
    returns_1d: Dict[_dt.date, float],
    ordered: list[_dt.date],
    idx: int,
    window: int,
) -> float | None:
    if idx < window:
        return None
    sample = [returns_1d.get(ordered[i]) for i in range(idx - window + 1, idx + 1)]
    series = [r for r in sample if r is not None]
    if len(series) < window:
        return None
    mean = sum(series) / len(series)
    var = sum((r - mean) ** 2 for r in series) / len(series)
    return math.sqrt(var)


__all__ = [
    "build_rebalance_schedule",
    "compute_holdings_at_rebalance",
    "compute_levels",
    "compute_constituent_daily",
    "compute_contributions",
    "compute_stats",
]
