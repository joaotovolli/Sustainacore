from __future__ import annotations

import datetime as dt
import math
import os
from functools import lru_cache
from typing import Iterable, Optional

from django.core.cache import cache

from core.oracle_db import get_connection

MODEL_ORDER = (
    "TECH100",
    "TECH100_EQ",
    "TECH100_GOV",
    "TECH100_MOM",
    "TECH100_LOWVOL",
    "TECH100_GOV_MOM",
)

MODEL_METADATA = {
    "TECH100": {
        "label": "Official TECH100",
        "short_label": "Official",
        "description": "Benchmark portfolio derived from the live TECH100 index.",
        "color": "#183153",
    },
    "TECH100_EQ": {
        "label": "Equal Weight",
        "short_label": "Equal Weight",
        "description": "Current TECH100 names rebalanced to equal weights.",
        "color": "#2f6f6f",
    },
    "TECH100_GOV": {
        "label": "Governance Tilt",
        "short_label": "Governance",
        "description": "Benchmark weights tilted by the existing governance composite.",
        "color": "#3b8f57",
    },
    "TECH100_MOM": {
        "label": "Momentum Tilt",
        "short_label": "Momentum",
        "description": "Benchmark weights tilted by the supported 20-day momentum signal.",
        "color": "#b06c1f",
    },
    "TECH100_LOWVOL": {
        "label": "Low Volatility Tilt",
        "short_label": "Low Vol",
        "description": "Benchmark weights tilted toward the supported 60-day low-vol signal.",
        "color": "#7f4ea3",
    },
    "TECH100_GOV_MOM": {
        "label": "Governance + Momentum",
        "short_label": "Gov + Mom",
        "description": "Hybrid model using the existing governance and momentum ranks.",
        "color": "#b44a3c",
    },
}

CACHE_TTLS = {
    "latest_date": 300,
    "snapshot": 300,
    "timeseries": 600,
    "positions": 300,
    "sectors": 300,
    "optimizer_summary": 300,
    "constraints": 600,
}

FIXTURE_LATEST_DATE = dt.date(2026, 3, 16)
FIXTURE_REBALANCE_DATE = dt.date(2026, 3, 2)

SUPPORTED_ANALYTICS = [
    "Official TECH100 benchmark",
    "Equal weight, governance tilt, momentum tilt, low volatility tilt, and governance + momentum model portfolios",
    "Rolling volatility, drawdown, and concentration analytics",
    "Governance composite summaries from the existing TECH100 dataset",
    "Sector weights and sector contribution when sector rows are present",
    "Attribution windows for 1D, 5D, 20D, MTD, and YTD",
    "Precomputed optimizer-ready inputs and portfolio constraints",
]

DEFERRED_ANALYTICS = [
    "Classical value, fundamental quality, dividend yield, and small-cap factor families",
    "Fundamentally weighted portfolios",
    "A live optimizer or on-demand portfolio solver",
    "A full institutional factor risk model",
]

CONSTRAINT_LABELS = {
    "FULLY_INVESTED": "Fully invested",
    "LONG_ONLY": "Long only",
    "MAX_NAME_WEIGHT": "Max single-name weight",
    "MIN_NAME_WEIGHT": "Min single-name weight",
    "WEIGHTING_RULE": "Weighting rule",
}

FIXTURE_UNIVERSE = [
    {
        "ticker": "NVDA",
        "company_name": "NVIDIA",
        "sector": "Semiconductors",
        "benchmark_weight": 0.124,
        "governance_score": 73.4,
        "momentum_20d": 0.118,
        "low_vol_60d": 0.246,
        "ret_1d": 0.010,
        "ret_5d": 0.026,
        "ret_20d": 0.082,
        "ret_mtd": 0.061,
        "ret_ytd": 0.173,
        "price_quality": "REAL",
    },
    {
        "ticker": "MSFT",
        "company_name": "Microsoft",
        "sector": "Software",
        "benchmark_weight": 0.119,
        "governance_score": 87.9,
        "momentum_20d": 0.052,
        "low_vol_60d": 0.172,
        "ret_1d": 0.004,
        "ret_5d": 0.011,
        "ret_20d": 0.036,
        "ret_mtd": 0.024,
        "ret_ytd": 0.091,
        "price_quality": "REAL",
    },
    {
        "ticker": "GOOGL",
        "company_name": "Alphabet",
        "sector": "Internet",
        "benchmark_weight": 0.104,
        "governance_score": 78.2,
        "momentum_20d": 0.046,
        "low_vol_60d": 0.188,
        "ret_1d": 0.003,
        "ret_5d": 0.010,
        "ret_20d": 0.031,
        "ret_mtd": 0.021,
        "ret_ytd": 0.082,
        "price_quality": "REAL",
    },
    {
        "ticker": "META",
        "company_name": "Meta Platforms",
        "sector": "Internet",
        "benchmark_weight": 0.096,
        "governance_score": 69.4,
        "momentum_20d": 0.072,
        "low_vol_60d": 0.225,
        "ret_1d": 0.007,
        "ret_5d": 0.017,
        "ret_20d": 0.056,
        "ret_mtd": 0.041,
        "ret_ytd": 0.121,
        "price_quality": "REAL",
    },
    {
        "ticker": "AVGO",
        "company_name": "Broadcom",
        "sector": "Semiconductors",
        "benchmark_weight": 0.082,
        "governance_score": 76.5,
        "momentum_20d": 0.064,
        "low_vol_60d": 0.214,
        "ret_1d": 0.008,
        "ret_5d": 0.019,
        "ret_20d": 0.061,
        "ret_mtd": 0.044,
        "ret_ytd": 0.136,
        "price_quality": "REAL",
    },
    {
        "ticker": "AMD",
        "company_name": "AMD",
        "sector": "Semiconductors",
        "benchmark_weight": 0.076,
        "governance_score": 71.8,
        "momentum_20d": 0.084,
        "low_vol_60d": 0.282,
        "ret_1d": 0.012,
        "ret_5d": 0.030,
        "ret_20d": 0.095,
        "ret_mtd": 0.070,
        "ret_ytd": 0.189,
        "price_quality": "REAL",
    },
    {
        "ticker": "CRM",
        "company_name": "Salesforce",
        "sector": "Software",
        "benchmark_weight": 0.071,
        "governance_score": 82.7,
        "momentum_20d": 0.031,
        "low_vol_60d": 0.164,
        "ret_1d": 0.002,
        "ret_5d": 0.008,
        "ret_20d": 0.024,
        "ret_mtd": 0.018,
        "ret_ytd": 0.063,
        "price_quality": "REAL",
    },
    {
        "ticker": "ORCL",
        "company_name": "Oracle",
        "sector": "Software",
        "benchmark_weight": 0.068,
        "governance_score": 79.6,
        "momentum_20d": 0.028,
        "low_vol_60d": 0.158,
        "ret_1d": 0.002,
        "ret_5d": 0.006,
        "ret_20d": 0.021,
        "ret_mtd": 0.015,
        "ret_ytd": 0.058,
        "price_quality": "REAL",
    },
    {
        "ticker": "ADBE",
        "company_name": "Adobe",
        "sector": "Software",
        "benchmark_weight": 0.064,
        "governance_score": 80.4,
        "momentum_20d": 0.018,
        "low_vol_60d": 0.149,
        "ret_1d": 0.001,
        "ret_5d": 0.004,
        "ret_20d": 0.015,
        "ret_mtd": 0.012,
        "ret_ytd": 0.049,
        "price_quality": "REAL",
    },
    {
        "ticker": "IBM",
        "company_name": "IBM",
        "sector": "Services",
        "benchmark_weight": 0.056,
        "governance_score": 89.1,
        "momentum_20d": 0.014,
        "low_vol_60d": 0.141,
        "ret_1d": 0.001,
        "ret_5d": 0.003,
        "ret_20d": 0.012,
        "ret_mtd": 0.010,
        "ret_ytd": 0.044,
        "price_quality": "REAL",
    },
    {
        "ticker": "TSM",
        "company_name": "TSMC",
        "sector": "Semiconductors",
        "benchmark_weight": 0.073,
        "governance_score": 84.0,
        "momentum_20d": 0.058,
        "low_vol_60d": 0.196,
        "ret_1d": 0.006,
        "ret_5d": 0.014,
        "ret_20d": 0.041,
        "ret_mtd": 0.030,
        "ret_ytd": 0.099,
        "price_quality": "IMPUTED",
    },
    {
        "ticker": "INTU",
        "company_name": "Intuit",
        "sector": "Software",
        "benchmark_weight": 0.067,
        "governance_score": 86.1,
        "momentum_20d": 0.024,
        "low_vol_60d": 0.152,
        "ret_1d": 0.002,
        "ret_5d": 0.005,
        "ret_20d": 0.017,
        "ret_mtd": 0.013,
        "ret_ytd": 0.052,
        "price_quality": "REAL",
    },
]


def _data_mode() -> str:
    return os.getenv("TECH100_UI_DATA_MODE", "oracle").lower()


def get_data_mode() -> str:
    return _data_mode()


def _cache_key(*parts: str) -> str:
    return "tech100_portfolio:" + ":".join(parts)


def _coerce_date(value) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rank_multipliers(values: dict[str, float], *, higher_is_better: bool) -> dict[str, float]:
    ordered = sorted(
        values.items(),
        key=lambda item: (item[1], item[0]),
        reverse=higher_is_better,
    )
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][0]: 1.0}
    multipliers: dict[str, float] = {}
    for idx, (ticker, _value) in enumerate(ordered):
        pct = 1.0 - (idx / max(len(ordered) - 1, 1))
        multipliers[ticker] = 0.5 + pct
    return multipliers


def _normalize_weights(raw_weights: dict[str, float]) -> dict[str, float]:
    total = sum(max(value, 0.0) for value in raw_weights.values())
    if total <= 0:
        return {}
    return {ticker: max(value, 0.0) / total for ticker, value in raw_weights.items() if value > 0}


def _execute_rows(sql: str, params: Optional[dict] = None) -> list[tuple]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        return cur.fetchall()


def _is_missing_object_error(exc: Exception) -> bool:
    code = getattr(exc, "code", None)
    if code in {942, 4043}:
        return True
    text = str(exc)
    return "ORA-00942" in text or "ORA-04043" in text


def _safe_execute_rows(sql: str, params: Optional[dict] = None) -> list[tuple]:
    try:
        return _execute_rows(sql, params)
    except Exception as exc:
        if _is_missing_object_error(exc):
            return []
        raise


def _safe_cached(key: str, ttl: int, loader):
    cached = cache.get(key)
    if cached is not None:
        return cached
    value = loader()
    cache.set(key, value, ttl)
    return value


def _order_case_sql(alias: str = "model_code") -> str:
    parts = ["CASE"]
    for idx, model_code in enumerate(MODEL_ORDER, start=1):
        parts.append(f"WHEN {alias} = '{model_code}' THEN {idx}")
    parts.append("ELSE 99 END")
    return " ".join(parts)


def _model_metadata(code: str) -> dict[str, str]:
    meta = MODEL_METADATA.get(code, {})
    return {
        "code": code,
        "label": meta.get("label", code),
        "short_label": meta.get("short_label", code),
        "description": meta.get("description", ""),
        "color": meta.get("color", "#183153"),
    }


def get_model_definitions() -> list[dict[str, str]]:
    return [_model_metadata(code) for code in MODEL_ORDER]


def get_supported_analytics() -> list[str]:
    return SUPPORTED_ANALYTICS[:]


def get_deferred_analytics() -> list[str]:
    return DEFERRED_ANALYTICS[:]


def _series_return(levels: list[float], offset: int) -> Optional[float]:
    if len(levels) <= offset or levels[-offset - 1] == 0:
        return None
    return levels[-1] / levels[-offset - 1] - 1.0


def _rolling_vol(returns: list[float], window: int) -> Optional[float]:
    if len(returns) < window:
        return None
    sample = returns[-window:]
    mean = sum(sample) / len(sample)
    variance = sum((value - mean) ** 2 for value in sample) / len(sample)
    return math.sqrt(variance) * math.sqrt(252)


def _drawdown_series(levels: list[float]) -> list[float]:
    if not levels:
        return []
    peak = levels[0]
    output = []
    for value in levels:
        peak = max(peak, value)
        output.append((value / peak) - 1.0 if peak else 0.0)
    return output


def _max_drawdown(levels: list[float], window: int = 252) -> Optional[float]:
    if not levels:
        return None
    sample = levels[-window:] if len(levels) > window else levels
    return min(_drawdown_series(sample))


def _month_start(target: dt.date) -> dt.date:
    return dt.date(target.year, target.month, 1)


def _year_start(target: dt.date) -> dt.date:
    return dt.date(target.year, 1, 1)


@lru_cache(maxsize=1)
def _fixture_trade_days() -> tuple[dt.date, ...]:
    days: list[dt.date] = []
    cursor = FIXTURE_LATEST_DATE
    while len(days) < 180:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= dt.timedelta(days=1)
    days.reverse()
    return tuple(days)


@lru_cache(maxsize=1)
def _fixture_position_rows() -> dict[str, list[dict[str, object]]]:
    governance_scores = {
        row["ticker"]: float(row["governance_score"]) for row in FIXTURE_UNIVERSE
    }
    momentum_scores = {
        row["ticker"]: float(row["momentum_20d"]) for row in FIXTURE_UNIVERSE
    }
    low_vol_scores = {
        row["ticker"]: float(row["low_vol_60d"]) for row in FIXTURE_UNIVERSE
    }
    benchmark_weights = {
        row["ticker"]: float(row["benchmark_weight"]) for row in FIXTURE_UNIVERSE
    }
    governance_mult = _rank_multipliers(governance_scores, higher_is_better=True)
    momentum_mult = _rank_multipliers(momentum_scores, higher_is_better=True)
    low_vol_mult = _rank_multipliers(low_vol_scores, higher_is_better=False)

    outputs: dict[str, list[dict[str, object]]] = {}
    for model_code in MODEL_ORDER:
        if model_code == "TECH100":
            weights = benchmark_weights
        elif model_code == "TECH100_EQ":
            equal_weight = 1.0 / len(FIXTURE_UNIVERSE)
            weights = {row["ticker"]: equal_weight for row in FIXTURE_UNIVERSE}
        else:
            raw_weights: dict[str, float] = {}
            for row in FIXTURE_UNIVERSE:
                ticker = row["ticker"]
                base_weight = benchmark_weights[ticker]
                if model_code == "TECH100_GOV":
                    multiplier = governance_mult[ticker]
                elif model_code == "TECH100_MOM":
                    multiplier = momentum_mult[ticker]
                elif model_code == "TECH100_LOWVOL":
                    multiplier = low_vol_mult[ticker]
                else:
                    multiplier = governance_mult[ticker] * momentum_mult[ticker]
                raw_weights[ticker] = base_weight * multiplier
            weights = _normalize_weights(raw_weights)

        rows: list[dict[str, object]] = []
        meta = _model_metadata(model_code)
        for item in FIXTURE_UNIVERSE:
            ticker = item["ticker"]
            model_weight = float(weights.get(ticker, 0.0))
            benchmark_weight = float(item["benchmark_weight"])
            rows.append(
                {
                    "model_code": model_code,
                    "trade_date": FIXTURE_LATEST_DATE,
                    "ticker": ticker,
                    "model_name": meta["label"],
                    "rebalance_date": FIXTURE_REBALANCE_DATE,
                    "company_name": item["company_name"],
                    "sector": item["sector"],
                    "model_weight": model_weight,
                    "benchmark_weight": benchmark_weight,
                    "active_weight": model_weight - benchmark_weight,
                    "price_quality": item["price_quality"],
                    "ret_1d": item["ret_1d"],
                    "contrib_1d": model_weight * float(item["ret_1d"]),
                    "contrib_5d": model_weight * float(item["ret_5d"]),
                    "contrib_20d": model_weight * float(item["ret_20d"]),
                    "contrib_mtd": model_weight * float(item["ret_mtd"]),
                    "contrib_ytd": model_weight * float(item["ret_ytd"]),
                    "governance_score": item["governance_score"],
                    "momentum_20d": item["momentum_20d"],
                    "low_vol_60d": item["low_vol_60d"],
                }
            )
        rows.sort(key=lambda row: (-float(row["model_weight"]), str(row["ticker"])))
        outputs[model_code] = rows
    return outputs


@lru_cache(maxsize=1)
def _fixture_series_map() -> dict[str, list[dict[str, object]]]:
    trade_days = list(_fixture_trade_days())
    position_rows = _fixture_position_rows()
    outputs: dict[str, list[dict[str, object]]] = {}
    month_start = _month_start(FIXTURE_LATEST_DATE)
    year_start = _year_start(FIXTURE_LATEST_DATE)
    month_start_idx = next((idx for idx, day in enumerate(trade_days) if day >= month_start), 0)
    year_start_idx = next((idx for idx, day in enumerate(trade_days) if day >= year_start), 0)

    model_offsets = {
        "TECH100": 0.0,
        "TECH100_EQ": -0.00002,
        "TECH100_GOV": 0.00004,
        "TECH100_MOM": 0.00006,
        "TECH100_LOWVOL": -0.00001,
        "TECH100_GOV_MOM": 0.00008,
    }

    model_factors = {
        "TECH100": 1.00,
        "TECH100_EQ": 0.92,
        "TECH100_GOV": 0.96,
        "TECH100_MOM": 1.12,
        "TECH100_LOWVOL": 0.82,
        "TECH100_GOV_MOM": 1.04,
    }

    for model_code in MODEL_ORDER:
        levels: list[float] = []
        daily_returns: list[float] = []
        level = 1000.0
        holdings = position_rows[model_code]
        avg_governance = sum(float(row["model_weight"]) * float(row["governance_score"]) for row in holdings)
        avg_momentum = sum(float(row["model_weight"]) * float(row["momentum_20d"]) for row in holdings)
        avg_low_vol = sum(float(row["model_weight"]) * float(row["low_vol_60d"]) for row in holdings)
        top_weights = sorted((float(row["model_weight"]) for row in holdings), reverse=True)
        sector_count = len({str(row["sector"]) for row in holdings if row.get("sector")})
        n_imputed = sum(1 for row in holdings if row["price_quality"] == "IMPUTED")
        herfindahl = sum(float(row["model_weight"]) ** 2 for row in holdings)
        base_exposure = herfindahl / 0.09
        factor = model_factors[model_code]
        offset = model_offsets[model_code]
        rows: list[dict[str, object]] = []

        for idx, trade_date in enumerate(trade_days):
            raw_ret = (
                0.00055
                + offset
                + 0.0016 * math.sin((idx + factor * 3.0) / 10.0)
                + 0.0008 * math.cos((idx + factor) / 5.0)
            )
            clipped_ret = max(min(raw_ret * factor, 0.018), -0.017)
            level *= 1.0 + clipped_ret
            levels.append(level)
            daily_returns.append(clipped_ret)

            ret_5d = _series_return(levels, 5)
            ret_20d = _series_return(levels, 20)
            ret_mtd = None
            if idx > month_start_idx and levels[month_start_idx] != 0:
                ret_mtd = level / levels[month_start_idx] - 1.0
            ret_ytd = None
            if idx > year_start_idx and levels[year_start_idx] != 0:
                ret_ytd = level / levels[year_start_idx] - 1.0

            rows.append(
                {
                    "model_code": model_code,
                    "trade_date": trade_date,
                    "model_name": _model_metadata(model_code)["label"],
                    "rebalance_date": FIXTURE_REBALANCE_DATE,
                    "level_tr": level,
                    "ret_1d": clipped_ret,
                    "ret_5d": ret_5d,
                    "ret_20d": ret_20d,
                    "ret_mtd": ret_mtd,
                    "ret_ytd": ret_ytd,
                    "vol_20d": _rolling_vol(daily_returns, 20),
                    "vol_60d": _rolling_vol(daily_returns, 60),
                    "drawdown_to_date": _drawdown_series(levels)[-1],
                    "max_drawdown_252d": _max_drawdown(levels, 252),
                    "n_constituents": len(holdings),
                    "n_imputed": n_imputed,
                    "top1_weight": top_weights[0],
                    "top5_weight": sum(top_weights[:5]),
                    "herfindahl": herfindahl,
                    "avg_governance_score": avg_governance,
                    "avg_momentum_20d": avg_momentum,
                    "avg_low_vol_60d": avg_low_vol,
                    "sector_count": sector_count,
                    "factor_governance_exposure": (avg_governance / 80.0) - 1.0,
                    "factor_momentum_exposure": avg_momentum,
                    "factor_low_vol_exposure": 0.22 - avg_low_vol,
                    "factor_sector_tilt_abs": sum(abs(float(row["active_weight"])) for row in holdings) / 2.0,
                    "factor_concentration": base_exposure,
                    "computed_at": dt.datetime.combine(trade_date, dt.time(17, 20)),
                }
            )
        outputs[model_code] = rows
    return outputs


@lru_cache(maxsize=1)
def _fixture_sector_rows() -> dict[str, list[dict[str, object]]]:
    outputs: dict[str, list[dict[str, object]]] = {}
    for model_code, rows in _fixture_position_rows().items():
        aggregates: dict[str, dict[str, object]] = {}
        for row in rows:
            sector = str(row["sector"] or "Unclassified")
            current = aggregates.setdefault(
                sector,
                {
                    "model_code": model_code,
                    "trade_date": FIXTURE_LATEST_DATE,
                    "sector": sector,
                    "sector_weight": 0.0,
                    "benchmark_sector_weight": 0.0,
                    "active_sector_weight": 0.0,
                    "contrib_1d": 0.0,
                    "contrib_5d": 0.0,
                    "contrib_20d": 0.0,
                    "contrib_mtd": 0.0,
                    "contrib_ytd": 0.0,
                },
            )
            current["sector_weight"] += float(row["model_weight"])
            current["benchmark_sector_weight"] += float(row["benchmark_weight"])
            current["active_sector_weight"] += float(row["active_weight"])
            current["contrib_1d"] += float(row["contrib_1d"])
            current["contrib_5d"] += float(row["contrib_5d"])
            current["contrib_20d"] += float(row["contrib_20d"])
            current["contrib_mtd"] += float(row["contrib_mtd"])
            current["contrib_ytd"] += float(row["contrib_ytd"])
        outputs[model_code] = sorted(
            aggregates.values(),
            key=lambda row: (-abs(float(row["active_sector_weight"])), str(row["sector"])),
        )
    return outputs


def _fixture_constraints() -> list[dict[str, object]]:
    weighting_rules = {
        "TECH100": "BENCHMARK",
        "TECH100_EQ": "EQUAL_WEIGHT",
        "TECH100_GOV": "BENCHMARK_X_GOVERNANCE_RANK",
        "TECH100_MOM": "BENCHMARK_X_MOMENTUM_RANK",
        "TECH100_LOWVOL": "BENCHMARK_X_LOW_VOL_RANK",
        "TECH100_GOV_MOM": "BENCHMARK_X_GOVERNANCE_RANK_X_MOMENTUM_RANK",
    }
    rows: list[dict[str, object]] = []
    for model_code in MODEL_ORDER:
        rows.extend(
            [
                {
                    "model_code": model_code,
                    "constraint_key": "FULLY_INVESTED",
                    "constraint_type": "FLAG",
                    "constraint_value": "TRUE",
                },
                {
                    "model_code": model_code,
                    "constraint_key": "LONG_ONLY",
                    "constraint_type": "FLAG",
                    "constraint_value": "TRUE",
                },
                {
                    "model_code": model_code,
                    "constraint_key": "MAX_NAME_WEIGHT",
                    "constraint_type": "PCT",
                    "constraint_value": "0.1500",
                },
                {
                    "model_code": model_code,
                    "constraint_key": "MIN_NAME_WEIGHT",
                    "constraint_type": "PCT",
                    "constraint_value": "0.0050",
                },
                {
                    "model_code": model_code,
                    "constraint_key": "WEIGHTING_RULE",
                    "constraint_type": "TEXT",
                    "constraint_value": weighting_rules[model_code],
                },
            ]
        )
    return rows


def _fixture_optimizer_summary() -> dict[str, object]:
    rows = FIXTURE_UNIVERSE
    eligible_count = sum(1 for row in rows if row["price_quality"] == "REAL")
    avg_governance = sum(float(row["governance_score"]) for row in rows) / len(rows)
    avg_momentum = sum(float(row["momentum_20d"]) for row in rows) / len(rows)
    avg_low_vol = sum(float(row["low_vol_60d"]) for row in rows) / len(rows)
    return {
        "trade_date": FIXTURE_LATEST_DATE,
        "input_count": len(rows),
        "eligible_count": eligible_count,
        "avg_governance_score": avg_governance,
        "avg_momentum_20d": avg_momentum,
        "avg_low_vol_60d": avg_low_vol,
    }


def get_latest_trade_date() -> Optional[dt.date]:
    if _data_mode() == "fixture":
        return FIXTURE_LATEST_DATE

    def _load() -> Optional[dt.date]:
        sql = "SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY"
        rows = _safe_execute_rows(sql)
        if not rows or not rows[0]:
            return None
        return _coerce_date(rows[0][0])

    return _safe_cached(_cache_key("latest_date"), CACHE_TTLS["latest_date"], _load)


def get_snapshot_rows(trade_date: Optional[dt.date] = None) -> list[dict[str, object]]:
    if _data_mode() == "fixture":
        latest = trade_date or FIXTURE_LATEST_DATE
        rows = [series[-1] for series in _fixture_series_map().values() if series[-1]["trade_date"] == latest]
        return sorted(rows, key=lambda row: MODEL_ORDER.index(str(row["model_code"])))

    latest = trade_date or get_latest_trade_date()
    if latest is None:
        return []

    cache_key = _cache_key("snapshot", latest.isoformat())

    def _load() -> list[dict[str, object]]:
        sql = (
            "SELECT model_code, trade_date, model_name, rebalance_date, level_tr, ret_1d, ret_5d, ret_20d, "
            "       ret_mtd, ret_ytd, vol_20d, vol_60d, drawdown_to_date, max_drawdown_252d, n_constituents, "
            "       n_imputed, top1_weight, top5_weight, herfindahl, avg_governance_score, avg_momentum_20d, "
            "       avg_low_vol_60d, sector_count, factor_governance_exposure, factor_momentum_exposure, "
            "       factor_low_vol_exposure, factor_sector_tilt_abs, factor_concentration, computed_at "
            "FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY "
            "WHERE trade_date = :trade_date "
            f"ORDER BY {_order_case_sql('model_code')}"
        )
        output = []
        for row in _safe_execute_rows(sql, {"trade_date": latest}):
            output.append(
                {
                    "model_code": row[0],
                    "trade_date": _coerce_date(row[1]),
                    "model_name": row[2],
                    "rebalance_date": _coerce_date(row[3]),
                    "level_tr": _safe_float(row[4]),
                    "ret_1d": _safe_float(row[5]),
                    "ret_5d": _safe_float(row[6]),
                    "ret_20d": _safe_float(row[7]),
                    "ret_mtd": _safe_float(row[8]),
                    "ret_ytd": _safe_float(row[9]),
                    "vol_20d": _safe_float(row[10]),
                    "vol_60d": _safe_float(row[11]),
                    "drawdown_to_date": _safe_float(row[12]),
                    "max_drawdown_252d": _safe_float(row[13]),
                    "n_constituents": row[14],
                    "n_imputed": row[15],
                    "top1_weight": _safe_float(row[16]),
                    "top5_weight": _safe_float(row[17]),
                    "herfindahl": _safe_float(row[18]),
                    "avg_governance_score": _safe_float(row[19]),
                    "avg_momentum_20d": _safe_float(row[20]),
                    "avg_low_vol_60d": _safe_float(row[21]),
                    "sector_count": row[22],
                    "factor_governance_exposure": _safe_float(row[23]),
                    "factor_momentum_exposure": _safe_float(row[24]),
                    "factor_low_vol_exposure": _safe_float(row[25]),
                    "factor_sector_tilt_abs": _safe_float(row[26]),
                    "factor_concentration": _safe_float(row[27]),
                    "computed_at": row[28],
                }
            )
        return output

    return _safe_cached(cache_key, CACHE_TTLS["snapshot"], _load)


def get_timeseries_rows() -> list[dict[str, object]]:
    if _data_mode() == "fixture":
        output: list[dict[str, object]] = []
        for model_code in MODEL_ORDER:
            output.extend(_fixture_series_map()[model_code])
        return output

    cache_key = _cache_key("timeseries", "full_history")
    model_code_sql = ", ".join(f"'{code}'" for code in MODEL_ORDER)

    def _load() -> list[dict[str, object]]:
        sql = (
            "SELECT model_code, trade_date, level_tr, vol_20d, vol_60d, drawdown_to_date, ret_ytd "
            "FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY "
            f"WHERE model_code IN ({model_code_sql}) "
            f"ORDER BY trade_date, {_order_case_sql('model_code')}"
        )
        rows = _safe_execute_rows(sql)
        return [
            {
                "model_code": row[0],
                "trade_date": _coerce_date(row[1]),
                "level_tr": _safe_float(row[2]),
                "vol_20d": _safe_float(row[3]),
                "vol_60d": _safe_float(row[4]),
                "drawdown_to_date": _safe_float(row[5]),
                "ret_ytd": _safe_float(row[6]),
            }
            for row in rows
            if _coerce_date(row[1]) is not None and _safe_float(row[2]) is not None
        ]

    return _safe_cached(cache_key, CACHE_TTLS["timeseries"], _load)


def get_position_rows(*, trade_date: dt.date, model_code: str) -> list[dict[str, object]]:
    if _data_mode() == "fixture":
        return _fixture_position_rows().get(model_code, [])[:]

    cache_key = _cache_key("positions", trade_date.isoformat(), model_code)

    def _load() -> list[dict[str, object]]:
        sql = (
            "SELECT model_code, trade_date, ticker, model_name, rebalance_date, company_name, sector, model_weight, "
            "       benchmark_weight, active_weight, price_quality, ret_1d, contrib_1d, contrib_5d, contrib_20d, "
            "       contrib_mtd, contrib_ytd, governance_score, momentum_20d, low_vol_60d "
            "FROM SC_IDX_PORTFOLIO_POSITION_DAILY "
            "WHERE trade_date = :trade_date AND model_code = :model_code "
            "ORDER BY model_weight DESC, ticker"
        )
        rows = _safe_execute_rows(sql, {"trade_date": trade_date, "model_code": model_code})
        return [
            {
                "model_code": row[0],
                "trade_date": _coerce_date(row[1]),
                "ticker": row[2],
                "model_name": row[3],
                "rebalance_date": _coerce_date(row[4]),
                "company_name": row[5],
                "sector": row[6] or "Unclassified",
                "model_weight": _safe_float(row[7]),
                "benchmark_weight": _safe_float(row[8]),
                "active_weight": _safe_float(row[9]),
                "price_quality": row[10],
                "ret_1d": _safe_float(row[11]),
                "contrib_1d": _safe_float(row[12]),
                "contrib_5d": _safe_float(row[13]),
                "contrib_20d": _safe_float(row[14]),
                "contrib_mtd": _safe_float(row[15]),
                "contrib_ytd": _safe_float(row[16]),
                "governance_score": _safe_float(row[17]),
                "momentum_20d": _safe_float(row[18]),
                "low_vol_60d": _safe_float(row[19]),
            }
            for row in rows
        ]

    return _safe_cached(cache_key, CACHE_TTLS["positions"], _load)


def get_sector_rows(*, trade_date: dt.date, model_code: str) -> list[dict[str, object]]:
    if _data_mode() == "fixture":
        return _fixture_sector_rows().get(model_code, [])[:]

    cache_key = _cache_key("sectors", trade_date.isoformat(), model_code)

    def _load() -> list[dict[str, object]]:
        sql = (
            "SELECT model_code, trade_date, sector, sector_weight, benchmark_sector_weight, active_sector_weight, "
            "       contrib_1d, contrib_5d, contrib_20d, contrib_mtd, contrib_ytd "
            "FROM SC_IDX_PORTFOLIO_SECTOR_DAILY_V "
            "WHERE trade_date = :trade_date AND model_code = :model_code "
            "ORDER BY ABS(active_sector_weight) DESC, sector"
        )
        rows = _safe_execute_rows(sql, {"trade_date": trade_date, "model_code": model_code})
        return [
            {
                "model_code": row[0],
                "trade_date": _coerce_date(row[1]),
                "sector": row[2] or "Unclassified",
                "sector_weight": _safe_float(row[3]),
                "benchmark_sector_weight": _safe_float(row[4]),
                "active_sector_weight": _safe_float(row[5]),
                "contrib_1d": _safe_float(row[6]),
                "contrib_5d": _safe_float(row[7]),
                "contrib_20d": _safe_float(row[8]),
                "contrib_mtd": _safe_float(row[9]),
                "contrib_ytd": _safe_float(row[10]),
            }
            for row in rows
        ]

    return _safe_cached(cache_key, CACHE_TTLS["sectors"], _load)


def get_constraints(model_code: str) -> list[dict[str, object]]:
    if _data_mode() == "fixture":
        return [row for row in _fixture_constraints() if row["model_code"] == model_code]

    cache_key = _cache_key("constraints", model_code)

    def _load() -> list[dict[str, object]]:
        sql = (
            "SELECT model_code, constraint_key, constraint_type, constraint_value "
            "FROM SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS "
            "WHERE model_code = :model_code "
            "ORDER BY constraint_key"
        )
        rows = _safe_execute_rows(sql, {"model_code": model_code})
        return [
            {
                "model_code": row[0],
                "constraint_key": row[1],
                "constraint_type": row[2],
                "constraint_value": row[3],
            }
            for row in rows
        ]

    return _safe_cached(cache_key, CACHE_TTLS["constraints"], _load)


def get_optimizer_summary(trade_date: dt.date) -> dict[str, object]:
    if _data_mode() == "fixture":
        return _fixture_optimizer_summary()

    cache_key = _cache_key("optimizer_summary", trade_date.isoformat())

    def _load() -> dict[str, object]:
        sql = (
            "SELECT COUNT(*), "
            "       SUM(CASE WHEN eligible_flag = 'Y' THEN 1 ELSE 0 END), "
            "       AVG(governance_score), AVG(momentum_20d), AVG(low_vol_60d) "
            "FROM SC_IDX_PORTFOLIO_OPT_INPUTS "
            "WHERE trade_date = :trade_date"
        )
        rows = _safe_execute_rows(sql, {"trade_date": trade_date})
        if not rows:
            return {}
        row = rows[0]
        return {
            "trade_date": trade_date,
            "input_count": int(row[0] or 0),
            "eligible_count": int(row[1] or 0),
            "avg_governance_score": _safe_float(row[2]),
            "avg_momentum_20d": _safe_float(row[3]),
            "avg_low_vol_60d": _safe_float(row[4]),
        }

    return _safe_cached(cache_key, CACHE_TTLS["optimizer_summary"], _load)


def build_timeseries_payload(rows: Iterable[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    output = {model_code: [] for model_code in MODEL_ORDER}
    for row in rows:
        model_code = str(row.get("model_code"))
        trade_date = row.get("trade_date")
        if model_code not in output or not isinstance(trade_date, dt.date):
            continue
        output[model_code].append(
            {
                "date": trade_date.isoformat(),
                "level": row.get("level_tr"),
                "vol20": row.get("vol_20d"),
                "vol60": row.get("vol_60d"),
                "drawdown": row.get("drawdown_to_date"),
                "retYtd": row.get("ret_ytd"),
            }
        )
    return output


def summarize_contribution_windows(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    windows = [
        ("contrib_1d", "1D"),
        ("contrib_5d", "5D"),
        ("contrib_20d", "20D"),
        ("contrib_mtd", "MTD"),
        ("contrib_ytd", "YTD"),
    ]
    items = [row for row in rows if row.get("ticker")]
    output = []
    for key, label in windows:
        available = [row for row in items if row.get(key) is not None]
        if not available:
            output.append({"label": label, "top": None, "bottom": None})
            continue
        top = max(available, key=lambda row: float(row.get(key) or 0.0))
        bottom = min(available, key=lambda row: float(row.get(key) or 0.0))
        output.append({"label": label, "top": top, "bottom": bottom, "metric_key": key})
    return output


def format_constraint(row: dict[str, object]) -> dict[str, str]:
    key = str(row.get("constraint_key") or "")
    label = CONSTRAINT_LABELS.get(key, key.replace("_", " ").title())
    value = str(row.get("constraint_value") or "—")
    if row.get("constraint_type") == "PCT":
        try:
            value = f"{float(value) * 100:.2f}%"
        except (TypeError, ValueError):
            pass
    elif value in {"TRUE", "FALSE"}:
        value = "Yes" if value == "TRUE" else "No"
    return {"label": label, "value": value}
