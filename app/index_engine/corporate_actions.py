"""Deterministic corporate-action guards for the TECH100 adjusted-price methodology."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Mapping

ADJUSTED_PRICE_METHOD = "REFRESH_ADJUSTED_HISTORY"
COMMON_SPLIT_RATIOS = (2.0, 3.0, 4.0, 5.0, 10.0)


@dataclass(frozen=True)
class CorporateActionCandidate:
    ticker: str
    effective_date: dt.date
    action_type: str
    ratio: float
    observed_price_ratio: float
    status: str = "PENDING"


@dataclass(frozen=True)
class ConfirmedCorporateAction:
    ticker: str
    effective_date: dt.date
    action_type: str
    ratio: float
    processing_method: str = ADJUSTED_PRICE_METHOD


def detect_split_candidate(
    *,
    ticker: str,
    effective_date: dt.date,
    previous_price: float,
    current_price: float,
    ratio_tolerance: float = 0.03,
) -> CorporateActionCandidate | None:
    """Return a pending candidate only when a move is near a common split ratio."""
    if previous_price <= 0 or current_price <= 0:
        return None
    observed = current_price / previous_price
    for ratio in COMMON_SPLIT_RATIOS:
        if abs(observed - (1.0 / ratio)) <= ratio_tolerance / ratio:
            return CorporateActionCandidate(
                ticker=ticker,
                effective_date=effective_date,
                action_type="FORWARD_SPLIT",
                ratio=ratio,
                observed_price_ratio=observed,
            )
        if abs(observed - ratio) <= ratio_tolerance * ratio:
            return CorporateActionCandidate(
                ticker=ticker,
                effective_date=effective_date,
                action_type="REVERSE_SPLIT",
                ratio=ratio,
                observed_price_ratio=observed,
            )
    return None


def adjusted_basis_is_consistent(
    *,
    previous_adjusted_price: float,
    current_adjusted_price: float,
    confirmed_action: ConfirmedCorporateAction,
    max_economic_return: float,
) -> bool:
    """Adjusted prices are consistent only when the split itself is economically neutral."""
    if confirmed_action.processing_method != ADJUSTED_PRICE_METHOD:
        return False
    if previous_adjusted_price <= 0 or current_adjusted_price <= 0:
        return False
    economic_return = current_adjusted_price / previous_adjusted_price - 1.0
    return abs(economic_return) <= max_economic_return


def earliest_material_change(
    stored: Mapping[dt.date, float],
    incoming: Mapping[dt.date, float],
    *,
    relative_tolerance: float = 1e-8,
) -> dt.date | None:
    """Return the first date whose adjusted price materially changes."""
    changed: list[dt.date] = []
    for day in sorted(set(stored) & set(incoming)):
        old = float(stored[day])
        new = float(incoming[day])
        scale = max(abs(old), abs(new), 1.0)
        if abs(new - old) > relative_tolerance * scale:
            changed.append(day)
    return changed[0] if changed else None


def should_apply_share_adjustment(action: ConfirmedCorporateAction) -> bool:
    """Adjusted-price methodology never also multiplies synthetic shares."""
    return action.processing_method != ADJUSTED_PRICE_METHOD


__all__ = [
    "ADJUSTED_PRICE_METHOD",
    "CorporateActionCandidate",
    "ConfirmedCorporateAction",
    "adjusted_basis_is_consistent",
    "detect_split_candidate",
    "earliest_material_change",
    "should_apply_share_adjustment",
]
