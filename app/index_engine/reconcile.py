"""Price reconciliation logic for SC_IDX canonical prices."""
from __future__ import annotations

from typing import Iterable

PREFERRED_PROVIDER = "MARKET_DATA"
SECONDARY_PROVIDER = "ALPHAVANTAGE"


def _median(values: Iterable[float]) -> float:
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 0:
        raise ValueError("Cannot compute median of empty list")
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _max_pairwise_percent_difference(values: list[float]) -> float:
    max_diff = 0.0
    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            a, b = values[i], values[j]
            if a == 0 and b == 0:
                diff = 0.0
            else:
                avg = (a + b) / 2.0
                if avg == 0:
                    diff = float("inf")
                else:
                    diff = abs(a - b) / avg * 100.0
            max_diff = max(max_diff, diff)
    return max_diff


def reconcile_canonical(
    provider_adj_closes: dict[str, float],
    provider_closes: dict[str, float] | None = None,
    divergence_threshold_pct: float = 0.50,
) -> dict:
    """
    Reconcile multiple provider prices into a canonical adjusted close.

    Returns:
      {
        "canon_adj_close": float|None,
        "canon_close": float|None,
        "chosen_provider": "MEDIAN"|<preferred>|<secondary>|<provider>,
        "providers_ok": int,
        "divergence_pct": float|None,
        "quality": "HIGH"|"LOW"|"CONFLICT"
      }
    Rules:
      - Use only non-null adjusted closes.
      - If providers_ok >= 2:
          divergence = max_pairwise_percent_difference(adjs)
          if divergence <= threshold: canon = median (2 vals => average), quality=HIGH, chosen=MEDIAN
          else: choose preferred if present else secondary else first provider; quality=CONFLICT
      - If providers_ok == 1: choose that provider, quality=LOW
      - If providers_ok == 0: all None + quality=LOW
    """

    adjs = {k: v for k, v in provider_adj_closes.items() if v is not None}
    providers_ok = len(adjs)

    result = {
        "canon_adj_close": None,
        "canon_close": None,
        "chosen_provider": None,
        "providers_ok": providers_ok,
        "divergence_pct": None,
        "quality": "LOW",
    }

    if providers_ok == 0:
        return result

    if providers_ok >= 2:
        adj_values = list(adjs.values())
        divergence = _max_pairwise_percent_difference(adj_values)
        result["divergence_pct"] = divergence

        if divergence <= divergence_threshold_pct:
            result["canon_adj_close"] = _median(adj_values)
            result["chosen_provider"] = "MEDIAN"
            result["quality"] = "HIGH"
            if provider_closes:
                closes = [
                    provider_closes[p]
                    for p in adjs
                    if provider_closes.get(p) is not None
                ]
                if closes:
                    result["canon_close"] = _median(closes)
        else:
            if PREFERRED_PROVIDER in adjs:
                chosen = PREFERRED_PROVIDER
            elif SECONDARY_PROVIDER in adjs:
                chosen = SECONDARY_PROVIDER
            else:
                chosen = next(iter(adjs.keys()))
            result["chosen_provider"] = chosen
            result["canon_adj_close"] = adjs[chosen]
            result["canon_close"] = provider_closes.get(chosen) if provider_closes else None
            result["quality"] = "CONFLICT"
        return result

    # Exactly one provider available
    chosen = next(iter(adjs.keys()))
    result["chosen_provider"] = chosen
    result["canon_adj_close"] = adjs[chosen]
    result["canon_close"] = provider_closes.get(chosen) if provider_closes else None
    return result
