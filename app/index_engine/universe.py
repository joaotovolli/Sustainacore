"""Helpers for TECH100 impacted-universe selection."""
from __future__ import annotations

from typing import Iterable

from index_engine.db import normalize_ticker


def select_top_weighted_tickers(rows: Iterable[dict], *, limit: int = 25) -> list[str]:
    """Select top tickers with PORT_WEIGHT > 0 sorted by weight then rank_index."""
    filtered = []
    for row in rows:
        ticker = row.get("ticker")
        weight = row.get("port_weight")
        rank_index = row.get("rank_index")
        if ticker is None or weight is None:
            continue
        try:
            weight_value = float(weight)
        except (TypeError, ValueError):
            continue
        if weight_value <= 0:
            continue
        filtered.append(
            (
                weight_value,
                rank_index if rank_index is not None else 0,
                normalize_ticker(str(ticker)),
            )
        )

    filtered.sort(key=lambda item: (-item[0], item[1], item[2]))
    return [ticker for _, _, ticker in filtered[:limit] if ticker]


__all__ = ["select_top_weighted_tickers"]
