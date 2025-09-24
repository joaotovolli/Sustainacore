"""Observability helpers for the Gemini-first orchestration."""

from __future__ import annotations

import datetime as _dt
import json
import logging
import threading
from collections import Counter
from typing import Any, Dict


LOGGER = logging.getLogger("observability")


class Observability:
    """Collects per-request telemetry and emits a daily snapshot."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._day = _dt.date.today()
        self._misses: Counter[str] = Counter()
        self._usage: Counter[str] = Counter()

    def _emit_daily(self) -> None:
        top_misses = self._misses.most_common(10)
        payload = {
            "event": "daily_report",
            "day": self._day.isoformat(),
            "top_10_misses": top_misses,
            "usage_snapshot": dict(self._usage),
        }
        LOGGER.info(json.dumps(payload, ensure_ascii=False))

    def _rollover_if_needed(self) -> None:
        today = _dt.date.today()
        if today != self._day:
            self._emit_daily()
            self._day = today
            self._misses.clear()
            self._usage.clear()

    def record(self, record: Dict[str, Any]) -> None:
        payload = dict(record)
        with self._lock:
            self._rollover_if_needed()
            question = payload.get("question") or ""
            if payload.get("final_sources_count", 0) == 0 and question:
                key = str(question)[:160]
                self._misses[key] += 1
            intent = payload.get("intent") or "INFO_REQUEST"
            self._usage[f"intent:{intent}"] += 1
            if payload.get("intent") == "INFO_REQUEST":
                self._usage["oracle_calls"] += 1
            if payload.get("gemini"):
                self._usage[f"gemini:{payload['gemini']}"] += 1
            LOGGER.info(json.dumps(payload, ensure_ascii=False))


observer = Observability()

