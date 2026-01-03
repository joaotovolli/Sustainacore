"""Quota guard for Gemini CLI usage."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import config

LOGGER = logging.getLogger("research_generator.quota")


@dataclass
class QuotaState:
    minute_calls: List[float] = field(default_factory=list)
    daily_counts: Dict[str, int] = field(default_factory=dict)


class QuotaGuard:
    def __init__(self, state_path: Optional[str] = None) -> None:
        self.state_path = state_path or config.QUOTA_STATE_PATH
        self.state = QuotaState()
        self._loaded = False

    def _now(self) -> dt.datetime:
        return dt.datetime.utcnow()

    def _prune_minute_calls(self) -> None:
        now_ts = self._now().timestamp()
        cutoff = now_ts - 60.0
        self.state.minute_calls = [ts for ts in self.state.minute_calls if ts >= cutoff]

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self.load()
        self._loaded = True

    def load(self) -> None:
        path = self._resolve_state_path()
        if not os.path.isfile(path):
            self.state = QuotaState()
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.warning("quota_state_load_failed: %s", exc)
            self.state = QuotaState()
            return
        minute_calls = payload.get("minute_calls", [])
        daily_counts = payload.get("daily_counts", {})
        self.state = QuotaState(
            minute_calls=[float(ts) for ts in minute_calls if isinstance(ts, (int, float))],
            daily_counts={str(k): int(v) for k, v in daily_counts.items() if isinstance(v, (int, float, str))},
        )

    def persist(self) -> None:
        path = self._resolve_state_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        payload = {
            "minute_calls": self.state.minute_calls,
            "daily_counts": self.state.daily_counts,
        }
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, path)

    def _resolve_state_path(self) -> str:
        preferred_dir = os.path.dirname(self.state_path)
        try:
            os.makedirs(preferred_dir, exist_ok=True)
            test_path = os.path.join(preferred_dir, ".write_check")
            with open(test_path, "w", encoding="utf-8") as handle:
                handle.write("ok")
            os.remove(test_path)
            return self.state_path
        except (OSError, PermissionError) as exc:
            LOGGER.warning("quota_state_fallback path=%s error=%s", self.state_path, exc)
            fallback_dir = os.path.dirname(config.QUOTA_FALLBACK_PATH)
            os.makedirs(fallback_dir, exist_ok=True)
            return config.QUOTA_FALLBACK_PATH

    def before_call(self) -> None:
        self._ensure_loaded()
        self._prune_minute_calls()
        now = self._now()
        today = now.strftime("%Y-%m-%d")
        daily_count = int(self.state.daily_counts.get(today, 0))

        if daily_count >= config.MAX_CALLS_PER_DAY:
            next_day = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_seconds = max(0, int((next_day - now).total_seconds()) + config.DAILY_BUFFER_SECONDS)
            LOGGER.warning("quota_daily_limit hit count=%s sleep_s=%s", daily_count, sleep_seconds)
            time.sleep(sleep_seconds)
            return

        if len(self.state.minute_calls) >= config.MAX_CALLS_PER_MINUTE:
            LOGGER.warning(
                "quota_minute_limit hit calls=%s sleep_s=%s",
                len(self.state.minute_calls),
                config.SLEEP_ON_MINUTE_LIMIT,
            )
            time.sleep(config.SLEEP_ON_MINUTE_LIMIT)

    def record_call(self, timestamp: Optional[dt.datetime] = None) -> None:
        self._ensure_loaded()
        now = timestamp or self._now()
        now_ts = now.timestamp()
        self.state.minute_calls.append(now_ts)
        self._prune_minute_calls()
        today = now.strftime("%Y-%m-%d")
        self.state.daily_counts[today] = int(self.state.daily_counts.get(today, 0)) + 1
        self.persist()

    def current_counts(self) -> Dict[str, int]:
        self._ensure_loaded()
        self._prune_minute_calls()
        today = self._now().strftime("%Y-%m-%d")
        return {
            "minute": len(self.state.minute_calls),
            "daily": int(self.state.daily_counts.get(today, 0)),
        }

    def near_limit(self) -> bool:
        counts = self.current_counts()
        return counts["minute"] >= (config.MAX_CALLS_PER_MINUTE - 2) or counts["daily"] >= (
            config.MAX_CALLS_PER_DAY - 5
        )
