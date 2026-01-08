import datetime as dt

import tools.index_engine.run_pipeline as pipeline


class _FakeRecord:
    def __init__(self, status="OK"):
        now = dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc)
        self.status = status
        self.started_at = now
        self.ended_at = now
        self.details = None


class _FakeStore:
    def __init__(self):
        self.records = {}

    def record_stage_start(self, run_id, stage_name, details=None):
        self.records[stage_name] = _FakeRecord("STARTED")

    def record_stage_end(self, run_id, stage_name, status, details=None):
        self.records[stage_name] = _FakeRecord(status)


def test_compute_impute_window_prefers_latest_of_impute_and_lookback():
    max_level = dt.date(2026, 1, 7)
    max_impute = dt.date(2026, 1, 1)
    window = pipeline._compute_impute_window(
        max_level_date=max_level,
        max_impute_date=max_impute,
        lookback_days=30,
    )
    assert window is not None
    start, end = window
    assert end == max_level
    assert start == max_impute + dt.timedelta(days=1)


def test_run_stage_with_retries_on_oracle_error(monkeypatch):
    store = _FakeStore()
    calls = {"count": 0}
    sleeps = []

    def _stage(args):
        calls["count"] += 1
        if calls["count"] < 3:
            raise RuntimeError("ORA-12545: Connect failed")
        return 0

    monkeypatch.setattr(pipeline.time, "sleep", lambda value: sleeps.append(value))

    result = pipeline._run_stage_with_retries(
        name="oracle_stage",
        func=_stage,
        args=[],
        state_store=store,
        run_id="run-1",
        max_attempts=3,
        backoff_base_sec=1,
    )

    assert result.status == "OK"
    assert calls["count"] == 3
    assert sleeps == [1, 2]
