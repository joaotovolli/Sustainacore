import datetime as dt
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import tools.index_engine.pipeline_state as pipeline_state


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, binds=None):
        return None

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self._rows)


def test_compact_details_for_oracle_shrinks_large_context():
    trading_days = [f"2026-01-{day:02d}" for day in range(1, 32)] * 12
    details = json.dumps(
        {
            "stage": "determine_target_dates",
            "status": "OK",
            "detail": "candidate_end=2026-03-27",
            "detail_json": {
                "context": {
                    "calendar_max_date": "2026-03-27",
                    "candidate_end_date": "2026-03-27",
                    "trading_days": trading_days,
                },
                "counts": {"candidate_end_date": "2026-03-27"},
                "warnings": [],
            },
        }
    )

    compacted = pipeline_state._compact_details_for_oracle(details)

    assert compacted is not None
    assert len(compacted) <= pipeline_state.ORACLE_DETAILS_MAX_CHARS
    assert json.loads(compacted)["stage"] == "determine_target_dates"


def test_local_state_resets_when_run_id_changes_same_day(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline_state, "_ensure_state_table", lambda: None)
    monkeypatch.setattr(pipeline_state, "_utc_today", lambda: dt.date(2026, 3, 28))

    store = pipeline_state.PipelineStateStore(state_path=tmp_path / "pipeline_state_latest.json")
    store._oracle_ok = False

    store.record_stage_end("run-one", "determine_target_dates", "OK", details='{"stage":"determine_target_dates"}')
    store.record_stage_end("run-two", "preflight_oracle", "OK", details='{"stage":"preflight_oracle"}')

    payload = json.loads((tmp_path / "pipeline_state_latest.json").read_text(encoding="utf-8"))

    assert payload["run_id"] == "run-two"
    assert sorted(payload["stages"].keys()) == ["preflight_oracle"]


def test_fetch_stage_statuses_prefers_local_details_when_oracle_present(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline_state, "_ensure_state_table", lambda: None)
    monkeypatch.setattr(pipeline_state, "_utc_today", lambda: dt.date(2026, 3, 28))

    state_path = tmp_path / "pipeline_state_latest.json"
    state_path.write_text(
        json.dumps(
            {
                "pipeline_name": "sc_idx_pipeline",
                "run_date": "2026-03-28",
                "run_id": "run-local",
                "stages": {
                    "determine_target_dates": {
                        "status": "OK",
                        "started_at": "2026-03-28T16:44:08+00:00",
                        "ended_at": "2026-03-28T16:44:09+00:00",
                        "details": '{"detail":"local"}',
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    oracle_rows = [
        (
            "determine_target_dates",
            "OK",
            dt.datetime(2026, 3, 28, 16, 44, 8, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 3, 28, 16, 44, 9, tzinfo=dt.timezone.utc),
            '{"detail":"oracle"}',
        )
    ]
    monkeypatch.setattr(pipeline_state, "get_connection", lambda: _FakeConnection(oracle_rows))

    store = pipeline_state.PipelineStateStore(state_path=state_path)
    store._oracle_ok = True

    records = store.fetch_stage_statuses("run-local")

    assert records["determine_target_dates"].details == '{"detail":"local"}'
