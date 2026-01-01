import datetime as dt

from tools.gemini_jobs_worker.quota_guard import QuotaGuard


def test_quota_guard_persists_state(tmp_path):
    state_path = tmp_path / "quota_state.json"
    guard = QuotaGuard(state_path=str(state_path))
    ts = dt.datetime(2025, 1, 1, 12, 0, 0)
    guard.record_call(timestamp=ts)

    reloaded = QuotaGuard(state_path=str(state_path))
    reloaded.load()
    assert reloaded.state.minute_calls
    assert reloaded.state.daily_counts.get("2025-01-01") == 1
