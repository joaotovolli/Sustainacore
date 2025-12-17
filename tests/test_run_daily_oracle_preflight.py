import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from tools.index_engine import run_daily


def test_run_daily_oracle_preflight_failure_exits_2_and_alerts(monkeypatch):
    calls = {"start": 0, "finish": [], "email": 0}

    def fail_preflight():
        raise RuntimeError("ORA-28759: failure to open file")

    monkeypatch.setattr(run_daily, "probe_oracle_user", fail_preflight)
    monkeypatch.setattr(run_daily, "collect_wallet_diagnostics", lambda: {"tns_admin": "X"})
    monkeypatch.setattr(run_daily, "format_wallet_diagnostics", lambda diag: "wallet_diag")
    monkeypatch.setattr(run_daily, "_safe_journal_tail", lambda: None)

    monkeypatch.setattr(run_daily, "start_run", lambda *args, **kwargs: calls.__setitem__("start", calls["start"] + 1))

    def finish(run_id, **kwargs):
        calls["finish"].append(kwargs)

    monkeypatch.setattr(run_daily, "finish_run", finish)
    monkeypatch.setattr(run_daily, "send_email", lambda *args, **kwargs: calls.__setitem__("email", calls["email"] + 1))

    rc = run_daily.main()

    assert rc == 2
    assert calls["start"] == 1
    assert calls["email"] == 1
    assert calls["finish"], "finish_run should be attempted"
    assert calls["finish"][0]["status"] == "ERROR"
    assert calls["finish"][0]["error"] == "oracle_preflight_failed"
