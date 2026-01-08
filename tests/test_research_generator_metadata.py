import json

from tools.research_generator.run_generator import _build_details
from tools.research_generator import run_generator


def test_generation_meta_in_details():
    bundle = {"report_type": "REBALANCE", "window": {"start": "2025-01-01", "end": "2025-01-02"}}
    draft = {"table_caption": "table", "chart_caption": "chart", "template_mode": True}
    meta = {
        "duration_seconds": 120,
        "profile_selected": "MEDIUM",
        "usage_before": {"available": False, "reason": "usage_unavailable"},
        "usage_after": {"available": False, "reason": "usage_unavailable"},
        "usage_delta": {"available": False, "reason": "usage_unavailable"},
        "stop_reason": "usage_unavailable",
    }
    details = _build_details(bundle, draft, "chart.png", generation_meta=meta)
    payload = json.loads(details)
    assert "generation_meta" in payload
    assert payload["generation_meta"]["profile_selected"] == "MEDIUM"


def test_skip_when_weekly_ge_95_creates_alert_no_approval(monkeypatch):
    calls = {"alert": 0, "approval": 0}

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(run_generator, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(run_generator, "current_schema", lambda conn: "WKSP")
    monkeypatch.setattr(run_generator, "ensure_proc_reports", lambda conn: None)
    monkeypatch.setattr(run_generator, "count_pending_approvals", lambda conn: 0)
    monkeypatch.setattr(run_generator, "_determine_trigger", lambda now, force: ("REBALANCE", None, {}))
    monkeypatch.setattr(run_generator, "_build_bundle", lambda report_type, label, company_ticker=None: ({"report_type": "REBALANCE", "window": {}}, None))
    monkeypatch.setattr(run_generator, "insert_alert", lambda *args, **kwargs: calls.__setitem__("alert", calls["alert"] + 1))
    monkeypatch.setattr(run_generator, "_create_approval", lambda *args, **kwargs: calls.__setitem__("approval", calls["approval"] + 1))
    monkeypatch.setattr(run_generator, "get_usage_snapshot", lambda: {"available": True, "weekly": {"used_pct": 96}, "five_hour": {"used_pct": 0}})

    result = run_generator.run_once(force=None, dry_run=False)
    assert result == 0
    assert calls["alert"] == 1
    assert calls["approval"] == 0
