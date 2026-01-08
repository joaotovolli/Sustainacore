import json

from tools.research_generator import codex_usage


def test_codex_usage_parses_json(monkeypatch):
    payload = {"weekly": {"used_pct": 42, "remaining_pct": 58}, "five_hour": {"used_pct": 10, "remaining_pct": 90}}

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    def fake_run(*args, **kwargs):
        return Result()

    monkeypatch.setattr(codex_usage.subprocess, "run", fake_run)
    snapshot = codex_usage.get_usage_snapshot()
    assert snapshot["available"] is True
    assert snapshot["weekly"]["used_pct"] == 42
    assert snapshot["five_hour"]["used_pct"] == 10


def test_codex_usage_parses_text(monkeypatch):
    def fake_run(cmd):
        if "--json" in cmd:
            return None, "no_json"
        return "Weekly usage: 55%\n5h usage: 12%", None

    monkeypatch.setattr(codex_usage, "_run_usage_command", fake_run)
    snapshot = codex_usage.get_usage_snapshot()
    assert snapshot["available"] is True
    assert snapshot["weekly"]["used_pct"] == 55.0
    assert snapshot["five_hour"]["used_pct"] == 12.0


def test_usage_delta_when_unavailable():
    before = {"available": False, "reason": "usage_unavailable"}
    after = {"available": False, "reason": "usage_unavailable"}
    delta = codex_usage.compute_usage_delta(before, after)
    assert delta["available"] is False
