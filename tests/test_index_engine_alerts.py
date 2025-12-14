import string

import app.index_engine.alerts as alerts
from app.index_engine.run_report import format_run_report
from tools.index_engine import run_daily


def test_send_email_no_env(monkeypatch):
    called = {"count": 0}

    class DummySMTP:
        def __init__(self, *args, **kwargs):
            called["count"] += 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def ehlo(self):
            pass

        def starttls(self):
            pass

        def login(self, user, password):
            pass

        def send_message(self, msg):
            pass

    monkeypatch.setattr(alerts.smtplib, "SMTP", DummySMTP)
    # Required envs missing, should no-op
    alerts.send_email("subj", "body")
    assert called["count"] == 0


def test_send_email_success(monkeypatch):
    called = {"count": 0, "subject": None, "body": None}

    class DummySMTP:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def ehlo(self):
            pass

        def starttls(self):
            pass

        def login(self, user, password):
            pass

        def send_message(self, msg):
            called["count"] += 1
            called["subject"] = msg["Subject"]
            called["body"] = msg.get_content()

    monkeypatch.setattr(alerts.smtplib, "SMTP", DummySMTP)
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user@test")
    monkeypatch.setenv("SMTP_PASS", "secret")
    monkeypatch.setenv("MAIL_FROM", "from@test")
    monkeypatch.setenv("MAIL_TO", "to@test")

    alerts.send_email("hello", "world")
    assert called["count"] == 1
    assert called["subject"] == "hello"
    assert "world" in called["body"]


def test_format_run_report_truncates_error():
    long_error = "X" * 900
    summary = {
        "status": "ERROR",
        "end_date": "2025-12-10",
        "provider": "TWELVEDATA",
        "max_provider_calls": 10,
        "provider_calls_used": 1,
        "raw_upserts": 2,
        "canon_upserts": 2,
        "raw_ok": 1,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": "2025-12-09",
        "oracle_user": "WKSP",
        "error_msg": long_error,
    }
    report = format_run_report("run123", summary, "tail log here")
    assert "run123" in report
    assert "ERROR" in report
    assert len(report) < len(long_error) + 200  # truncated
    assert "tail log here" in report


def test_maybe_send_alert_controls_budget_stop(monkeypatch):
    calls = {"subjects": []}

    def fake_send_email(subj, body):
        calls["subjects"].append(subj)

    monkeypatch.setattr(run_daily, "send_email", fake_send_email)
    monkeypatch.setattr(run_daily, "_safe_journal_tail", lambda: "log")

    summary = {"status": "ERROR"}
    run_daily._maybe_send_alert("ERROR", summary, "rid1", False)
    assert any("ERROR" in s for s in calls["subjects"])

    calls["subjects"].clear()
    run_daily._maybe_send_alert("DAILY_BUDGET_STOP", summary, "rid2", False)
    assert calls["subjects"] == []

    run_daily._maybe_send_alert("DAILY_BUDGET_STOP", summary, "rid3", True)
    assert any("DAILY_BUDGET_STOP" in s for s in calls["subjects"])
