from typing import Any, Dict

import pytest

from app.retrieval import service


@pytest.fixture(autouse=True)
def _reset_rate_limit():
    service._RATE_BUCKETS.clear()
    yield
    service._RATE_BUCKETS.clear()


def test_run_pipeline_forces_small_talk(monkeypatch):
    monkeypatch.setattr(service.settings, "gemini_first_enabled", True)
    monkeypatch.setattr(service.settings, "show_debug_block", False)
    monkeypatch.setattr(service.settings, "rate_limit_window_seconds", 10)
    monkeypatch.setattr(service.settings, "rate_limit_max_requests", 10)

    def fake_classify(question: str) -> Dict[str, Any]:
        return {"intent": "INFO_REQUEST"}

    compose_called = {}

    def fake_compose_small_talk(question: str) -> str:
        compose_called["question"] = question
        return "Hi there!"

    def fail_plan(*args, **kwargs):  # pragma: no cover - defensive
        raise AssertionError("plan_retrieval should not be called for forced small talk")

    def fail_retrieve(*args, **kwargs):  # pragma: no cover - defensive
        raise AssertionError("retrieve should not be called for forced small talk")

    events = []

    def record_event(event: Dict[str, Any]) -> None:
        events.append(event)

    monkeypatch.setattr(service.gateway, "classify_intent", fake_classify)
    monkeypatch.setattr(service.gateway, "compose_small_talk", fake_compose_small_talk)
    monkeypatch.setattr(service.gateway, "plan_retrieval", fail_plan)
    monkeypatch.setattr(service.retriever, "retrieve", fail_retrieve)
    monkeypatch.setattr(service.observer, "record", record_event)

    result = service.run_pipeline("hello", k=4, client_ip="9.9.9.9")

    assert result["answer"] == "Hi there!"
    assert result["sources"] == []
    assert result["meta"]["intent"] == "SMALL_TALK"
    assert result["meta"]["k"] == 4
    assert compose_called["question"] == "hello"
    assert events and events[0]["intent"] == "SMALL_TALK"
