from __future__ import annotations

import pytest

from app.retrieval import service


def test_service_bypasses_smalltalk_without_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure the bypass works even when gemini is disabled.
    monkeypatch.setattr(service.settings, "gemini_first_enabled", False, raising=False)

    def boom(*args, **kwargs):
        raise AssertionError("gateway.classify_intent should not be called for greetings")

    monkeypatch.setattr(service.gateway, "classify_intent", boom, raising=True)

    payload = service.run_pipeline("hello", k=4, client_ip="1.2.3.4")
    assert isinstance(payload, dict)
    assert payload.get("sources") == []
    assert payload.get("contexts") == []
    assert "Try asking" in payload.get("answer", "")


def test_service_bypasses_low_info_without_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service.settings, "gemini_first_enabled", False, raising=False)

    def boom(*args, **kwargs):
        raise AssertionError("gateway should not be called for low-info prompts")

    monkeypatch.setattr(service.gateway, "classify_intent", boom, raising=True)

    payload = service.run_pipeline("asdasdasd", k=4, client_ip="1.2.3.4")
    assert payload.get("sources") == []
    assert payload.get("contexts") == []
    assert "couldn’t find" in payload.get("answer", "").lower() or "couldn't find" in payload.get("answer", "").lower() or "could not" in payload.get("answer", "").lower()
