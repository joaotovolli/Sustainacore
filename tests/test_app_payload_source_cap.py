from __future__ import annotations

import importlib
import sys

import pytest


def test_build_payload_caps_sources_to_five(monkeypatch: pytest.MonkeyPatch) -> None:
    # app.retrieval.app reads this env var at import time.
    monkeypatch.setenv("AUTH_TOKEN_SIGNING_KEY", "test-signing-key")

    sys.modules.pop("app.retrieval.app", None)
    app_mod = importlib.import_module("app.retrieval.app")

    raw_sources = [{"title": f"Doc {i}", "url": f"https://sustainacore.org/x/{i}"} for i in range(10)]
    payload = app_mod._build_payload(  # type: ignore[attr-defined]
        answer="ok",
        raw_sources=raw_sources,
        raw_contexts=None,
        top_k=10,
        note="ok",
        meta={},
        limit_sources=None,
    )
    assert isinstance(payload, dict)
    assert len(payload.get("sources") or []) == 5
    assert len(payload.get("contexts") or []) == 5

