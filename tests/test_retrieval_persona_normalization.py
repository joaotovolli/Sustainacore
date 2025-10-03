import importlib
import sys
from pathlib import Path
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient


MODULE_NAME = "app.retrieval.app"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_app(monkeypatch: pytest.MonkeyPatch, **env: str) -> Any:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for key in {"PERSONA_V1", "REQUEST_NORMALIZE"} - set(env):
        monkeypatch.delenv(key, raising=False)
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    module = importlib.import_module(MODULE_NAME)
    module = importlib.reload(module)
    monkeypatch.setitem(sys.modules, MODULE_NAME, module)
    return module


def _stub_pipeline(monkeypatch: pytest.MonkeyPatch, module: Any, response: Dict[str, Any]) -> None:
    def _fake_pipeline(question_text: str, *, k: int, client_ip: str) -> Dict[str, Any]:
        payload = dict(response)
        payload.setdefault("meta", {})
        payload.setdefault("contexts", [])
        payload.setdefault("sources", [])
        return payload

    monkeypatch.setattr(module, "run_pipeline", _fake_pipeline)


def test_text_plain_request_returns_200(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_app(monkeypatch, REQUEST_NORMALIZE="1")
    async def _run_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(module.to_thread, "run_sync", _run_sync)
    _stub_pipeline(
        monkeypatch,
        module,
        {
            "answer": "Text body handled.",
            "contexts": [{"title": "Doc", "source_url": "https://example.com/doc"}],
        },
    )

    with TestClient(module.app) as client:
        response = client.post(
            "/ask2",
            data="plain body",
            headers={"Content-Type": "text/plain"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["answer"].startswith("Text body handled.")
    assert isinstance(body["contexts"], list)
    assert isinstance(body["sources"], list)


def test_persona_answer_includes_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_app(monkeypatch, REQUEST_NORMALIZE="1", PERSONA_V1="1")
    async def _run_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(module.to_thread, "run_sync", _run_sync)
    _stub_pipeline(
        monkeypatch,
        module,
        {
            "answer": "Overview sentence. Additional detail.",
            "contexts": [
                {"title": "Doc A", "source_url": "https://example.com/a"},
                {"title": "Doc B", "source_url": "https://example.com/b"},
            ],
        },
    )

    with TestClient(module.app) as client:
        response = client.post(
            "/ask2",
            json={"question": "Tell me something"},
        )

    assert response.status_code == 200
    body = response.json()
    assert "Sources:" in body["answer"]
    assert isinstance(body["sources"], list)
    assert body["sources"]
