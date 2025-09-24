import importlib
import importlib.machinery
import importlib.util
import sys
import types
from pathlib import Path

from flask import Flask

FULL_MODE_META_KEY = "_full_mode_payload"


def _load_facade_module():
    # Ensure a lightweight ``app`` module exists before the facade is imported.
    sys.modules.pop("app", None)
    sys.modules.pop("wsgi_ask_facade_test", None)
    stub_app = types.ModuleType("app")
    stub_app.app = Flask("stub-app")
    pkg_path = Path(__file__).resolve().parents[1] / "app"
    stub_app.__path__ = [str(pkg_path)]  # type: ignore[attr-defined]
    spec = importlib.machinery.ModuleSpec("app", loader=None, is_package=True)
    spec.submodule_search_locations = stub_app.__path__
    stub_app.__spec__ = spec  # type: ignore[attr-defined]
    sys.modules["app"] = stub_app

    module_path = Path(__file__).resolve().parents[1] / ".ask_facade" / "wsgi_ask_facade.py"
    module_spec = importlib.util.spec_from_file_location("wsgi_ask_facade_test", module_path)
    assert module_spec and module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def test_facade_default_mode_contract(monkeypatch):
    facade = _load_facade_module()

    def _stub_route(query, k):
        assert k == 3
        return {"answer": "ok", "sources": ["Source 1"], "meta": {}}

    monkeypatch.setattr(facade, "route_ask2", _stub_route)

    assert facade.base_app is not None
    client = facade.base_app.test_client()
    resp = client.get("/ask2", query_string={"q": "hello", "k": 3})
    assert resp.status_code == 200
    payload = resp.get_json()
    assert set(payload.keys()) == {"answer", "sources", "meta"}
    assert payload["meta"]["k"] == 3


def test_facade_full_mode_includes_extra_fields(monkeypatch):
    facade = _load_facade_module()

    def _stub_route(query, k):
        return {
            "answer": "hello",
            "sources": ["Source 1"],
            "meta": {FULL_MODE_META_KEY: {"type": "greeting", "suggestions": ["Option"]}},
        }

    monkeypatch.setattr(facade, "route_ask2", _stub_route)

    assert facade.base_app is not None
    client = facade.base_app.test_client()
    resp = client.get("/ask2", query_string={"q": "hello", "mode": "full"})
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["answer"] == "hello"
    assert payload["meta"]["k"] == 4  # default value
    assert payload["type"] == "greeting"
    assert payload["suggestions"] == ["Option"]
