import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import nosuggest_mw


def test_ask2_post_without_special_headers(monkeypatch, facade_module, facade_client):
    def _stub_route(query, k=None, **_kwargs):
        return {
            "answer": "stubbed",
            "sources": ["Source 1: Stub"],
            "meta": {"routing": "stub", "k": k or 4},
        }

    original_call = nosuggest_mw.NoSuggestMiddleware.__call__

    def _passthrough(self, environ, start_response):
        if environ.get("PATH_INFO") == "/ask2":
            return self.app(environ, start_response)
        return original_call(self, environ, start_response)

    monkeypatch.setattr(nosuggest_mw.NoSuggestMiddleware, "__call__", _passthrough, raising=True)
    monkeypatch.setattr(facade_module, "route_ask2", _stub_route, raising=True)

    response = facade_client.post("/ask2", json={"q": "check headers"})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["answer"] == "stubbed"
    assert isinstance(payload.get("sources"), list)
