import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import app as flask_app


def test_pipeline_first_success(monkeypatch):
    from app.retrieval import adapter
    from app import ask2 as route_handler, ask2_pipeline_first as route_adapter

    called = {"count": 0}

    def fake_run(question, k, client_ip=None):
        called["count"] += 1
        return {
            "answer": "Hello",
            "contexts": [{"id": "x"}],
            "sources": [{"id": "s1"}],
            "meta": {"compose": {"status": "ok"}},
        }

    monkeypatch.setattr(adapter, "run_pipeline", fake_run)
    assert adapter.run_pipeline is fake_run
    assert route_adapter is adapter.ask2_pipeline_first
    monkeypatch.setitem(flask_app.view_functions, "ask2", route_handler)
    client = flask_app.test_client()
    response = client.post("/ask2", json={"q": "Ping", "top_k": 6})
    payload = response.get_json()
    assert response.status_code == 200
    assert called["count"] == 1
    assert payload["answer"] == "Hello"
    assert "Sources:" not in payload["answer"]
    assert isinstance(payload.get("contexts"), list)
    assert payload["contexts"], "pipeline response should include contexts"
    assert payload.get("meta", {}).get("routing") == "gemini_first"


def test_pipeline_first_fallback(monkeypatch):
    from app.retrieval import adapter

    def boom(*args, **kwargs):
        raise RuntimeError("no cli")

    monkeypatch.setattr(adapter, "run_pipeline", boom)
    client = flask_app.test_client()
    response = client.post("/ask2", json={"q": "Ping", "top_k": 6})
    payload = response.get_json()
    assert response.status_code == 200
    assert isinstance(payload.get("answer", ""), str)
    for key in ("answer", "sources", "contexts", "meta"):
        assert key in payload
    assert isinstance(payload.get("contexts"), list)
    assert isinstance(payload.get("sources"), list)



def test_pipeline_fs_fallback_populates_contexts(monkeypatch):
    from app.retrieval import adapter, fs_retriever

    def fake_pipeline(question, k, client_ip=None):
        return {
            "answer": "Fallback answer",
            "sources": [],
            "contexts": [],
            "meta": {},
        }

    monkeypatch.setattr(adapter, "run_pipeline", fake_pipeline, raising=False)
    monkeypatch.setattr(fs_retriever, "search", lambda question, top_k=6: [{"id": "fs-1"}])

    shaped, status = adapter.ask2_pipeline_first("Ping", 3, client_ip="1.2.3.4")
    assert status == 200
    assert shaped["contexts"] == [{"id": "fs-1"}]
    assert shaped["meta"]["routing"] == "gemini_first"
