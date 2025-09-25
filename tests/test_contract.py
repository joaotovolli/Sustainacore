from fastapi.testclient import TestClient
import importlib


def test_ask2_contract_nonempty():
    mod = importlib.import_module("app.retrieval.app")
    client = TestClient(mod.app)
    r = client.get("/ask2", params={"q": "hello", "k": 2})
    assert r.status_code == 200
    j = r.json()
    assert set(["answer", "sources", "meta"]).issubset(j.keys())
    assert isinstance(j["answer"], str) and j["answer"].strip() != ""
    assert isinstance(j["sources"], list)
    assert isinstance(j["meta"], dict)


def test_healthz():
    mod = importlib.import_module("app.retrieval.app")
    client = TestClient(mod.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"
