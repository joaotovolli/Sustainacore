import importlib.util, pathlib

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

# Load the facade over HTTP via requests mock would be overkill;
# just assert the FastAPI app contract if present:
p = pathlib.Path("/opt/sustainacore-ai/app/retrieval/app.py")
if p.exists():
    spec = importlib.util.spec_from_file_location("retrieval_app", str(p))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    client = TestClient(mod.app)

    def test_healthz():
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"ok": True}

    def test_ask_contract():
        r = client.post("/ask", json={"question": "ping", "top_k": 1})
        assert r.status_code == 200
        payload = r.json()
        assert "answer" in payload and isinstance(payload["answer"], str)
        assert payload["answer"].strip() != ""
        assert isinstance(payload.get("contexts"), list)
        assert isinstance(payload.get("sources"), list)
        assert isinstance(payload.get("meta"), dict)

    def test_ask2_contract():
        r = client.get("/ask2", params={"q":"test","k":1})
        j = r.json()
        assert "answer" in j and "sources" in j and "meta" in j
        assert isinstance(j["answer"], str) and j["answer"].strip() != ""
