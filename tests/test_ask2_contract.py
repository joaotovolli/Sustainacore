from importlib import util
from pathlib import Path

from fastapi.testclient import TestClient

_module_path = Path(__file__).resolve().parents[1] / "app" / "retrieval" / "app.py"
_spec = util.spec_from_file_location("retrieval_app", _module_path)
assert _spec and _spec.loader
_module = util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
app = _module.app


client = TestClient(app)


def test_ask2_contract_keys():
    response = client.get("/ask2", params={"q": "contract check"})
    assert response.status_code == 200
    payload = response.json()
    for key in ("answer", "sources", "meta"):
        assert key in payload


def test_ask2_non_empty_answer():
    response = client.get("/ask2", params={"q": "test"})
    payload = response.json()
    assert len(payload["answer"]) > 0
    assert payload["answer"].strip()
