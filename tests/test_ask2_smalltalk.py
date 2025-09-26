import pytest


SMALLTALK_CASES = ["hi", "hello", "help", "thanks", "goodbye"]


@pytest.mark.parametrize("phrase", SMALLTALK_CASES)
def test_smalltalk_short_circuit(monkeypatch, facade_module, facade_client, phrase):
    def _fail_route(*args, **kwargs):
        raise AssertionError("route_ask2 should not run for small-talk inputs")

    monkeypatch.setattr(facade_module, "route_ask2", _fail_route, raising=True)

    response = facade_client.post("/ask2", json={"q": phrase, "top_k": 8})
    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload.get("answer")
    assert payload.get("contexts") in (None, [], {})
    assert response.headers.get("X-Smalltalk-Handled") == "/ask2"
