import importlib.util
import io
import json
from pathlib import Path

_module_path = Path(__file__).resolve().parents[1] / "ask2_llm_orchestrator.py"
_spec = importlib.util.spec_from_file_location("ask2_llm_orchestrator", _module_path)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
Ask2LLMOrchestratorMiddleware = _module.Ask2LLMOrchestratorMiddleware


def _environ(payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    return {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": "/ask2",
        "wsgi.input": io.BytesIO(body),
        "CONTENT_LENGTH": str(len(body)),
    }


def test_middleware_passthrough_for_gemini_first():
    response_payload = {
        "answer": "Hello there",
        "sources": ["Tech100 overview"],
        "meta": {"gemini_first": True, "foo": "bar"},
    }

    def fake_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps(response_payload).encode("utf-8")]

    middleware = Ask2LLMOrchestratorMiddleware(fake_app)

    status_headers = {}

    def start_response(status, headers, exc_info=None):
        status_headers["status"] = status
        status_headers["headers"] = headers

    body = b"".join(middleware(_environ({"question": "Explain tech100"}), start_response))
    shaped = json.loads(body)

    assert shaped == response_payload
    assert status_headers["status"] == "200 OK"


def test_middleware_propagates_non_200():
    body_bytes = json.dumps({"answer": "", "sources": []}).encode("utf-8")

    def rate_limited_app(environ, start_response):
        start_response("429 Too Many Requests", [("Content-Type", "application/json")])
        return [body_bytes]

    middleware = Ask2LLMOrchestratorMiddleware(rate_limited_app)
    status_headers = {}

    def start_response(status, headers, exc_info=None):
        status_headers["status"] = status
        status_headers["headers"] = headers

    body = b"".join(middleware(_environ({"question": "Explain tech100"}), start_response))

    assert status_headers["status"].startswith("429")
    assert body == body_bytes
