from typing import Any, Dict, Tuple

try:
    from app.retrieval.service import run_pipeline, _strip_sources_block  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    run_pipeline = None  # type: ignore

    def _strip_sources_block(answer: str) -> str:  # type: ignore
        return answer


def normalize_shape(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize pipeline payloads into the /ask2 contract."""

    data = payload or {}
    answer_text = data.get("answer") or data.get("text") or data.get("summary") or ""
    answer_text = _strip_sources_block(answer_text)
    answer_text = answer_text.strip()
    sources = data.get("sources") or data.get("chunks") or data.get("results") or []
    if not isinstance(sources, list):
        sources = []
    meta = data.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    contexts = data.get("contexts") or []
    if not isinstance(contexts, list):
        contexts = []
    return {"answer": answer_text, "sources": sources, "contexts": contexts, "meta": meta}


def ask2_pipeline_first(question: str, k: int, *, client_ip: str = "unknown") -> Tuple[Dict[str, Any], int]:
    """Attempt Gemini-first routing before falling back to legacy behavior."""

    if callable(run_pipeline):
        try:
            payload = run_pipeline(question, k=k, client_ip=client_ip)  # type: ignore[misc]
        except Exception as exc:  # pragma: no cover - defensive fallback
            shaped = {
                "answer": "",
                "sources": [],
                "contexts": [],
                "meta": {"routing": "gemini_first_fail", "error": str(exc)},
            }
            return shaped, 599

        if isinstance(payload, dict):
            shaped = normalize_shape(payload)
        else:
            shaped = normalize_shape({})
        meta = dict(shaped.get("meta") or {})
        meta["routing"] = "gemini_first"
        shaped["meta"] = meta
        return shaped, 200

    shaped = {
        "answer": "",
        "sources": [],
        "contexts": [],
        "meta": {"routing": "gemini_first_unavailable"},
    }
    return shaped, 598
