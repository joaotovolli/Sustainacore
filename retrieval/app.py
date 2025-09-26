import os
import time
from flask import Flask, request, jsonify
import requests

from embedder_settings import get_embed_settings
from embedding_client import embed_text
from retrieval.config import INSUFFICIENT_CONTEXT_MESSAGE, RETRIEVAL_TOP_K, SIMILARITY_FLOOR
from retrieval.scope import compute_similarity, dedupe_contexts, infer_scope, extract_entities
from db_helper import top_k_by_vector, get_connection

app = Flask(__name__)

_OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434")
_EMBED_SETTINGS = get_embed_settings()


@app.get("/healthz")
def healthz():
    status = {"ok": True, "deps": {}}
    try:
        resp = requests.get(f"{_OLLAMA_URL}/api/tags", timeout=5)
        status["deps"]["ollama"] = {"ok": resp.ok}
        if not resp.ok:
            status["ok"] = False
    except Exception as exc:
        status["deps"]["ollama"] = {"ok": False, "err": str(exc)}
        status["ok"] = False

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1 from dual")
                cur.fetchone()
        status["deps"]["oracle"] = {"ok": True}
    except Exception as exc:
        status["deps"]["oracle"] = {"ok": False, "err": str(exc)}
        status["ok"] = False

    return jsonify(status), (200 if status["ok"] else 503)


@app.get("/ask2")
def ask2():
    started = time.time()
    q = request.args.get("q", "").strip()
    try:
        k = int(request.args.get("k") or RETRIEVAL_TOP_K)
    except ValueError:
        k = RETRIEVAL_TOP_K
    if not q:
        return jsonify({"answer": "", "sources": [], "error": "empty_query"}), 400

    explicit_filters = {
        key: request.args.get(key, "")
        for key in ("docset", "namespace", "ticker", "company")
        if request.args.get(key)
    }
    scope = infer_scope(q, explicit_filters=explicit_filters, entities=extract_entities(q))
    filters = scope.applied_filters or {}

    try:
        embedding = embed_text(q, settings=_EMBED_SETTINGS)
        rows = top_k_by_vector(embedding, k=k, filters=filters)
    except Exception as exc:
        return jsonify({"answer": "", "sources": [], "error": "retrieval_failed", "detail": str(exc)}), 500

    rows = dedupe_contexts(rows)
    sources = []
    top_score = None
    for row in rows:
        score = compute_similarity(row.get("dist"))
        if top_score is None and score is not None:
            top_score = score
        sources.append(
            {
                "id": f"{row.get('doc_id', '')}-{row.get('chunk_ix', '')}",
                "title": row.get("title") or "",
                "score": score,
                "snippet": (row.get("chunk_text") or "")[:320],
                "url": row.get("source_url"),
                "source_type": row.get("source_type"),
            }
        )

    insufficient = top_score is None or top_score < SIMILARITY_FLOOR
    took = int((time.time() - started) * 1000)
    meta = {
        "took_ms": took,
        "k": k,
        "model_info": {"embed": _EMBED_SETTINGS.model_name},
        "scope": scope.label,
        "filters": {k: list(v) for k, v in (scope.applied_filters or {}).items()},
        "top_score": top_score,
        "insufficient_context": insufficient,
    }
    if insufficient:
        meta["message"] = INSUFFICIENT_CONTEXT_MESSAGE

    return jsonify({"answer": "", "sources": sources, "meta": meta})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080)
