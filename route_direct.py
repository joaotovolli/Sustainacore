from flask import request, jsonify
from db_helper import top_k_by_vector
from embedder_settings import get_embed_settings
from embedding_client import embed_text
from retrieval.config import INSUFFICIENT_CONTEXT_MESSAGE, RETRIEVAL_TOP_K, SIMILARITY_FLOOR
from retrieval.scope import compute_similarity, dedupe_contexts, infer_scope, extract_entities
from traceback import format_exc

_EMBED_SETTINGS = get_embed_settings()


def register(app):
    @app.get("/ask2_direct")
    def ask2_direct():
        q = request.args.get("q", "").strip()
        try:
            k = int(request.args.get("k") or RETRIEVAL_TOP_K)
        except ValueError:
            k = RETRIEVAL_TOP_K
        if not q:
            return jsonify({"answer": "", "sources": []})

        explicit_filters = {
            key: request.args.get(key, "")
            for key in ("docset", "namespace", "ticker", "company")
            if request.args.get(key)
        }
        scope = infer_scope(q, explicit_filters=explicit_filters, entities=extract_entities(q))
        filters = scope.applied_filters or {}

        try:
            emb = embed_text(q, settings=_EMBED_SETTINGS)
        except Exception as exc:
            return (
                jsonify(
                    {
                        "answer": "",
                        "sources": [],
                        "error": f"embed_failed: {exc}",
                        "trace": format_exc(),
                    }
                ),
                502,
            )

        try:
            rows = top_k_by_vector(emb, k=k, filters=filters)
        except Exception as exc:
            return (
                jsonify(
                    {
                        "answer": "",
                        "sources": [],
                        "error": f"db_failed: {exc}",
                        "trace": format_exc(),
                    }
                ),
                500,
            )

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
        scope_meta = {
            "label": scope.label,
            "filters": {k: list(v) for k, v in (scope.applied_filters or {}).items()},
            "source_types": list(scope.source_types),
            "company": scope.company,
            "top_score": top_score,
            "insufficient_context": insufficient,
        }
        if insufficient:
            scope_meta["message"] = INSUFFICIENT_CONTEXT_MESSAGE

        return jsonify({"answer": "", "sources": sources, "meta": scope_meta})
