import json
import logging
import time

from flask import request, jsonify
from app.http_compat.compat import (
    SC_RAG_FAIL_OPEN,
    SC_RAG_MIN_CONTEXTS,
    SC_RAG_MIN_SCORE,
    normalize_response,
)
from db_helper import top_k_by_vector
from embedder_settings import get_embed_settings
from embedding_client import embed_text
from retrieval.config import INSUFFICIENT_CONTEXT_MESSAGE, RETRIEVAL_TOP_K, SIMILARITY_FLOOR
from retrieval.scope import compute_similarity, dedupe_contexts, infer_scope, extract_entities
from traceback import format_exc

_EMBED_SETTINGS = get_embed_settings()
LOGGER = logging.getLogger("app.ask2_direct")
METRIC_LOGGER = logging.getLogger("app.ask2_direct.metrics")
if not METRIC_LOGGER.handlers:
    _metric_handler = logging.StreamHandler()
    _metric_handler.setLevel(logging.INFO)
    METRIC_LOGGER.addHandler(_metric_handler)
    METRIC_LOGGER.setLevel(logging.INFO)
    METRIC_LOGGER.propagate = False


def register(app):
    def _compat_response(payload, contexts=None, confidence=None, status=200, *, started_at=None):
        compat_payload = normalize_response(
            payload,
            contexts,
            provider='retriever',
            confidence=confidence,
        )
        compat_contexts = compat_payload.get('contexts') if isinstance(compat_payload.get('contexts'), list) else []
        max_score = None
        for entry in compat_contexts:
            if not isinstance(entry, dict):
                continue
            value = entry.get('score')
            if value is None:
                continue
            try:
                score_float = float(value)
            except (TypeError, ValueError):
                continue
            max_score = score_float if max_score is None else max(max_score, score_float)

        latency_ms = None
        if started_at is not None:
            latency_ms = int((time.perf_counter() - started_at) * 1000)

        log_record = {
            'route': '/ask2_direct',
            'provider': compat_payload.get('provider'),
            'ok': compat_payload.get('ok'),
            'answered': compat_payload.get('answered'),
            'n_contexts': len(compat_contexts),
            'max_score': max_score,
            'confidence': compat_payload.get('confidence'),
            'lat_ms': latency_ms,
        }
        try:
            METRIC_LOGGER.info(json.dumps(log_record, separators=(',', ':')))
        except Exception:  # pragma: no cover - defensive logging
            METRIC_LOGGER.info(log_record)

        return jsonify(compat_payload), status

    @app.route("/ask2_direct", methods=["GET", "POST"])
    def ask2_direct():
        started_at = time.perf_counter()
        if request.method == "POST":
            data = request.get_json(silent=True) or {}
            q_raw = data.get("q") or data.get("question") or data.get("text") or ""
            k_raw = data.get("k") or data.get("top_k") or data.get("limit")
            filter_source = data
        else:
            q_raw = request.args.get("q") or request.args.get("question") or request.args.get("text") or ""
            k_raw = request.args.get("k") or request.args.get("top_k") or request.args.get("limit")
            filter_source = request.args

        q = str(q_raw or "").strip()
        try:
            k = int(k_raw) if k_raw is not None else RETRIEVAL_TOP_K
        except (TypeError, ValueError):
            k = RETRIEVAL_TOP_K
        if k < 1:
            k = 1
        if k > 25:
            k = 25

        if not q:
            empty_payload = {"answer": "", "sources": [], "contexts": [], "meta": {}}
            return _compat_response(empty_payload, [], 0.0, 200, started_at=started_at)

        explicit_filters = {
            key: (filter_source.get(key, "") if hasattr(filter_source, "get") else "")
            for key in ("docset", "namespace", "ticker", "company")
            if (filter_source.get(key) if hasattr(filter_source, "get") else None)
        }
        scope = infer_scope(q, explicit_filters=explicit_filters, entities=extract_entities(q))
        filters = scope.applied_filters or {}

        try:
            emb = embed_text(q, settings=_EMBED_SETTINGS)
        except Exception as exc:
            error_payload = {
                "answer": "",
                "sources": [],
                "contexts": [],
                "meta": {"stage": "embed", "error": str(exc)},
                "error": f"embed_failed: {exc}",
                "trace": format_exc(),
            }
            return _compat_response(error_payload, [], 0.0, 502, started_at=started_at)

        try:
            rows = top_k_by_vector(emb, k=k, filters=filters)
        except Exception as exc:
            error_payload = {
                "answer": "",
                "sources": [],
                "contexts": [],
                "meta": {"stage": "db", "error": str(exc)},
                "error": f"db_failed: {exc}",
                "trace": format_exc(),
            }
            return _compat_response(error_payload, [], 0.0, 500, started_at=started_at)

        rows = dedupe_contexts(rows)
        contexts = []
        top_score = None
        for row in rows:
            score = compute_similarity(row.get("dist"))
            if top_score is None and score is not None:
                top_score = score
            contexts.append(
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
            "insufficient_context": insufficient and not SC_RAG_FAIL_OPEN,
            "compat_thresholds": {"min_score": SC_RAG_MIN_SCORE, "min_contexts": SC_RAG_MIN_CONTEXTS},
            "fail_open": SC_RAG_FAIL_OPEN,
        }
        if insufficient and not SC_RAG_FAIL_OPEN:
            scope_meta["message"] = INSUFFICIENT_CONTEXT_MESSAGE

        payload = {"answer": "", "sources": contexts, "contexts": contexts, "meta": scope_meta}
        return _compat_response(payload, contexts, top_score, 200, started_at=started_at)
