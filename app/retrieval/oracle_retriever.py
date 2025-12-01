"""Capability-aware Oracle retriever with vector and text fallbacks."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Mapping

from db_helper import _build_filter_clause, get_connection, top_k_by_vector

from .db_capability import Capability, capability_snapshot as _capability_snapshot, get_capability
from .embedding_client import embed_text
from .settings import settings

try:  # pragma: no cover - optional dependency
    import oracledb  # type: ignore
except Exception as exc:  # pragma: no cover - optional dependency
    oracledb = None  # type: ignore
    _ORACLE_IMPORT_ERROR = exc
else:  # pragma: no cover - optional dependency
    _ORACLE_IMPORT_ERROR = None


LOGGER = logging.getLogger("app.retrieval.oracle")


@dataclass
class RetrievalResult:
    """Return type for capability-aware retrieval."""

    contexts: List[Dict[str, Any]]
    mode: str
    latency_ms: int
    capability: Capability
    note: str = ""


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:  # pragma: no cover - depends on driver
            pass
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:  # pragma: no cover - best effort
            return value.decode("latin-1", "ignore")
    return value


def _normalize_score(value: Any, *, invert_distance: bool = False, max_scale: float = 100.0) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if invert_distance:
        numeric = 1.0 - numeric
    if max_scale and max_scale > 0:
        numeric = numeric / max_scale
    numeric = max(0.0, min(1.0, numeric))
    return round(numeric, 4)


def _prepare_filter_clause(filters: Optional[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
    clause, binds = _build_filter_clause(filters or {})
    clause = clause.strip()
    if clause.upper().startswith("WHERE "):
        clause = clause[6:].strip()
    return clause, binds


class OracleRetriever:
    """Retrieve ESG contexts using Oracle vectors with graceful fallback."""

    def __init__(self) -> None:
        self.table = (settings.oracle_table or "ESG_DOCS").strip().upper()
        self.text_column = (settings.oracle_text_column or "CHUNK_TEXT").strip().upper()
        self.url_column = (settings.oracle_url_column or "SOURCE_URL").strip().upper()
        self.title_column = (settings.oracle_title_column or "TITLE").strip().upper()
        self.source_column = (settings.oracle_source_column or "SOURCE_NAME").strip().upper()
        self.doc_id_column = (settings.oracle_doc_id_column or "DOC_ID").strip().upper()
        self.chunk_ix_column = (settings.oracle_chunk_ix_column or "CHUNK_IX").strip().upper()
        self.metric = settings.oracle_knn_metric
        self._warned_dim_mismatch = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def retrieve(
        self,
        question: str,
        k: int,
        *,
        prefer_vector: Optional[bool] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> RetrievalResult:
        clean_question = (question or "").strip()
        capability = get_capability()
        if not clean_question:
            return RetrievalResult([], "none", 0, capability, note="empty_question")

        prefer_vector = settings.rag_prefer_vector if prefer_vector is None else bool(prefer_vector)
        filter_payload = dict(filters.items()) if isinstance(filters, Mapping) else None

        start = time.perf_counter()
        contexts: List[Dict[str, Any]] = []
        mode = "none"
        note = ""

        use_vector = (
            prefer_vector
            and capability.vector_supported
            and capability.vec_col is not None
            and capability.vector_rows > 0
            and oracledb is not None
        )

        if use_vector:
            try:
                contexts = self._vector_search(clean_question, k, capability, filter_payload)
                mode = "vector"
                note = "vector_search"
            except Exception as exc:
                LOGGER.warning("vector_search_failed; falling back to text: %s", exc)
                capability = get_capability(refresh=True)
                contexts = []
                mode = "vector_fail"

        if not contexts:
            text_mode = self._select_text_mode(capability)
            try:
                if text_mode == "oracle_text":
                    contexts = self._oracle_text_search(clean_question, k, capability, filter_payload)
                    mode = "oracle_text"
                else:
                    contexts = self._like_search(clean_question, k, capability, filter_payload)
                    mode = "like"
                note = note or f"{mode}_fallback"
            except Exception as exc:
                LOGGER.warning("text_search_failed: mode=%s err=%s", mode, exc)
                contexts = []
                note = "text_search_failed"

        latency_ms = int((time.perf_counter() - start) * 1000)
        return RetrievalResult(contexts, mode, latency_ms, capability, note=note)

    # ------------------------------------------------------------------
    # Capability helpers
    # ------------------------------------------------------------------
    def capability_snapshot(self) -> Dict[str, Any]:
        snapshot = _capability_snapshot()
        snapshot["text_mode"] = self._select_text_mode(get_capability())
        return snapshot

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _vector_search(
        self,
        question: str,
        k: int,
        capability: Capability,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if oracledb is None:
            raise RuntimeError("oracledb_unavailable")

        embedding = embed_text(question, timeout=5.0)
        if capability.vec_dim and len(embedding) != capability.vec_dim and not self._warned_dim_mismatch:
            LOGGER.warning(
                "vector_dim_mismatch expected=%s actual=%s", capability.vec_dim, len(embedding)
            )
            self._warned_dim_mismatch = True
        elif (
            not capability.vec_dim
            and settings.rag_expected_dim
            and len(embedding) != settings.rag_expected_dim
            and not self._warned_dim_mismatch
        ):
            LOGGER.warning(
                "vector_dim_mismatch expected_env=%s actual=%s", settings.rag_expected_dim, len(embedding)
            )
            self._warned_dim_mismatch = True

        limit = max(1, min(int(k or settings.oracle_knn_k), 64))
        # Reuse the hardened helper used by /ask2_direct to avoid driver quirks.
        rows = top_k_by_vector(
            embedding,
            k=limit,
            filters=filters or {},
        )

        return self._shape_contexts(rows, score_key="dist", invert_distance=True, max_scale=1.0)

    def _select_text_mode(self, capability: Capability) -> str:
        override = settings.rag_text_mode
        if override in {"oracle_text", "like"}:
            if override == "oracle_text" and not capability.oracle_text_supported:
                return "like"
            return override
        return "oracle_text" if capability.oracle_text_supported else "like"

    def _oracle_text_search(
        self,
        question: str,
        k: int,
        capability: Capability,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(k or settings.oracle_knn_k), 64))
        filter_clause, filter_binds = _prepare_filter_clause(filters)
        conditions = [f"CONTAINS({self.text_column}, :query, 1) > 0"]
        if filter_clause:
            conditions.append(filter_clause)
        where_sql = " WHERE " + " AND ".join(conditions)
        select = [
            f"{self.doc_id_column} AS DOC_ID",
            f"{self.title_column} AS TITLE",
            f"{self.source_column} AS SOURCE_NAME",
            f"{self.url_column} AS SOURCE_URL",
            f"{self.text_column} AS CHUNK_TEXT",
            "SCORE(1) AS SCORE",
        ]
        sql = (
            "SELECT "
            + ", ".join(select)
            + f" FROM {self.table}"
            + where_sql
            + " ORDER BY SCORE(1) DESC"
            + f" FETCH FIRST {limit} ROWS ONLY"
        )

        params = {"query": question}
        params.update(filter_binds)
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                columns = [col[0].lower() for col in cursor.description]
                rows = [dict(zip(columns, map(_to_plain, rec))) for rec in cursor.fetchall()]
        except Exception as exc:
            raise RuntimeError(f"oracle_text_query_failed: {exc}") from exc

        return self._shape_contexts(rows, score_key="score", max_scale=100.0)

    def _like_search(
        self,
        question: str,
        k: int,
        capability: Capability,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(k or settings.oracle_knn_k), 64))
        pattern = f"%{question.strip().upper()}%"
        filter_clause, filter_binds = _prepare_filter_clause(filters)
        select = [
            f"{self.doc_id_column} AS DOC_ID",
            f"{self.title_column} AS TITLE",
            f"{self.source_column} AS SOURCE_NAME",
            f"{self.url_column} AS SOURCE_URL",
            f"{self.text_column} AS CHUNK_TEXT",
            "MATCH_SCORE AS SCORE",
        ]
        where_condition = (
            f"  WHERE (UPPER({self.title_column}) LIKE :title"
            f"\n     OR UPPER({self.source_column}) LIKE :title"
            f"\n     OR UPPER({self.text_column}) LIKE :body)"
        )
        if filter_clause:
            where_condition += f"\n    AND {filter_clause}"
        sql = "\n".join(
            [
                "WITH ranked AS (",
                f" SELECT {self.doc_id_column} AS DOC_ID,",
                f"        {self.title_column} AS TITLE,",
                f"        {self.source_column} AS SOURCE_NAME,",
                f"        {self.url_column} AS SOURCE_URL,",
                f"        {self.text_column} AS CHUNK_TEXT,",
                "        CASE",
                f"            WHEN UPPER({self.title_column}) LIKE :title THEN 100",
                f"            WHEN UPPER({self.source_column}) LIKE :title THEN 80",
                f"            WHEN UPPER({self.text_column}) LIKE :body THEN 60",
                "            ELSE 30",
                "        END AS MATCH_SCORE",
                f"   FROM {self.table}",
                where_condition,
                ")",
                " SELECT " + ", ".join(select),
                " FROM ranked",
                " ORDER BY MATCH_SCORE DESC",
                f" FETCH FIRST {limit} ROWS ONLY",
            ]
        )
        params = {"title": pattern, "body": pattern}
        params.update(filter_binds)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0].lower() for col in cursor.description]
            rows = [dict(zip(columns, map(_to_plain, rec))) for rec in cursor.fetchall()]

        return self._shape_contexts(rows, score_key="score", max_scale=100.0)

    def _shape_contexts(
        self,
        rows: List[Dict[str, Any]],
        *,
        score_key: str,
        invert_distance: bool = False,
        max_scale: float = 100.0,
    ) -> List[Dict[str, Any]]:
        shaped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            context: Dict[str, Any] = {}
            doc_id = row.get("doc_id")
            if doc_id is not None:
                context["doc_id"] = str(doc_id)
            title = row.get("title") or row.get("source_name")
            if isinstance(title, str) and title.strip():
                context["title"] = title.strip()
            source_url = row.get("source_url")
            if isinstance(source_url, str) and source_url.strip():
                context["source_url"] = source_url.strip()
            chunk = row.get("chunk_text")
            if isinstance(chunk, str) and chunk.strip():
                context["chunk_text"] = chunk.strip()
            score = _normalize_score(row.get(score_key), invert_distance=invert_distance, max_scale=max_scale)
            if score is not None:
                context["score"] = score
            key = (context.get("source_url") or context.get("doc_id") or "").lower()
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            if context:
                shaped.append(context)
        return shaped


retriever = OracleRetriever()


def retrieve(
    question: str,
    k: int,
    *,
    prefer_vector: Optional[bool] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> RetrievalResult:
    """Module-level convenience wrapper returning a :class:`RetrievalResult`."""

    return retriever.retrieve(question, k, prefer_vector=prefer_vector, filters=filters)


def capability_snapshot() -> Dict[str, Any]:
    """Expose the current capability snapshot for debugging responses."""

    return retriever.capability_snapshot()


__all__ = ["RetrievalResult", "capability_snapshot", "retrieve", "retriever"]
