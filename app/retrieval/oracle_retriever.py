"""Oracle 23ai retriever that honours the Gemini-first contract."""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import oracledb

from .settings import settings


LOGGER = logging.getLogger("oracle-retriever")


def _read_env_file_var(path: str, key: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                k, _, v = line.partition("=")
                if k.strip() == key:
                    return v.strip()
    except OSError:
        return None
    return None


def _connection() -> oracledb.Connection:
    password = (
        os.environ.get("DB_PASSWORD")
        or os.environ.get("DB_PASS")
        or os.environ.get("DB_PWD")
        or _read_env_file_var("/etc/sustainacore/db.env", "DB_PASSWORD")
    )
    return oracledb.connect(
        user=os.environ.get("DB_USER", "WKSP_ESGAPEX"),
        password=password,
        dsn=os.environ.get("DB_DSN", "dbri4x6_high"),
        config_dir=os.environ.get("TNS_ADMIN", "/opt/adb_wallet"),
        wallet_location=os.environ.get("TNS_ADMIN", "/opt/adb_wallet"),
        wallet_password=os.environ.get("WALLET_PWD"),
    )


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    filtered_params = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=False)
        if not k.lower().startswith(("utm_", "session", "ref", "fbclid", "gclid"))
    ]
    normalized_query = urlencode(filtered_params, doseq=True)
    sanitized = parsed._replace(query=normalized_query, fragment="")
    if sanitized.scheme in {"http", "https"}:
        sanitized = sanitized._replace(netloc=sanitized.netloc.lower())
    return urlunparse(sanitized)


def _infer_source_name(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    host = parsed.netloc.lower()
    return host.split(":", 1)[0]


def _to_plain(value: Any) -> Any:
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:  # pragma: no cover - defensive
            return str(value)
    if isinstance(value, oracledb.Vector):  # type: ignore[attr-defined]
        return list(value)
    return value


def _safe_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%b-%Y", "%Y-%m", "%Y"):
            try:
                parsed = _dt.datetime.strptime(text[:len(fmt)], fmt)
                return parsed.date().isoformat()
            except ValueError:
                continue
        return text
    return str(value)


def _score_from_distance(distance: Any) -> Optional[float]:
    try:
        dist = float(distance)
    except (TypeError, ValueError):
        return None
    if dist != dist:  # NaN check
        return None
    score = 1.0 - dist
    if score > 1.0:
        score = 1.0
    if score < 0.0:
        score = 0.0
    return round(score, 4)


@dataclass
class RetrievalResult:
    facts: List[Dict[str, Any]]
    context_note: str
    latency_ms: int
    candidates: int
    deduped: int
    hop_count: int
    raw_facts: Optional[List[Dict[str, Any]]] = None


class OracleRetriever:
    """Implements the Oracle retriever contract for Gemini."""

    def __init__(self) -> None:
        self.table = settings.oracle_table.upper()
        self.embedding_column = settings.oracle_embedding_column.upper()
        self.text_column = settings.oracle_text_column.upper()
        self.url_column = settings.oracle_url_column.upper()
        self.norm_url_column = settings.oracle_normalized_url_column.upper()
        self.title_column = settings.oracle_title_column.upper()
        self.source_column = settings.oracle_source_column.upper()
        self.date_column = settings.oracle_date_column.upper()
        self.doc_id_column = settings.oracle_doc_id_column.upper()
        self.source_id_column = settings.oracle_source_id_column.upper()
        self.chunk_ix_column = settings.oracle_chunk_ix_column.upper()
        self.metric = settings.oracle_knn_metric
        self._available_columns: set[str] = set()
        self._refresh_columns()
        self._verify_embeddings()

    # ---- metadata helpers -------------------------------------------------

    def _refresh_columns(self) -> None:
        try:
            with _connection() as conn:
                owner = conn.username.upper()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT column_name FROM ALL_TAB_COLUMNS WHERE OWNER = :owner AND TABLE_NAME = :table",
                        owner=owner,
                        table=self.table,
                    )
                    cols = [row[0].upper() for row in cur]
                except oracledb.DatabaseError:
                    cur = conn.cursor()
                    cur.execute("SELECT column_name FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :table", table=self.table)
                    cols = [row[0].upper() for row in cur]
        except Exception as exc:  # pragma: no cover - requires DB
            LOGGER.warning("Failed to introspect Oracle columns: %s", exc)
            cols = []
        self._available_columns = set(cols)

    def _has_column(self, column: str) -> bool:
        return column.upper() in self._available_columns

    def _verify_embeddings(self) -> None:
        if not self._has_column(self.embedding_column):
            LOGGER.warning("Embedding column %s not found in %s", self.embedding_column, self.table)
            return
        try:
            with _connection() as conn:
                cur = conn.cursor()
                query = (
                    f"SELECT MIN(vector_dims({self.embedding_column})), MAX(vector_dims({self.embedding_column})) "
                    f"FROM {self.table}"
                )
                cur.execute(query)
                row = cur.fetchone()
                if not row:
                    return
                min_dim, max_dim = row
                if min_dim != max_dim:
                    LOGGER.warning("Embedding dimensions vary across rows: min=%s max=%s", min_dim, max_dim)
                if max_dim and int(max_dim) != 384:
                    LOGGER.warning("Expected 384-dim embeddings, got %s", max_dim)
        except Exception as exc:  # pragma: no cover - requires DB
            LOGGER.warning("Could not verify embedding dimensions: %s", exc)

    # ---- embedding --------------------------------------------------------

    def _embed(self, conn: oracledb.Connection, text: str) -> Optional[Sequence[float]]:
        text = (text or "").strip()
        if not text:
            return None
        try:
            cur = conn.cursor()
            sql = settings.oracle_embed_sql
            binds = {"model": settings.oracle_embed_model, "text": text}
            cur.execute(sql, binds)
            row = cur.fetchone()
            if not row:
                return None
            vec = _to_plain(row[0])
            if isinstance(vec, (list, tuple)):
                return [float(x) for x in vec]
        except Exception as exc:  # pragma: no cover - requires DB
            LOGGER.error("Oracle embed failed: %s", exc)
        return None

    # ---- SQL construction -------------------------------------------------

    def _build_filter_clause(self, filters: Dict[str, Any], binds: Dict[str, Any]) -> str:
        clauses: List[str] = []
        counter = 0
        for key, value in (filters or {}).items():
            if not key:
                continue
            key_u = key.upper()
            if key_u == "SOURCE_TYPE" and self._has_column("SOURCE_TYPE"):
                values = value if isinstance(value, list) else [value]
                clean = [str(v).strip() for v in values if str(v).strip()]
                if clean:
                    names = []
                    for item in clean:
                        bind = f"p{counter}"; counter += 1
                        binds[bind] = item
                        names.append(f":{bind}")
                    clauses.append("UPPER(SOURCE_TYPE) IN (" + ",".join(names) + ")")
            elif key_u == "TICKER" and self._has_column("TICKER"):
                values = value if isinstance(value, list) else [value]
                clean = [str(v).strip().upper() for v in values if str(v).strip()]
                if clean:
                    names = []
                    for item in clean:
                        bind = f"p{counter}"; counter += 1
                        binds[bind] = item
                        names.append(f":{bind}")
                    clauses.append("UPPER(TICKER) IN (" + ",".join(names) + ")")
            elif key_u == "DOC_ID" and self._has_column(self.doc_id_column):
                bind = f"p{counter}"; counter += 1
                binds[bind] = str(value)
                clauses.append(f"{self.doc_id_column} = :{bind}")
            elif key_u == "SOURCE_ID" and self._has_column(self.source_id_column):
                bind = f"p{counter}"; counter += 1
                binds[bind] = str(value)
                clauses.append(f"{self.source_id_column} = :{bind}")
            elif key_u == "DATE_FROM" and self._has_column(self.date_column):
                bind = f"p{counter}"; counter += 1
                binds[bind] = str(value)
                clauses.append(f"{self.date_column} >= TO_DATE(:{bind}, 'YYYY-MM-DD')")
            elif key_u == "DATE_TO" and self._has_column(self.date_column):
                bind = f"p{counter}"; counter += 1
                binds[bind] = str(value)
                clauses.append(f"{self.date_column} <= TO_DATE(:{bind}, 'YYYY-MM-DD')")
        return (" AND ".join(clauses)) if clauses else ""

    def _select_clause(self) -> str:
        cols: List[str] = []
        if self._has_column(self.doc_id_column):
            cols.append(f"{self.doc_id_column} AS DOC_ID")
        if self._has_column(self.chunk_ix_column):
            cols.append(f"{self.chunk_ix_column} AS CHUNK_IX")
        if self._has_column(self.source_id_column):
            cols.append(f"{self.source_id_column} AS SOURCE_ID")
        if self._has_column(self.source_column):
            cols.append(f"{self.source_column} AS SOURCE_NAME")
        if self._has_column(self.title_column):
            cols.append(f"{self.title_column} AS TITLE")
        if self._has_column(self.url_column):
            cols.append(f"{self.url_column} AS SOURCE_URL")
        if self._has_column(self.norm_url_column):
            cols.append(f"{self.norm_url_column} AS NORMALIZED_URL")
        if self._has_column(self.date_column):
            cols.append(f"{self.date_column} AS PUBLISHED_DATE")
        if self._has_column(self.text_column):
            cols.append(f"{self.text_column} AS CHUNK_TEXT")
        cols.append(
            f"VECTOR_DISTANCE({self.embedding_column}, :vec, '{self.metric}') AS DIST"
        )
        return ", ".join(cols)

    def _vector_query(
        self,
        conn: oracledb.Connection,
        embedding: Sequence[float],
        filters: Dict[str, Any],
        k: int,
    ) -> List[Dict[str, Any]]:
        binds: Dict[str, Any] = {}
        filter_clause = self._build_filter_clause(filters, binds)
        where = f"WHERE {filter_clause}" if filter_clause else ""
        sql = (
            f"SELECT {self._select_clause()} FROM {self.table} {where} "
            f"ORDER BY VECTOR_DISTANCE({self.embedding_column}, :vec, '{self.metric}') "
            "FETCH FIRST :k ROWS ONLY"
        )
        cur = conn.cursor()
        cur.setinputsizes(vec=oracledb.DB_TYPE_VECTOR)
        binds.update({"vec": embedding, "k": int(k)})
        cur.execute(sql, binds)
        columns = [desc[0].lower() for desc in cur.description]
        rows: List[Dict[str, Any]] = []
        for raw in cur.fetchall():
            row = {col: _to_plain(value) for col, value in zip(columns, raw)}
            if "normalized_url" not in row or not row["normalized_url"]:
                row["normalized_url"] = _normalize_url(row.get("source_url"))
            rows.append(row)
        return rows

    # ---- deduplication ----------------------------------------------------

    @staticmethod
    def _canonical_key(row: Dict[str, Any]) -> str:
        for key in ("normalized_url", "source_id", "doc_id"):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
        chunk = row.get("chunk_ix")
        if chunk is not None and row.get("doc_id"):
            return f"{row.get('doc_id')}::{chunk}"
        return hashlib.sha1(json.dumps(row, sort_keys=True).encode("utf-8")).hexdigest()

    def _deduplicate_rows(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        last_index: Dict[str, int] = {}
        for idx, row in enumerate(rows):
            last_index[self._canonical_key(row)] = idx
        ordered: List[Dict[str, Any]] = []
        seen_idx = set()
        for idx, row in enumerate(rows):
            key = self._canonical_key(row)
            if last_index.get(key) != idx:
                continue
            if idx in seen_idx:
                continue
            ordered.append(row)
            seen_idx.add(idx)
        near_seen: Dict[Tuple[str, str], int] = {}
        collapsed: List[Dict[str, Any]] = []
        for row in ordered:
            title = (row.get("title") or "").strip().lower()
            snippet = (row.get("chunk_text") or "")[:200]
            snippet_hash = hashlib.sha1(snippet.encode("utf-8", "ignore")).hexdigest() if snippet else ""
            dedup_key = (title, snippet_hash)
            if dedup_key in near_seen:
                collapsed[near_seen[dedup_key]] = row
            else:
                near_seen[dedup_key] = len(collapsed)
                collapsed.append(row)
        limited: List[Dict[str, Any]] = []
        per_source: Dict[str, int] = {}
        per_url: Dict[str, int] = {}
        for row in collapsed:
            source_key = str(row.get("source_id") or row.get("source_name") or "").lower()
            url_key = str(row.get("normalized_url") or row.get("source_url") or "").lower()
            if source_key:
                if per_source.get(source_key, 0) >= settings.retriever_per_source_cap:
                    continue
            if url_key:
                if per_url.get(url_key, 0) >= 1:
                    continue
            limited.append(row)
            if source_key:
                per_source[source_key] = per_source.get(source_key, 0) + 1
            if url_key:
                per_url[url_key] = 1
            if len(limited) >= settings.retriever_max_facts:
                break
        return limited

    # ---- facts ------------------------------------------------------------

    def _row_to_fact(self, row: Dict[str, Any], idx: int) -> Dict[str, Any]:
        citation_source = (
            row.get("source_id")
            or row.get("doc_id")
            or row.get("normalized_url")
            or row.get("source_url")
            or f"FACT_{idx+1}"
        )
        citation = "".join(ch for ch in str(citation_source) if ch.isalnum() or ch in "-_")
        if not citation:
            citation = f"FACT_{idx+1}"
        snippet = (row.get("chunk_text") or "").strip()
        if snippet:
            snippet = " ".join(snippet.split())[:600]
        source_name = (
            (row.get("source_name") or "").strip()
            or _infer_source_name(row.get("normalized_url") or row.get("source_url"))
        )
        fact = {
            "citation_id": citation,
            "title": (row.get("title") or "").strip() or "Untitled excerpt",
            "source_name": source_name or "",
            "url": row.get("normalized_url") or row.get("source_url"),
            "date": _safe_date(row.get("published_date")),
            "snippet": snippet,
            "score": _score_from_distance(row.get("dist")),
        }
        return fact

    def _build_context_note(
        self,
        filters: Dict[str, Any],
        k: int,
        candidates: int,
        dedup_count: int,
        final_count: int,
    ) -> str:
        parts: List[str] = []
        if filters:
            formatted = ", ".join(
                f"{key}={value}" for key, value in filters.items() if value
            )
            parts.append(f"- filters applied: {formatted}")
        parts.append(f"- knn_top_k={k} → dedup={dedup_count} → final={final_count}")
        parts.append(f"- oracle_candidates={candidates}")
        return "\n".join(parts)

    # ---- public API -------------------------------------------------------

    def retrieve(
        self,
        filters: Dict[str, Any],
        query_variants: Iterable[str],
        k: int,
        hop_count: int = 1,
    ) -> RetrievalResult:
        start = time.time()
        k_eff = max(1, min(int(k or settings.oracle_knn_k), 64))
        rows: List[Dict[str, Any]] = []
        variants = [v for v in query_variants if isinstance(v, str) and v.strip()]
        if not variants:
            variants = [""]
        try:
            with _connection() as conn:
                for variant in variants:
                    embedding = self._embed(conn, variant)
                    if embedding is None:
                        LOGGER.warning("Failed to embed variant: %s", variant)
                        continue
                    chunk_rows = self._vector_query(conn, embedding, filters, k_eff)
                    rows.extend(chunk_rows)
        except Exception as exc:  # pragma: no cover - requires DB
            LOGGER.error("Oracle retrieval failed: %s", exc)
            rows = []
        deduped = self._deduplicate_rows(rows)
        facts = [self._row_to_fact(row, idx) for idx, row in enumerate(deduped[: settings.retriever_fact_cap])]
        context_note = self._build_context_note(filters, k_eff, len(rows), len(deduped), len(facts))
        latency_ms = int((time.time() - start) * 1000)
        raw = deduped if settings.show_debug_block else None
        return RetrievalResult(
            facts=facts,
            context_note=context_note,
            latency_ms=latency_ms,
            candidates=len(rows),
            deduped=len(deduped),
            hop_count=hop_count,
            raw_facts=raw,
        )


retriever = OracleRetriever()


