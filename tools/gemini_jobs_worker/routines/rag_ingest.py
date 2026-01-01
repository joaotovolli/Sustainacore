"""RAG ingestion routine for the Gemini Jobs Worker."""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import db_helper
from embedding_client import embed_text
from embedder_settings import get_embed_settings

try:  # pragma: no cover - optional dependency
    import oracledb  # type: ignore
except Exception:  # pragma: no cover
    oracledb = None  # type: ignore

from ..chunking import chunk_text_boundary
from ..config import CHUNK_MAX_CHARS, CHUNK_OVERLAP_CHARS, PAYLOAD_MIME
from ..learned_notes import LearnedNote, append_learned_note

LOGGER = logging.getLogger("gemini_jobs_worker.rag_ingest")

TEXT_COLUMNS = [
    "chunk_text",
    "text",
    "content",
    "body",
    "details",
    "summary",
    "description",
]
TITLE_COLUMNS = ["title", "headline", "company", "name", "topic"]
COMPANY_COLUMNS = ["company", "name", "issuer", "organization"]
TICKER_COLUMNS = ["ticker", "symbol", "ric", "isin"]
SOURCE_URL_COLUMNS = ["source_url", "url", "link", "source_link"]
SOURCE_FIELDS = ["source", "sources", "citation", "citations", "reference", "references"]
DATE_COLUMNS = ["date", "published", "timestamp", "as_of"]
ROW_ID_COLUMNS = ["row_id", "id", "doc_id", "source_id", "record_id"]

SOURCE_TYPE_VALUE = "PROC_GEMINI_JOB"
EXPECTED_EMBED_DIM = 384
SUMMARY_SENTENCE_LIMIT = 3
_NON_WORD_RE = re.compile(r"[^a-zA-Z0-9]+")


@dataclass
class ApprovalPayload:
    payload_bytes: bytes
    file_name: str
    file_mime: str
    proposed_text: str
    details: str
    comments: str
    row_count: int
    chunk_count: int
    warnings: List[str]
    detected_columns: List[str]
    skipped_rows: List[str]
    quality_errors: List[str]


@dataclass
class InsertStats:
    inserted: int
    skipped_existing: int


class EmbeddingDimMismatch(RuntimeError):
    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(f"embedding_dim_mismatch expected={expected} actual={actual}")
        self.expected = expected
        self.actual = actual


def _decode_bytes(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _normalize_headers(fieldnames: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for name in fieldnames:
        if not name:
            continue
        mapping[name.strip().lower()] = name
    return mapping


def _pick_column(headers: Dict[str, str], candidates: Iterable[str]) -> Optional[str]:
    for candidate in candidates:
        key = candidate.lower()
        if key in headers:
            return headers[key]
    return None


def _slugify(value: str, max_len: int = 32) -> str:
    text = _NON_WORD_RE.sub("_", value.strip().lower()).strip("_")
    return text[:max_len] or "row"


def _row_key_from_row(row: Dict[str, str], header_map: Dict[str, str], row_index: int) -> str:
    for key in TICKER_COLUMNS + COMPANY_COLUMNS + ROW_ID_COLUMNS:
        col = header_map.get(key)
        if col:
            value = (row.get(col) or "").strip()
            if value:
                return value
    return str(row_index)


def _hash_key(value: str, length: int = 12) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:length]


def _normalize_chunk_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _format_source_id(job_id: int, row_key: str, chunk_ix: int, chunk_text: str) -> str:
    base = f"JOB{job_id}:ROW{row_key}:CH{chunk_ix}"
    digest = _hash_key(f"{base}:{_normalize_chunk_text(chunk_text)}", 12)
    slug = _slugify(row_key, 32)
    source_id = f"PGJ:{job_id}:{slug}:{chunk_ix}:{digest}"
    return source_id[:120]


def _parse_csv(text: str) -> Tuple[List[Dict[str, str]], List[str]]:
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], []
    rows: List[Dict[str, str]] = []
    for row in reader:
        rows.append({k: (v or "") for k, v in row.items()})
    return rows, [name for name in reader.fieldnames if name]


def _extract_date_range(values: List[str]) -> Tuple[Optional[str], Optional[str]]:
    cleaned = [v.strip() for v in values if v and v.strip()]
    if not cleaned:
        return None, None
    cleaned.sort()
    return cleaned[0], cleaned[-1]


def _split_sentences(text: str) -> List[str]:
    sentences: List[str] = []
    buf: List[str] = []
    for ch in text.strip():
        buf.append(ch)
        if ch in ".!?":
            sentence = "".join(buf).strip()
            if sentence:
                sentences.append(sentence)
            buf = []
    tail = "".join(buf).strip()
    if tail:
        sentences.append(tail)
    return sentences


def _summary_from_body(body: str) -> str:
    sentences = _split_sentences(body)
    if not sentences:
        return ""
    return " ".join(sentences[:SUMMARY_SENTENCE_LIMIT]).strip()


def _format_chunk_text(
    *,
    company: str,
    ticker: str,
    section: str,
    summary: str,
    evidence: List[str],
    sources: List[str],
    metadata: List[str],
) -> str:
    lines: List[str] = []
    if company:
        if ticker:
            lines.append(f"Company: {company} ({ticker})")
        else:
            lines.append(f"Company: {company}")
    else:
        lines.append("Company: Unknown")
    lines.append(f"Section: {section}")
    if summary:
        lines.append(f"Summary: {summary}")
    if evidence:
        lines.append("Evidence:")
        lines.extend(f"- {item}" for item in evidence)
    if sources:
        lines.append("Sources:")
        lines.extend(f"- {item}" for item in sources)
    if metadata:
        lines.append("Metadata: " + ", ".join(metadata))
    return "\n".join(lines)


def _build_chunks(
    *,
    header_text: str,
    body_text: str,
    overlap: int,
) -> List[str]:
    return chunk_text_boundary(
        body_text,
        header=header_text,
        target=CHUNK_MAX_CHARS,
        min_len=max(800, CHUNK_MAX_CHARS - 400),
        max_len=max(CHUNK_MAX_CHARS, 1400),
        overlap=overlap,
    )


def _internal_source_url(job_id: int, row_key: str, chunk_ix: int) -> str:
    slug = _slugify(row_key, 40)
    return f"sustainacore://proc_gemini_job/{job_id}/row/{slug}/chunk/{chunk_ix}"


def _validate_payload_rows(rows: List[Dict[str, str]]) -> List[str]:
    errors: List[str] = []
    source_ids = set()
    per_row_ix: Dict[str, int] = {}

    for row in rows:
        source_id = (row.get("source_id") or "").strip()
        if not source_id:
            errors.append("Missing source_id in payload row.")
        elif source_id in source_ids:
            errors.append(f"Duplicate source_id detected: {source_id}")
        else:
            source_ids.add(source_id)

        chunk_text = (row.get("chunk_text") or "").strip()
        if not chunk_text.startswith("Company:"):
            errors.append("Chunk text does not start with 'Company:' header.")

        row_key = (row.get("row_key") or "").strip()
        try:
            chunk_ix = int(row.get("chunk_ix") or 0)
        except ValueError:
            chunk_ix = 0
        if row_key:
            last_ix = per_row_ix.get(row_key, 0)
            if chunk_ix != last_ix + 1:
                errors.append(f"Chunk index sequence break for row_key={row_key} (got {chunk_ix}).")
            per_row_ix[row_key] = chunk_ix

    return errors


def build_approval_payload(job_id: int, file_bytes: bytes) -> ApprovalPayload:
    warnings: List[str] = []
    skipped_rows: List[str] = []
    try:
        text = _decode_bytes(file_bytes)
        rows, headers = _parse_csv(text)
    except Exception as exc:
        warnings.append("CSV parse failed; no rows read.")
        append_learned_note(LearnedNote("CSV parse failed", context=str(exc)[:200]))
        rows, headers = [], []

    header_map = _normalize_headers(headers)
    detected_columns = [name for name in headers if name]

    text_col = _pick_column(header_map, TEXT_COLUMNS)
    title_col = _pick_column(header_map, TITLE_COLUMNS)
    source_url_col = _pick_column(header_map, SOURCE_URL_COLUMNS)
    date_col = _pick_column(header_map, DATE_COLUMNS)

    if not rows:
        warnings.append("No rows found in CSV.")
    if not text_col:
        warnings.append("No text column detected; no chunks will be generated.")
        append_learned_note(
            LearnedNote("Missing text column for RAG_INGEST", context=", ".join(detected_columns)[:200])
        )

    payload_rows: List[Dict[str, str]] = []
    date_values: List[str] = []
    row_count = 0
    chunk_count = 0

    for idx, row in enumerate(rows, start=1):
        row_count += 1
        row_key = _row_key_from_row(row, header_map, idx)
        company = (row.get(_pick_column(header_map, COMPANY_COLUMNS) or "") or row_key).strip()
        ticker = (row.get(_pick_column(header_map, TICKER_COLUMNS) or "") or "").strip()
        title = (row.get(title_col) if title_col else "")
        if not title:
            if ticker:
                title = f"{company} ({ticker}) – AI Governance & Ethics"
            else:
                title = f"{company} – AI Governance & Ethics"

        body = (row.get(text_col) if text_col else "") or ""
        if not body.strip():
            skipped_rows.append(f"row {idx}: missing body text")
            continue

        source_url = (row.get(source_url_col) if source_url_col else "") or ""
        date_value = (row.get(date_col) if date_col else "") or ""
        if date_value:
            date_values.append(date_value)

        sources: List[str] = []
        for header_key, header_name in header_map.items():
            if header_key in SOURCE_FIELDS and header_name != source_url_col:
                value = (row.get(header_name) or "").strip()
                if value:
                    sources.append(value)
        if source_url:
            sources.append(source_url)

        evidence: List[str] = []
        for header_name in headers:
            if header_name in {text_col, title_col, source_url_col, date_col}:
                continue
            value = (row.get(header_name) or "").strip()
            if value:
                evidence.append(f"{header_name}: {value}")
            if len(evidence) >= 8:
                break

        metadata: List[str] = []
        if date_value:
            metadata.append(f"as_of={date_value}")
        if ticker:
            metadata.append(f"ticker={ticker}")

        summary = _summary_from_body(body)
        header_text = _format_chunk_text(
            company=company,
            ticker=ticker,
            section="AI Governance & Ethics Signals",
            summary=summary,
            evidence=evidence,
            sources=sources,
            metadata=metadata,
        )

        chunks = _build_chunks(
            header_text=header_text,
            body_text=body,
            overlap=CHUNK_OVERLAP_CHARS,
        )
        if not chunks:
            skipped_rows.append(f"row {idx}: no chunks produced")
            continue

        for c_ix, chunk in enumerate(chunks, start=1):
            chunk_count += 1
            source_id = _format_source_id(job_id, row_key, c_ix, chunk)
            resolved_url = source_url or _internal_source_url(job_id, row_key, c_ix)
            payload_rows.append(
                {
                    "row_key": row_key,
                    "source_type": SOURCE_TYPE_VALUE,
                    "source_id": source_id,
                    "source_url": resolved_url,
                    "title": title.strip(),
                    "chunk_ix": str(c_ix),
                    "chunk_text": chunk,
                }
            )

    quality_errors = _validate_payload_rows(payload_rows)
    if not payload_rows:
        quality_errors.append("No payload rows generated.")

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "source_type",
            "source_id",
            "source_url",
            "title",
            "chunk_ix",
            "chunk_text",
        ],
    )
    writer.writeheader()
    for row in payload_rows:
        writer.writerow({key: row[key] for key in writer.fieldnames})

    payload_bytes = output.getvalue().encode("utf-8")
    date_start, date_end = _extract_date_range(date_values)

    summary_parts = [f"rows={row_count}", f"chunks={chunk_count}"]
    if date_start and date_end:
        summary_parts.append(f"date_range={date_start}..{date_end}")
    summary_parts.append(f"detected_columns={len(detected_columns)}")
    summary_parts.append(
        "key_columns=text:{},title:{},source_url:{}".format(
            text_col or "MISSING",
            title_col or "MISSING",
            source_url_col or "MISSING",
        )
    )
    summary_parts.append("dedupe=SOURCE_ID")
    summary_parts.append("expected_embed_dim=384")
    proposed_text = "Summary: Prepared payload for ESG_DOCS insertion: " + ", ".join(summary_parts)

    mapping_lines = [
        "Schema mapping:",
        f"- text: {text_col or 'MISSING'}",
        f"- title: {title_col or 'MISSING'}",
        f"- source_url: {source_url_col or 'MISSING'}",
        f"- date: {date_col or 'MISSING'}",
        "- source_type: PROC_GEMINI_JOB",
    ]

    plan_lines = [
        "Plan:",
        "- Build chunk_text with structured headers.",
        "- Chunk text on boundary-aligned sentences.",
        "- Create deterministic SOURCE_ID values.",
        "- Insert rows into ESG_DOCS after approval, skipping existing SOURCE_ID values.",
        "- Generate embeddings using the configured embedding service (384 dimensions).",
    ]

    warning_lines = ["Warnings:"] + (warnings if warnings else ["None"]) 
    if skipped_rows:
        warning_lines.append("Skipped rows: " + "; ".join(skipped_rows))

    sample_chunk = payload_rows[0]["chunk_text"] if payload_rows else ""
    sample_header = " | ".join(sample_chunk.splitlines()[:2]) if sample_chunk else ""

    comments = (
        "GEMINI_COMMENTS: rows={} chunks={} detected_columns=[{}] dedupe=SOURCE_ID warnings={}"
    ).format(
        row_count,
        chunk_count,
        ", ".join(detected_columns)[:400],
        "; ".join(warnings) if warnings else "None",
    )

    details = "\n".join(
        plan_lines
        + [""]
        + mapping_lines
        + [""]
        + warning_lines
        + [""]
        + [f"Example chunk header: {sample_header}"]
        + [f"Quality checks: starts_with_company={'OK' if not quality_errors else 'FAIL'}"]
        + [comments]
    )

    file_name = f"JOB_{job_id}_ESG_DOCS_payload.csv"
    return ApprovalPayload(
        payload_bytes=payload_bytes,
        file_name=file_name,
        file_mime=PAYLOAD_MIME,
        proposed_text=proposed_text,
        details=details,
        comments=comments,
        row_count=row_count,
        chunk_count=chunk_count,
        warnings=warnings,
        detected_columns=detected_columns,
        skipped_rows=skipped_rows,
        quality_errors=quality_errors,
    )


def _batch_existing_source_ids(conn, table_name: str, source_ids: List[str]) -> List[str]:
    if not source_ids:
        return []
    existing: List[str] = []
    cur = conn.cursor()
    batch_size = 900
    for i in range(0, len(source_ids), batch_size):
        chunk = source_ids[i : i + batch_size]
        binds = {f"id_{idx}": value for idx, value in enumerate(chunk)}
        placeholders = ", ".join(f":id_{idx}" for idx in range(len(chunk)))
        cur.execute(
            f"SELECT source_id FROM {table_name} WHERE source_id IN ({placeholders})",
            binds,
        )
        existing.extend(row[0] for row in cur.fetchall())
    return existing


def _parse_payload_rows(payload_bytes: bytes) -> List[Dict[str, str]]:
    text = _decode_bytes(payload_bytes)
    rows, _ = _parse_csv(text)
    return rows


def apply_payload(conn, payload_bytes: bytes) -> InsertStats:
    rows = _parse_payload_rows(payload_bytes)
    if not rows:
        return InsertStats(inserted=0, skipped_existing=0)

    table_info = db_helper.get_vector_column("ESG_DOCS")
    vector_col = table_info.get("column") or "EMBEDDING"
    table_name = table_info.get("table") or "ESG_DOCS"

    source_ids = [row.get("source_id", "").strip() for row in rows if row.get("source_id")]
    existing = set(_batch_existing_source_ids(conn, table_name, source_ids))

    insert_sql = (
        f"INSERT INTO {table_name} "
        f"(SOURCE_TYPE, SOURCE_ID, SOURCE_URL, TITLE, CHUNK_IX, CHUNK_TEXT, {vector_col}) "
        "VALUES (:source_type, :source_id, :source_url, :title, :chunk_ix, :chunk_text, :embedding)"
    )

    settings = get_embed_settings()
    if settings.expected_dimension != EXPECTED_EMBED_DIM:
        raise EmbeddingDimMismatch(EXPECTED_EMBED_DIM, settings.expected_dimension)

    cur = conn.cursor()
    if oracledb is not None and hasattr(oracledb, "DB_TYPE_VECTOR"):
        try:
            cur.setinputsizes(embedding=oracledb.DB_TYPE_VECTOR)
        except Exception:
            pass

    inserted = 0
    skipped_existing = 0
    batch: List[Dict[str, object]] = []
    for row in rows:
        source_id = (row.get("source_id") or "").strip()
        if not source_id:
            continue
        if source_id in existing:
            skipped_existing += 1
            continue
        chunk_text = (row.get("chunk_text") or "").strip()
        if not chunk_text:
            continue
        try:
            chunk_ix = int(row.get("chunk_ix") or 0)
        except ValueError:
            chunk_ix = 0
        embedding = embed_text(chunk_text, timeout=15.0, settings=settings)
        if len(embedding) != EXPECTED_EMBED_DIM:
            raise EmbeddingDimMismatch(EXPECTED_EMBED_DIM, len(embedding))
        batch.append(
            {
                "source_type": (row.get("source_type") or SOURCE_TYPE_VALUE).strip(),
                "source_id": source_id,
                "source_url": (row.get("source_url") or "").strip(),
                "title": (row.get("title") or "").strip(),
                "chunk_ix": chunk_ix or None,
                "chunk_text": chunk_text,
                "embedding": embedding,
            }
        )
        existing.add(source_id)
        if len(batch) >= 50:
            cur.executemany(insert_sql, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        cur.executemany(insert_sql, batch)
        inserted += len(batch)
    conn.commit()
    return InsertStats(inserted=inserted, skipped_existing=skipped_existing)
