
# SustainaCore app.py — SMART v2
import base64
import importlib
import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
import zlib
from datetime import date, datetime
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow this module to expose the sibling package under app/
__path__ = [str(Path(__file__).resolve().parent / "app")]

import requests
from collections import defaultdict
from flask import Flask, request, jsonify

from app.http.compat import (
    SC_RAG_FAIL_OPEN,
    SC_RAG_MIN_CONTEXTS,
    SC_RAG_MIN_SCORE,
    normalize_response,
)
from app.news_service import create_curated_news_item, fetch_news_items, fetch_news_item_detail
from app.retrieval.adapter import ask2_pipeline_first

from embedder_settings import (
    EmbedParityError,
    get_embed_settings,
    run_startup_parity_check,
)
from embedding_client import embed_text
from retrieval.config import (
    INSUFFICIENT_CONTEXT_MESSAGE,
    RETRIEVAL_SCOPING_ENABLED,
    RETRIEVAL_TOP_K,
    SIMILARITY_FLOOR,
    SIMILARITY_FLOOR_MODE,
)
from retrieval.scope import (
    compute_similarity,
    dedupe_contexts,
    detect_intent as scope_detect_intent,
    extract_entities as scope_extract_entities,
    infer_scope,
)
try:
    from smalltalk import smalltalk_response
except Exception:
    smalltalk_response = lambda _q: None
try:
    from db_helper import top_k_by_vector as _top_k_by_vector
except Exception:
    _top_k_by_vector = None

try:
    from gemini_adapter import generate as _gemini_generate  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _gemini_generate = None

def _load_ask2_routing_module():
    """Best-effort loader for ``app.rag.routing`` despite the module/package clash."""

    try:
        return importlib.import_module("app.rag.routing")
    except ModuleNotFoundError as exc:
        # When ``app`` refers to this module, importing ``app.rag`` fails. Fall back to
        # loading from the package path manually. Re-raise for unrelated missing deps.
        if exc.name not in {"app", "app.rag", "app.rag.routing"}:
            raise
    except Exception:
        return None

    pkg_root = Path(__file__).resolve().parent / "app"
    candidate = pkg_root / "rag" / "routing.py"
    if not candidate.exists():
        return None

    spec = importlib.util.spec_from_file_location("app_rag_routing", candidate)
    if spec is None or spec.loader is None:
        return None

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[arg-type]
    except Exception:
        return None
    return module


_ask2_routing = _load_ask2_routing_module()

try:  # Gemini-first shared service (optional)
    from app.retrieval.service import (
        GeminiUnavailableError as _GeminiUnavailableError,
        RateLimitError as _GeminiRateLimitError,
        run_pipeline as _gemini_run_pipeline,
    )
    from app.retrieval.settings import settings as _gemini_settings
except Exception:  # pragma: no cover - optional dependency
    class _GeminiRateLimitError(Exception):  # type: ignore[no-redef]
        detail = "rate_limited"

    class _GeminiUnavailableError(Exception):  # type: ignore[no-redef]
        pass

    _gemini_run_pipeline = None
    _gemini_settings = None


def _sanitize_meta_k(value, default=4):
    """Best-effort coercion of the ``k`` parameter for /ask2."""

    try:
        k_val = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        k_val = default
    if k_val < 1:
        k_val = 1
    if k_val > 10:
        k_val = 10
    return k_val


if _ask2_routing is not None:
    _route_ask2 = getattr(_ask2_routing, "route_ask2", None)
    _sanitize_meta_k = getattr(_ask2_routing, "_sanitize_k", _sanitize_meta_k)
    _format_router_sources = getattr(_ask2_routing, "format_sources", None)
    _router_is_smalltalk = getattr(_ask2_routing, "_is_smalltalk", None)
    _ASK2_ROUTER_FALLBACK = getattr(
        _ask2_routing,
        "NO_HIT_FALLBACK",
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look.",
    )
else:  # pragma: no cover - import fallback
    _route_ask2 = None
    _format_router_sources = None
    _router_is_smalltalk = None
    _ASK2_ROUTER_FALLBACK = (
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look."
    )


_LOGGER = logging.getLogger("app.ask2")
_READINESS_LOGGER = logging.getLogger("app.readyz")
_MULTI_LOGGER = logging.getLogger("app.multihit")
_STARTUP_LOGGER = logging.getLogger("app.startup")
_API_LOGGER = logging.getLogger("app.api")
_API_AUTH_TOKEN = os.getenv("BACKEND_API_TOKEN") or os.getenv("API_AUTH_TOKEN")
_API_AUTH_WARNED = False
_API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN") or os.getenv("BACKEND_API_TOKEN")
_API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN") or os.getenv("BACKEND_API_TOKEN")
_API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")
_API_AUTH_WARNED = False
_METRIC_LOGGER = logging.getLogger("app.ask2.metrics")
if not _METRIC_LOGGER.handlers:
    _metric_handler = logging.StreamHandler()
    _metric_handler.setLevel(logging.INFO)
    _METRIC_LOGGER.addHandler(_metric_handler)
    _METRIC_LOGGER.setLevel(logging.INFO)
    _METRIC_LOGGER.propagate = False


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _api_auth_guard() -> bool:
    """Return True when the Authorization header matches the configured token."""
def _api_auth_guard():
    """Return ``True`` when the Authorization header matches API_AUTH_TOKEN."""

    global _API_AUTH_WARNED

    if not _API_AUTH_TOKEN:
        if not _API_AUTH_WARNED:
            _LOGGER.warning("API_AUTH_TOKEN not set; denying /api/* requests")
            _API_AUTH_WARNED = True
        return False

    header = request.headers.get("Authorization", "")
    if not isinstance(header, str) or not header.lower().startswith("bearer "):
        return False

    candidate = header.split(" ", 1)[1].strip()
    return bool(candidate) and candidate == _API_AUTH_TOKEN


def _api_auth_or_unauthorized():
    """Return a 401 response when auth fails, or None when authorized."""
    """Return a 401 response when auth fails, or ``None`` when authorized."""

    if _api_auth_guard():
        return None
    return jsonify({"error": "unauthorized"}), 401


def _api_auth_optional():
    """
    Return a 401 only when an Authorization header is provided but invalid.

    When callers omit Authorization entirely, allow the request to proceed so
    token wiring is optional for read-only endpoints.
    """

    if not _API_AUTH_TOKEN:
        return None

    header = request.headers.get("Authorization", "")
    if not isinstance(header, str) or not header.strip():
        return None

    if _api_auth_guard():
        return None
    return jsonify({"error": "unauthorized"}), 401


def _api_auth_optional_or_unauthorized():
    """Allow requests without Authorization, but validate when provided."""

    header = request.headers.get("Authorization", "")
    if not header:
        return None

    if _api_auth_guard():
        return None
    return jsonify({"error": "unauthorized"}), 401


def _format_date(value: Any):
    """Normalize Oracle date-like values to ISO date strings."""

    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    return None


def _format_timestamp(value: Any):
    """Normalize Oracle timestamp-like values to ISO 8601 strings in UTC."""

    if isinstance(value, datetime):
        ts = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        ts = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return candidate
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(str(value).strip())
        except Exception:
            return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        try:
            return float(str(value).strip())
        except Exception:
            return None


def _normalize_weight(value: Any) -> Optional[float]:
    """Normalize TECH100 weight values to percentages rounded to two decimals."""

    num = _coerce_float(value)
    if num is None:
        return None
    normalized = num * 100 if abs(num) <= 1 else num
    try:
        return round(normalized, 2)
    except Exception:
        return normalized
def _coerce_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        text = str(value).strip()
    except Exception:
        return None
    return text or None


def _sanitize_limit(value: Any, default: int = 1000, max_limit: int = 2000) -> int:
    """Clamp the limit parameter to a safe range."""

    limit = _coerce_int(value)
    if limit is None:
        limit = default
    if limit < 1:
        limit = 1
    if limit > max_limit:
        limit = max_limit
    return limit


def _coerce_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        text = str(value).strip()
    except Exception:
        return None
    return text or None


def _format_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%d-%b-%y"):
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except Exception:
                continue
        return candidate
    try:
        return str(value)
    except Exception:
        return None


def _format_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    try:
        return str(value)
    except Exception:
        return None


def _normalize_tech100_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map TECH11_AI_GOV_ETH_INDEX columns to canonical TECH100 response fields."""

    port_date = _format_date(row.get("port_date") or row.get("asof") or row.get("updated_at"))
    weight_candidates = (
        row.get("port_weight"),
        row.get("weight"),
        row.get("portfolio_weight"),
        row.get("index_weight"),
    )
    raw_weight = next((val for val in weight_candidates if val is not None), None)
    weight_value = _coerce_float(raw_weight)
    if weight_value is not None:
        weight_value = weight_value * 100 if abs(weight_value) <= 1 else weight_value
        weight_value = round(weight_value, 2)
    aiges = _coerce_float(
        row.get("aiges_composite_average") or row.get("aiges_composite") or row.get("overall")
    )
    item = {
        "port_date": port_date,
        "rank_index": _coerce_int(row.get("rank_index") or row.get("rank")),
        "company_name": row.get("company_name") or row.get("company") or row.get("name"),
        "ticker": row.get("ticker"),
        "port_weight": weight_value,
        "sector": row.get("gics_sector") or row.get("sector"),
        "gics_sector": row.get("gics_sector") or row.get("sector"),
        "transparency": _coerce_float(row.get("transparency")),
        "ethical_principles": _coerce_float(row.get("ethical_principles") or row.get("ethics")),
        "governance_structure": _coerce_float(
            row.get("governance_structure") or row.get("accountability")
        ),
        "regulatory_alignment": _coerce_float(row.get("regulatory_alignment")),
        "stakeholder_engagement": _coerce_float(
            row.get("stakeholder_engagement") or row.get("stakeholder")
        ),
        "aiges_composite": aiges,
        "summary": _coerce_str(row.get("summary")),
        "source_links": _coerce_str(row.get("source_links")),
    }
    # Backwards-compatible aliases
    item["rank"] = item.get("rank_index")
    item["company"] = item.get("company_name")
    item["overall"] = item.get("aiges_composite")
    item["accountability"] = item.get("governance_structure")
    item["updated_at"] = port_date
    item["weight"] = item.get("port_weight")
    return item


def _api_coerce_k(value, default: int = 4) -> int:
    """Best-effort coercion for /api/ask2 k parameter with safe bounds."""

    try:
        # Preferred path: use the configured sanitizer (router may override).
        return _sanitize_meta_k(value)  # type: ignore[arg-type]
    except TypeError:
        # Some router overrides may not accept kwargs; fall through.
        try:
            return _sanitize_meta_k(value, default)  # type: ignore[arg-type]
        except Exception:
            pass
    except Exception:
        pass

    try:
        k_val = int(value)
    except Exception:
        k_val = default

    if k_val < 1:
        k_val = 1
    if k_val > 10:
        k_val = 10
    return k_val


def _format_date(value: Any) -> Optional[str]:
    """Return a YYYY-MM-DD string for Oracle DATE/TIMESTAMP values."""

    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return value.strftime("%Y-%m-%d")  # type: ignore[call-arg]
    except Exception:
        try:
            text = str(value).strip()
            if text:
                return text.split("T")[0].split(" ")[0]
        except Exception:
            pass
    return None


def _format_timestamp(value: Any) -> Optional[str]:
    """Return an ISO 8601 timestamp string in UTC with Z suffix."""

    dt_value: Optional[datetime] = None
    if isinstance(value, datetime):
        dt_value = value
    elif isinstance(value, date):
        dt_value = datetime.combine(value, datetime.min.time())

    if dt_value is None:
        return None

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    else:
        dt_value = dt_value.astimezone(timezone.utc)
    return dt_value.isoformat().replace("+00:00", "Z")


_ASK2_ENABLE_SMALLTALK = _env_flag("ASK2_ENABLE_SMALLTALK", True)
try:
    _ASK2_MAX_SOURCES = int(os.getenv("ASK2_MAX_SOURCES", "6"))
except ValueError:
    _ASK2_MAX_SOURCES = 6

if _ASK2_MAX_SOURCES < 1:
    _ASK2_MAX_SOURCES = 1

_ASK2_SOURCE_LABEL_MODE = os.getenv("ASK2_SOURCE_LABEL_MODE", "default").strip().lower() or "default"

_LOW_CONFIDENCE_MESSAGE = (
    "I didn’t find enough Sustainacore context for that. Want a general answer (no SC sources) "
    "or search a specific company/document?"
)


def _log_build_identifier() -> None:
    build_id = os.getenv("SUSTAINACORE_BUILD_ID")
    if not build_id:
        try:
            build_id = (
                subprocess.check_output(
                    ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )
        except Exception:
            build_id = "unknown"
    _STARTUP_LOGGER.info("build_id=%s", build_id)


_log_build_identifier()


def _log_disk_space_warning() -> None:
    try:
        repo_root = Path(__file__).resolve().parent
        usage = shutil.disk_usage(repo_root)
    except Exception as exc:  # pragma: no cover - diagnostics only
        _STARTUP_LOGGER.debug("disk_space_probe_failed: %s", exc)
        return

    free_bytes = usage.free
    if free_bytes < 2 * 1024 * 1024 * 1024:
        _STARTUP_LOGGER.warning("disk_free_bytes=%d path=%s", free_bytes, repo_root)
    else:
        _STARTUP_LOGGER.info("disk_free_bytes=%d path=%s", free_bytes, repo_root)


_log_disk_space_warning()

_EMBED_SETTINGS = get_embed_settings()
try:
    run_startup_parity_check(_EMBED_SETTINGS)
except EmbedParityError as exc:  # pragma: no cover - fail-fast path
    raise

_SMALL_TALK_ENABLED = _env_flag("SMALL_TALK", True)
_INLINE_SOURCES_ENABLED = _env_flag("INLINE_SOURCES", False)

_SMALL_TALK_TERMS = {
    "hi",
    "hello",
    "hey",
    "thanks",
    "thank you",
    "help",
    "goodbye",
}
_SMALL_TALK_FOLLOW_UPS = (
    "Check whether a company is in the TECH100.",
    "Ask for the latest TECH100 ranking for a company.",
    "Request an ESG or AI governance summary.",
)


def _is_small_talk_message(text: str) -> bool:
    """Return ``True`` when the user greets or requests generic help."""

    if not _SMALL_TALK_ENABLED:
        return False
    normalized = re.sub(r"[^a-zA-Z\s]", " ", (text or "").strip()).lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized in _SMALL_TALK_TERMS


def _build_small_talk_answer(prefix: Optional[str] = None) -> str:
    opening = (prefix or "Hello! I’m the Sustainacore assistant. How can I help today?").strip()
    lines = [opening, "", "Suggested follow-ups:"]
    for item in _SMALL_TALK_FOLLOW_UPS:
        lines.append(f"- {item}")
    return "\n".join(lines).strip()


def _coerce_small_talk_text(candidate: Optional[object]) -> str:
    if isinstance(candidate, str):
        return candidate.strip()
    if isinstance(candidate, dict):
        for key in ("answer", "content", "message", "text"):
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""

DEFAULT_EMPTY_ANSWER = (
    "I couldn’t find Sustainacore documents for that yet.\n"
    "- Scope: TECH100 companies and ESG/AI governance sources.\n"
    "- Tip: try a company name or ask about TECH100 membership or latest rank."
)


def _ensure_non_empty_answer(value: Optional[str], fallback: Optional[str] = None) -> str:
    """Return a trimmed answer or a safe fallback message."""

    candidate = (value or "").strip()
    if candidate:
        return candidate
    fallback_text = (fallback or DEFAULT_EMPTY_ANSWER).strip()
    return fallback_text if fallback_text else DEFAULT_EMPTY_ANSWER


def _strip_source_sections(text: Optional[str]) -> str:
    if not text:
        return ""

    banned_prefixes = (
        "sources:",
        "why this answer",
        "here's the best supported answer",
        "here’s the best supported answer",
    )
    lines = []
    skip_bullets = False
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        lower = line.lower()
        if any(lower.startswith(prefix) for prefix in banned_prefixes):
            skip_bullets = True
            continue
        if skip_bullets:
            if not line:
                skip_bullets = False
                continue
            if line.startswith("-") or line.startswith("•"):
                continue
            skip_bullets = False
        lines.append(raw_line)

    cleaned = "\n".join(lines).strip()
    if not cleaned:
        return ""

    def _scrub_inline_markers(value: str) -> str:
        patterns = [
            r"(?im)^[\s>*-]*why this answer:?\s*",
            r"(?im)^[\s>*-]*sources:?\s*.*$",
            r"(?im)^[\s>*-]*here['’]s the best supported answer:?\s*",
        ]
        for pattern in patterns:
            value = re.sub(pattern, "", value)
        return re.sub(r"\n{3,}", "\n\n", value)

    cleaned = _scrub_inline_markers(cleaned).strip()
    return cleaned


def _safe_header_payload(raw_value: Optional[str]) -> Dict[str, Any]:
    if not raw_value:
        return {}
    text_value = str(raw_value or "").strip()
    if not text_value:
        return {}

    def _attempt_json(candidate: bytes) -> Optional[Dict[str, Any]]:
        try:
            parsed = json.loads(candidate.decode("utf-8"))
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else {"value": parsed}

    direct = _attempt_json(text_value.encode("utf-8"))
    if direct is not None:
        return direct

    try:
        decoded = base64.b64decode(text_value)
    except Exception:
        return {}

    for candidate in (decoded,):
        parsed = _attempt_json(candidate)
        if parsed is not None:
            return parsed

    try:
        inflated = zlib.decompress(decoded)
        parsed = _attempt_json(inflated)
        if parsed is not None:
            return parsed
    except Exception:
        pass

    return {}


def _collect_request_hints(flask_request) -> Dict[str, Any]:
    hints: Dict[str, Any] = {}
    if not hasattr(flask_request, "headers"):
        return hints

    header_map = {
        "x-ask2-hints": "hints",
        "x-ask2-meta": "meta",
        "x-ask2-routing": "routing",
    }

    for header_name, target_key in header_map.items():
        value = flask_request.headers.get(header_name)
        payload = _safe_header_payload(value)
        if payload:
            hints[target_key] = payload

    return hints


def _extract_top_similarity(contexts: Optional[list]) -> Optional[float]:
    if not contexts:
        return None
    candidate = contexts[0]
    if not isinstance(candidate, dict):
        return None
    if candidate.get("score") is not None:
        try:
            return max(0.0, min(1.0, float(candidate["score"])))
        except (TypeError, ValueError):
            pass
    for key in ("similarity", "confidence"):
        if candidate.get(key) is not None:
            try:
                return max(0.0, min(1.0, float(candidate[key])))
            except (TypeError, ValueError):
                continue
    distance = candidate.get("dist") or candidate.get("distance")
    return compute_similarity(distance)


def _below_similarity_floor(score: Optional[float]) -> bool:
    if SIMILARITY_FLOOR_MODE == "off":
        return False
    if score is None:
        return True
    try:
        return float(score) < float(SIMILARITY_FLOOR)
    except (TypeError, ValueError):
        return True


def _call_route_ask2_facade(
    question: str,
    k_value,
    *,
    client_ip: Optional[str] = None,
    header_hints: Optional[Dict[str, Any]] = None,
):
    """Invoke the smart router with graceful fallbacks.

    Always returns ``(payload, 200)`` with a non-empty answer, list sources, and
    meta containing ``routing``, ``k``, and ``latency_ms``.
    """

    sanitized_k = _sanitize_meta_k(k_value)
    question_text = (question or "").strip()
    started_at = time.perf_counter()

    def _finalize(
        payload: Optional[dict],
        *,
        routing: str,
        answer_override: Optional[str] = None,
        extra_meta: Optional[dict] = None,
        intent_label: str = "qa",
        retrieval_skipped: bool = False,
        include_contexts: bool = True,
    ):
        latency_ms = int((time.perf_counter() - started_at) * 1000)

        data = payload if isinstance(payload, dict) else {}
        answer_value = answer_override
        if answer_value is None:
            answer_candidate = data.get("answer") if isinstance(data, dict) else None
            answer_value = str(answer_candidate) if answer_candidate is not None else ""

        contexts = data.get("contexts") if isinstance(data, dict) else None
        if not isinstance(contexts, list):
            contexts = []

        sources_raw = data.get("sources") if isinstance(data, dict) else None
        if isinstance(sources_raw, list):
            sources_list = sources_raw
        else:
            sources_list = []

        meta_raw = data.get("meta") if isinstance(data, dict) else None
        meta_dict = dict(meta_raw) if isinstance(meta_raw, dict) else {}
        if extra_meta:
            meta_dict.update(extra_meta)
        meta_dict.setdefault("inline_sources_enabled", _INLINE_SOURCES_ENABLED)
        meta_dict.setdefault("small_talk_enabled", _SMALL_TALK_ENABLED)
        meta_dict["routing"] = routing
        meta_dict["k"] = sanitized_k
        meta_dict["latency_ms"] = latency_ms
        if header_hints:
            meta_dict.setdefault("request_hints", header_hints)

        top_similarity = _extract_top_similarity(contexts)
        floor_decision = "ok"
        allow_contexts = include_contexts

        if allow_contexts:
            if contexts:
                if (
                    top_similarity is not None
                    and SIMILARITY_FLOOR_MODE != "off"
                    and top_similarity < SIMILARITY_FLOOR
                ):
                    floor_decision = "below_floor"
                    meta_dict.setdefault("floor_warning", True)
                else:
                    floor_decision = "ok"
            else:
                if SIMILARITY_FLOOR_MODE == "enforce":
                    floor_decision = "below_floor"
                    meta_dict.setdefault("floor_warning", True)

        normalized_answer = _ensure_non_empty_answer(
            answer_value, fallback=DEFAULT_EMPTY_ANSWER
        )
        normalized_answer = _strip_source_sections(normalized_answer)

        shaped = {
            "answer": normalized_answer,
            "sources": sources_list,
            "meta": meta_dict,
        }

        if allow_contexts and intent_label == "qa":
            shaped["contexts"] = contexts

        _LOGGER.info(
            "ask2_response intent=%s routing=%s top1=%s retrieval_skipped=%s floor_decision=%s",
            intent_label,
            routing,
            "{:.3f}".format(top_similarity) if isinstance(top_similarity, float) else "None",
            retrieval_skipped,
            floor_decision,
        )

        return shaped, 200

    if _is_small_talk_message(question_text):
        try:
            smalltalk_answer = smalltalk_response(question_text)
        except Exception:  # pragma: no cover - defensive fallback
            smalltalk_answer = None
        base_text = _coerce_small_talk_text(smalltalk_answer)
        answer_text = _build_small_talk_answer(base_text or None)
        payload = {"answer": answer_text, "sources": []}
        extra_meta = {"intent": "SMALL_TALK"}
        return _finalize(
            payload,
            routing="smalltalk",
            extra_meta=extra_meta,
            intent_label="small_talk",
            retrieval_skipped=True,
            include_contexts=False,
        )

    if (
        _gemini_run_pipeline is not None
        and _gemini_settings is not None
        and _gemini_settings.gemini_first_enabled
    ):
        try:
            payload = _gemini_run_pipeline(
                question_text, k=sanitized_k, client_ip=client_ip or "unknown"
            )
            if isinstance(payload, dict):
                payload.setdefault("meta", {})
            return _finalize(payload, routing="gemini_first")
        except _GeminiRateLimitError as exc:  # type: ignore[arg-type]
            message = "You’ve hit the current rate limit. Please retry in a few seconds."
            extra_meta = {
                "error": getattr(exc, "detail", "rate_limited"),
                "intent": "RATE_LIMIT",
            }
            return _finalize(
                {"sources": [], "contexts": []},
                routing="gemini_first",
                answer_override=message,
                extra_meta=extra_meta,
            )
        except _GeminiUnavailableError:  # type: ignore[arg-type]
            pass  # fall back to legacy router
        except Exception as exc:  # pragma: no cover - defensive logging
            _LOGGER.exception("Gemini-first pipeline failed; using legacy router", exc_info=exc)

    if callable(_route_ask2):
        try:
            shaped = _route_ask2(question_text, sanitized_k)
            if isinstance(shaped, dict):
                return _finalize(shaped, routing="legacy_router")
        except ValueError as exc:
            _LOGGER.warning("Legacy /ask2 router invalid hint; using fallback", exc_info=exc)
        except Exception as exc:  # pragma: no cover - defensive logging
            _LOGGER.exception("Legacy /ask2 router failed", exc_info=exc)

    fallback = {
        "answer": _ASK2_ROUTER_FALLBACK,
        "sources": [],
        "meta": {"error": "router_unavailable"},
        "contexts": [],
    }
    return _finalize(fallback, routing="router_unavailable")

EMBED_DIM = _EMBED_SETTINGS.expected_dimension
EMBED_MODEL_NAME = _EMBED_SETTINGS.model_name

try:
    _legacy_topk = int(os.getenv("FUSION_TOPK_BASE", str(RETRIEVAL_TOP_K)))
except ValueError:
    _legacy_topk = RETRIEVAL_TOP_K
FUSION_TOPK_BASE = max(RETRIEVAL_TOP_K, _legacy_topk)
FUSION_TOPK_MAX  = int(os.getenv("FUSION_TOPK_MAX", str(max(FUSION_TOPK_BASE * 3, 24))))
RRF_K            = int(os.getenv("RRF_K", "60"))
MMR_LAMBDA       = float(os.getenv("MMR_LAMBDA", "0.7"))
DOC_CAP          = int(os.getenv("DOC_CAP", "3"))
CHUNKS_MAX       = int(os.getenv("CHUNKS_MAX", "12"))
CITES_MAX        = int(os.getenv("CITES_MAX", "6"))
LATENCY_BUDGET_MS= int(os.getenv("LATENCY_BUDGET_MS", "1200"))
RETURN_TOP_AS_ANSWER = os.getenv("RETURN_TOP_AS_ANSWER","1") == "1"

app = Flask(__name__)


def embed(text: str):
    return embed_text(text, settings=_EMBED_SETTINGS)

META_KEYS = ("ROLE:", "TASK:", "PREVIOUS ANSWER", "QUESTION TYPE", "CONTEXT TITLES:", "buttons to click", "What would you like to explore?")
SOURCE_TAG_RE = re.compile(r"\[(?:S|s)\d+\]")

def normalize_question(q: str):
    q0 = (q or "").strip()
    changed = False
    if any(k in q0 for k in META_KEYS):
        changed = True
        m = re.findall(r'([A-Z][^?]{3,}\?)', q0, flags=re.S)
        if m:
            q1 = m[-1].strip()
        else:
            q1 = re.sub(r'(?mi)^(ROLE|TASK|PREVIOUS ANSWER|QUESTION TYPE|CONTEXT TITLES).*$','',q0)
            q1 = re.sub(r'buttons to click.*','',q1, flags=re.I)
            q1 = re.sub(r'\s+',' ', q1).strip()
        q0 = q1 or "help"
    return q0, changed

def detect_intent(q: str, ents):
    return scope_detect_intent(q, ents)


def extract_entities(q: str):
    return scope_extract_entities(q)[:5]

def make_variants(q: str, ents):
    vs=[q.strip()]
    qn=re.sub(r'\btech[-\s]?100\b','TECH100',q,flags=re.I)
    if qn!=q: vs.append(qn)
    vs.append(re.sub(r'[^\w\s]',' ',qn))
    seen=set(); out=[]
    for s in vs:
        s=s.strip()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out[:5]

def rrf_fuse(variant_results, k=RRF_K):
    from collections import defaultdict
    scores=defaultdict(float); bykey={}
    for res in variant_results:
        for rank,item in enumerate(res,1):
            key=(item.get("doc_id"),item.get("chunk_ix"))
            bykey[key]=item; scores[key]+=1.0/(k+rank)
    ranked=sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    return [bykey[k] for k,_ in ranked]

def _tok(s): 
    import re
    return set(re.findall(r'[A-Za-z0-9]{2,}', (s or "").lower()))

def mmr_select(cands, max_k=CHUNKS_MAX, lambda_=MMR_LAMBDA, per_doc=DOC_CAP):
    sel=[]; used=defaultdict(int)
    cand_tokens=[(_tok(c.get("chunk_text","")),i) for i,c in enumerate(cands)]
    while len(sel)<max_k and cands:
        best=None;best_score=-1;best_idx=None
        for idx,c in enumerate(cands):
            if used[c.get("doc_id")]>=per_doc: continue
            rel=1.0/(1.0+float(c.get("dist",0.0) or 0.0))
            if not sel: score=rel
            else:
                t_c=cand_tokens[idx][0]
                sim=max((len(t_c & _tok(s.get("chunk_text","")))/max(1,len(t_c|_tok(s.get("chunk_text","")))) for s in sel), default=0.0)
                score=lambda_*rel-(1.0-lambda_)*sim
            if score>best_score: best_score=score; best=c; best_idx=idx
        if best is None: break
        sel.append(best); used[best.get("doc_id")]+=1
        cands.pop(best_idx); cand_tokens.pop(best_idx)
    return sel

def retrieve(q: str, *, explicit_filters: Optional[Dict[str, str]] = None):
    ents = extract_entities(q)
    scope = infer_scope(q, explicit_filters=explicit_filters or {}, entities=ents)
    variants = make_variants(q, ents)
    topk = FUSION_TOPK_BASE
    per_variant = []
    filters = scope.applied_filters or {}

    for v in variants:
        vec = embed(v)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, topk), filters=filters)
        per_variant.append(rows)

    fused = rrf_fuse(per_variant, k=RRF_K)
    if len(fused) < CHUNKS_MAX // 2 and topk < FUSION_TOPK_MAX:
        topk = min(FUSION_TOPK_MAX, topk * 2)
        per_variant = []
        for v in variants:
            vec = embed(v)
            if _top_k_by_vector is None:
                rows = []
            else:
                rows = _top_k_by_vector(vec, max(1, topk), filters=filters)
            per_variant.append(rows)
        fused = rrf_fuse(per_variant, k=RRF_K)

    fused = mmr_select(fused[: max(32, FUSION_TOPK_MAX)], max_k=CHUNKS_MAX, lambda_=MMR_LAMBDA, per_doc=DOC_CAP)
    fused = dedupe_contexts(fused)

    scope_payload = {
        "label": scope.label,
        "source_types": list(scope.source_types),
        "company": scope.company,
        "filters": {k: list(v) for k, v in (scope.applied_filters or {}).items()},
    }

    return {"entities": ents, "variants": variants, "chunks": fused, "k": topk, "scope": scope_payload}

def extract_quotes(chunks, limit_words=60):
    quotes=[]; total=0
    for i,c in enumerate(chunks,1):
        txt=(c.get("chunk_text") or "").strip()
        if not txt: continue
        parts=re.split(r'(?<=[.!?])\s+', txt)
        for p in parts[:2]:
            w=len(p.split())
            if w==0: continue
            if total + w > limit_words: break
            quotes.append((i, p.strip())); total+=w
        if total>=limit_words: break
    return quotes

def find_first(chunks, key):
    for c in chunks:
        t=(c.get("title") or "").lower()
        if key in t: return c
    return None

def parse_rank(chunk):
    if not chunk: return None
    t=(chunk.get("chunk_text") or "") + " " + (chunk.get("title") or "")
    m=re.search(r'\brank\s*(?:is\s*)?(?:#\s*)?(\d{1,3})\b', t, flags=re.I)
    if m: return m.group(1)
    if re.search(r'\branks?\s*first\b', t, flags=re.I): return "1"
    if re.search(r'\branks?\s*second\b', t, flags=re.I): return "2"
    if re.search(r'\branks?\s*third\b', t, flags=re.I): return "3"
    return None

def parse_asof(chunk):
    if not chunk: return None
    txt=(chunk.get("chunk_text") or "")
    m=re.search(r'(20\d{2}[-/]\d{2}|20\d{2}-\d{2}-\d{2}|[A-Za-z]{3,9}\s+20\d{2})', txt)
    return m.group(1) if m else None

def _collect_sources(chunks, maxn=CITES_MAX):
    seen=set(); collected=[]
    for c in chunks:
        title=(c.get("title") or "").strip()
        url=(c.get("source_url") or "").strip()
        if not title and not url:
            continue
        key=(title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        collected.append({
            "title": title or (url or "Source"),
            "url": url,
        })
        if len(collected) >= maxn:
            break
    return collected


def _build_default_sources(chunks):
    if not chunks:
        return []
    labels = []
    seen = set()
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        title = (chunk.get('title') or '').strip()
        url = (chunk.get('source_url') or '').strip()
        label = title or url
        if not label:
            continue
        key = (label.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        if url and url not in label:
            labels.append(f"{label} - {url}")
        else:
            labels.append(label)
        if len(labels) >= _ASK2_MAX_SOURCES:
            break
    return labels

def sources_block(chunks, maxn=CITES_MAX):
    return [c["title"] for c in _collect_sources(chunks, maxn)]

def sources_detailed(chunks, maxn=CITES_MAX):
    return _collect_sources(chunks, maxn)

def _strip_source_refs(text: str) -> str:
    cleaned = SOURCE_TAG_RE.sub("", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def _chunk_snippets_for_llm(chunks, limit=5, max_chars=360):
    prepared=[]
    for idx, c in enumerate(chunks):
        raw=(c.get("chunk_text") or "").strip()
        if not raw:
            continue
        snippet=re.sub(r"\s+", " ", raw)
        if max_chars and len(snippet) > max_chars:
            snippet=snippet[:max_chars].rsplit(" ", 1)[0].strip()
        title=(c.get("title") or "").strip()
        label=title if title else f"Snippet {idx+1}"
        prepared.append((label, snippet))
        if len(prepared) >= limit:
            break
    return prepared

def _tailor_with_gemini(question, intent, baseline, chunks, sources):
    if not baseline or not _gemini_generate:
        return baseline
    snippets=_chunk_snippets_for_llm(chunks)
    if not snippets:
        return baseline
    context_blob="\n".join(f"- {label}: {text}" for label, text in snippets)
    context_payload=[{"title": label, "snippet": text} for label, text in snippets]
    source_summary=", ".join(s.get("title", "") for s in sources[:3] if s.get("title")) if sources else ""
    prompt_parts = [
        "You are the SustainaCore Assistant. Rewrite the baseline answer so it stays factual but sounds conversational and helpful.\n",
        f"Question: {question.strip()}\n",
        f"Intent: {intent}\n",
        "Baseline answer (preserve facts and direct conclusions):\n",
        _strip_source_refs(baseline) + "\n\n",
        "Evidence excerpts:\n",
        context_blob + "\n\n",
    ]
    if source_summary:
        prompt_parts.append(f"Key sources: {source_summary}\n")
    prompt_parts.extend([
        "Guidelines:\n",
        "- Start with a sentence that answers the question directly.\n",
        "- Refer to the evidence naturally; do not use bracketed citations like [S1].\n",
        "- Keep the tone professional, friendly, and under 130 words.\n",
        "- Admit when information is missing.\n",
        "- Do not fabricate content beyond the excerpts.\n",
        "- End before the Sources section; the system will append sources.",
    ])
    prompt="".join(prompt_parts)
    try:
        candidate=_gemini_generate(prompt, context=context_payload) or ""
    except Exception:
        return baseline
    candidate=candidate.strip()
    if not candidate:
        return baseline
    return candidate

def compose_overview(entity, chunks, quotes):
    mem=find_first(chunks,"membership")
    rank=find_first(chunks,"rank")
    rnk=parse_rank(rank)
    asof=parse_asof(mem) or parse_asof(rank)
    lines=[]
    if mem: lines.append(f"TECH100 membership: Yes{f' (as of {asof})' if asof else ''}.")
    else:   lines.append("TECH100 membership: Not found in retrieved membership list.")
    if rnk: lines.append(f"Latest rank: {rnk}.")
    src=sources_detailed(chunks)
    heading=f"{entity}: overview from SustainaCore’s knowledge base"
    details="\n".join("- "+w for w in lines)
    body="\n".join(filter(None, [heading, details])).strip()
    return body, src

def compose_answer_baseline(intent, q, entities, chunks, quotes):
    if intent=="about":
        text=("SustainaCore Assistant helps you explore the TECH100 AI Governance & Ethics Index and related ESG/AI sources.\n"
              "- Ask about a company’s TECH100 membership or latest rank.\n"
              "- Request a quick AI & ESG snapshot for any TECH100 company.\n"
              "- Data comes from Oracle Autonomous DB + Vector Search.\n")
        return text, "about", []
    if intent=="overview" and entities:
        overview_text, overview_sources = compose_overview(entities[0], chunks, quotes)
        return overview_text, "overview", overview_sources
    if intent in ("membership","rank") and entities:
        mem=find_first(chunks,"membership") if intent=="membership" else None
        rank=find_first(chunks,"rank")
        rnk=parse_rank(rank)
        asof=parse_asof(mem) or parse_asof(rank)
        lines=[]
        if intent=="membership":
            if mem: lines.append(f"Yes — {entities[0]} is in TECH100{f' (as of {asof})' if asof else ''}.")
            else:   lines.append(f"No evidence of {entities[0]} in TECH100 from retrieved membership context.")
        if intent=="rank":
            if rnk: lines.append(f"{entities[0]} latest TECH100 rank: {rnk}.")
            else:   lines.append("No clear rank found in retrieved context.")
        body="\n".join(lines).strip()
        return body, intent, sources_detailed(chunks)
    head="Here’s the best supported answer from the retrieved sources."
    body=head
    if chunks:
        bullets=[]
        for chunk in chunks[:4]:
            snippet=(chunk.get("chunk_text") or "").strip()
            if not snippet:
                continue
            sentence=snippet.split("\n")[0].strip()
            if not sentence:
                continue
            if len(sentence) > 220:
                sentence=sentence[:217].rsplit(" ",1)[0].strip()+"…"
            bullets.append(f"- {sentence}")
        if bullets:
            body=head+"\n"+"\n".join(bullets)
    return body, "general", sources_detailed(chunks)

def compose_answer(intent, q, entities, chunks, quotes):
    baseline, shape, sources = compose_answer_baseline(intent, q, entities, chunks, quotes)
    tailored = _tailor_with_gemini(q, intent, baseline, chunks, sources) if intent != "about" else baseline
    answer = tailored or baseline
    return answer, shape, sources
class NormalizeMiddleware:
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        # No-op (Ask2 orchestrator removed)
        return self.app(environ, start_response)

class OrchestrateMiddleware:
    def __init__(self, app): self.app=app
    def __call__(self, environ, start_response):
        path=environ.get("PATH_INFO",""); method=(environ.get("REQUEST_METHOD") or "GET").upper()
        if not (path=="/ask" and method=="POST"):
            return self.app(environ, start_response)

        import io as _io
        try:
            size=int(environ.get("CONTENT_LENGTH") or "0")
            # Ask2 orchestrator disabled
            pass
        except Exception:
            size=0
            pass
        body=environ["wsgi.input"].read(size) if size>0 else b""
        try:
            req=json.loads(body.decode("utf-8") or "{}")
            # Ask2 orchestrator disabled
            pass
        except Exception:
            req={}
            pass
        q=(req.get("question") or req.get("q") or "").strip()
        if not q:
            return self.app(environ, start_response)

        t0=time.time()
        retrieval=retrieve(q)
        entities=retrieval["entities"]
        chunks=retrieval["chunks"]
        quotes=extract_quotes(chunks, limit_words=60)
        intent=detect_intent(q, entities)

        def _ctx_has_entity(ctx, normalized_ent):
            if not normalized_ent:
                return False
            title = ctx.get("title") or ""
            text = (
                ctx.get("chunk_text")
                or ctx.get("text")
                or ctx.get("content")
                or ctx.get("summary")
                or ""
            )
            return _fuzzy_contains(title, normalized_ent) or _fuzzy_contains(text, normalized_ent)

        normalized_entity = ""
        if entities:
            normalized_entity = _normalize_company_name(
                _canonicalize_question_entity(entities[0])
            )

        membership_hit = False
        if chunks and normalized_entity:
            for ctx in chunks:
                title = ctx.get("title") or ""
                if "Membership › TECH100" in title and _fuzzy_contains(title, normalized_entity):
                    membership_hit = True
                    break

        chunks_ok = bool(chunks)
        if normalized_entity:
            chunks_ok = any(_ctx_has_entity(ctx, normalized_entity) for ctx in chunks)
            if not chunks_ok and membership_hit:
                chunks_ok = True

        if (not chunks) or not chunks_ok:
            ans=("I couldn’t find that in SustainaCore’s knowledge base.\n"
                 "- Scope: TECH100 companies and ESG/AI governance sources.\n"
                 "- Tip: try a company name or ask about TECH100 membership or latest rank.")
            payload={"answer": ans, "contexts": chunks, "mode":"simple", "sources": []}
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
            hdrs=[("Content-Type","application/json"),
                  ("X-Intent", intent), ("X-K", str(retrieval["k"])),
                  ("X-RRF","on"), ("X-MMR", str(MMR_LAMBDA)),
                  ("X-Loops","1"), ("X-BudgetMs", str(int((time.time()-t0)*1000))),
                  ("X-Answer-Shape","not-found"), ("Content-Length", str(len(data)))]
            start_response("200 OK", hdrs)
            return [data]

        answer, shape, sources_out = compose_answer(intent, q, entities, chunks, quotes)
        if answer:
            cleaned_answer, stripped_flag = _strip_scaffold(answer)
            if stripped_flag and cleaned_answer:
                answer = cleaned_answer
        payload={"answer": answer, "contexts": chunks, "mode":"simple", "sources": sources_out}
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
        hdrs=[("Content-Type","application/json"),
              ("X-Intent", intent), ("X-K", str(retrieval["k"])),
              ("X-RRF","on"), ("X-MMR", str(MMR_LAMBDA)),
              ("X-Loops","1"), ("X-BudgetMs", str(int((time.time()-t0)*1000))),
              ("X-Answer-Shape", shape), ("Content-Length", str(len(data)))]
        start_response("200 OK", hdrs)
        return [data]

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": time.time()})


@app.route("/readyz")
def readyz():
    if _top_k_by_vector is None:
        message = "vector_search_unavailable"
        _READINESS_LOGGER.error("Readiness probe failed: %s", message)
        return jsonify({"ok": False, "error": message}), 503

    try:
        vec = embed("sustainacore readiness probe")
        rows = _top_k_by_vector(vec, max(1, RETRIEVAL_TOP_K))
    except Exception as exc:  # pragma: no cover - defensive logging
        _READINESS_LOGGER.error("Readiness probe failed", exc_info=exc)
        return jsonify({"ok": False, "error": str(exc)}), 503

    _READINESS_LOGGER.info(
        "Readiness probe succeeded",
        extra={"rows": len(rows), "scoping": RETRIEVAL_SCOPING_ENABLED},
    )
    return jsonify({"ok": True, "rows": len(rows)}), 200


@app.route("/api/health", methods=["GET"])
def api_health():
    auth = _api_auth_or_unauthorized()
    if auth is not None:
        return auth

    oracle_status = "unknown"
    model_status = "unknown"

    try:
        from db_helper import get_connection  # local import to avoid startup failures

        with get_connection() as conn:
            try:
                ping = getattr(conn, "ping", None)
                if callable(ping):
                    ping()
            except Exception:
                pass
        oracle_status = "ok"
    except Exception as exc:
        oracle_status = "error"
        _LOGGER.debug("api_health oracle check failed", exc_info=exc)

    try:
        embed("api health probe")
        model_status = "ok"
    except Exception as exc:
        model_status = "error"
        _LOGGER.debug("api_health model check failed", exc_info=exc)

    status_value = "ok" if oracle_status != "error" and model_status != "error" else "degraded"
    payload = {
        "status": status_value,
        "oracle": oracle_status,
        "model": model_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return jsonify(payload), 200


@app.route("/api/tech100", methods=["GET"])
def api_tech100():
    auth = _api_auth_or_unauthorized()
    if auth is not None:
        return auth

    try:
        from db_helper import _to_plain, get_connection  # type: ignore
    except Exception as exc:
        _LOGGER.exception("api_tech100 helper import failed", exc_info=exc)
        return (
            jsonify(
                {"error": "backend_failure", "message": "Unable to load TECH100 data."}
            ),
            500,
        )

    port_date_filter = (request.args.get("port_date") or request.args.get("as_of") or "").strip()
    sector_filter = (request.args.get("sector") or request.args.get("gics_sector") or "").strip()
    ticker_filter = (request.args.get("ticker") or "").strip()
    search_filter = (
        request.args.get("company")
        or request.args.get("search")
        or request.args.get("q")
        or ""
    ).strip()
    limit = _sanitize_limit(request.args.get("limit"))

    _API_LOGGER.info(
        "api_tech100 request",
        extra={
            "port_date": port_date_filter or None,
            "sector": sector_filter or None,
            "ticker": ticker_filter or None,
            "search": search_filter or None,
            "limit": limit,
        },
    )

    where_clauses: List[str] = []
    binds: Dict[str, Any] = {}

    # Map TECH11_AI_GOV_ETH_INDEX columns to canonical TECH100 fields without forcing "latest" only.
    if port_date_filter:
        try:
            binds["port_date"] = datetime.fromisoformat(port_date_filter).date()
        except Exception:
            binds["port_date"] = port_date_filter
        where_clauses.append("TRUNC(port_date) = TRUNC(:port_date)")
    if sector_filter:
        binds["sector"] = sector_filter
        where_clauses.append("LOWER(gics_sector) = LOWER(:sector)")
    if ticker_filter:
        binds["ticker_exact"] = ticker_filter
        where_clauses.append("LOWER(ticker) = LOWER(:ticker_exact)")
    if search_filter:
        binds["search_like"] = f"%{search_filter}%"
        where_clauses.append(
            "(LOWER(company_name) LIKE LOWER(:search_like) OR LOWER(ticker) LIKE LOWER(:search_like))"
        )

    sql = (
        "SELECT port_date, rank_index, company_name, ticker, port_weight, gics_sector, "
        "transparency, ethical_principles, governance_structure, regulatory_alignment, "
        "stakeholder_engagement, aiges_composite_average, summary, source_links "
        "FROM tech11_ai_gov_eth_index"
    )
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY port_date DESC, rank_index NULLS LAST FETCH FIRST :limit ROWS ONLY"
    binds["limit"] = limit

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, binds)
            columns = [desc[0].lower() for desc in cur.description]
            raw_rows = [
                {col: _to_plain(val) for col, val in zip(columns, raw)}
                for raw in cur.fetchall()
            ]
    except Exception as exc:
        _LOGGER.exception("api_tech100 query failed", exc_info=exc)
        return jsonify({"error": "backend_failure", "message": "Unable to load TECH100 data."}), 500

    items = [_normalize_tech100_row(row) for row in raw_rows]
    if items:
        sample = items[0]
        pillar_keys = [
            key
            for key in (
                "transparency",
                "ethical_principles",
                "governance_structure",
                "regulatory_alignment",
                "stakeholder_engagement",
                "summary",
            )
            if sample.get(key) not in (None, "", [])
        ]
        _API_LOGGER.info(
            "api_tech100 sample",
            extra={
                "company": sample.get("company_name"),
                "ticker": sample.get("ticker"),
                "port_date": sample.get("port_date"),
                "aiges_composite": sample.get("aiges_composite"),
                "pillars": pillar_keys,
            },
        )
    distinct_dates = sorted({item.get("port_date") for item in items if item.get("port_date")})
    _API_LOGGER.info(
        "/api/tech100 rows=%d dates=%s",
        len(items),
        ", ".join(distinct_dates[:5]),
    )
    _API_LOGGER.info("/api/tech100 rows=%d dates=%s", len(items), ", ".join(distinct_dates[:5]))

    return jsonify({"items": items, "count": len(items)}), 200


@app.route("/api/tech100/rebalance_dates", methods=["GET"])
def api_tech100_rebalance_dates():
    # Allow anonymous access so the website can populate the dropdown without a token.
    auth = _api_auth_optional()
    if auth is not None:
        return auth

    try:
        from db_helper import _to_plain, get_connection  # type: ignore
    except Exception as exc:
        _LOGGER.exception("api_tech100_rebalance_dates helper import failed", exc_info=exc)
        return (
            jsonify({"error": "backend_failure", "message": "Unable to load TECH100 dates."}),
            500,
        )

    limit = _sanitize_limit(request.args.get("limit"), default=200)

    sql = (
        "SELECT port_date FROM ("
        "  SELECT DISTINCT TRUNC(port_date) AS port_date"
        "  FROM tech11_ai_gov_eth_index"
        "  WHERE port_date IS NOT NULL"
        "  ORDER BY port_date DESC"
        ") WHERE ROWNUM <= :limit"
    )

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    except Exception as exc:
        _LOGGER.exception("api_tech100_rebalance_dates query failed", exc_info=exc)
        return (
            jsonify({"error": "backend_failure", "message": "Unable to load TECH100 dates."}),
            500,
        )

    items: List[str] = []
    for raw in rows:
        formatted = _format_date(_to_plain(raw[0]))
        if formatted:
            items.append(formatted)

    _API_LOGGER.info("/api/tech100/rebalance_dates count=%d", len(items))

    return jsonify({"items": items}), 200


@app.route("/api/news", methods=["GET"])
def api_news():
    auth = _api_auth_optional()
    if auth is not None:
        return auth

    try:
        from app.news_service import fetch_news_items  # type: ignore
    except Exception as exc:
        _LOGGER.exception("api_news helper import failed", exc_info=exc)
        return jsonify({"error": "backend_failure", "message": "Unable to load news data."}), 500

    args = request.args or {}
    raw_limit = args.get("limit")
    try:
        limit = int(raw_limit) if raw_limit is not None else None
    except (TypeError, ValueError):
        limit = None

    raw_days = args.get("days")
    try:
        days = int(raw_days) if raw_days is not None else None
    except (TypeError, ValueError):
        days = None

    source = args.get("source") or None
    tag_value = args.get("tag")
    tags = [tag_value] if tag_value else []
    ticker = args.get("ticker") or None

    try:
        items, has_more, effective_limit = fetch_news_items(
            limit=limit,
            days=days,
            source=source,
            tags=tags,
            ticker=ticker,
        )
    except Exception as exc:
        _LOGGER.exception("api_news query failed", exc_info=exc)
        return jsonify({"error": "backend_failure", "message": "Unable to load news data."}), 500

    payload = {
        "items": items,
        "meta": {
            "count": len(items),
            "limit": effective_limit,
            "has_more": has_more,
        },
    }

    return jsonify(payload), 200


@app.route("/api/news/<news_id>", methods=["GET"])
def api_news_detail(news_id: str):
    auth = _api_auth_optional()
    if auth is not None:
        return auth

    try:
        item = fetch_news_item_detail(news_id)
    except Exception as exc:
        _LOGGER.exception("api_news_detail query failed", exc_info=exc)
        return jsonify({"error": "backend_failure", "message": "Unable to load news data."}), 500

    if not item:
        return jsonify({"error": "not_found", "message": "News item not found."}), 404

    return jsonify({"item": item}), 200
    auth = _api_auth_optional_or_unauthorized()
    if auth is not None:
        return auth

    limit = request.args.get("limit", type=int)
    days = request.args.get("days", type=int)
    source = request.args.get("source")
    ticker = request.args.get("ticker")
    tag_values = request.args.getlist("tag") or request.args.get("tag")

    try:
        items, has_more, effective_limit = fetch_news_items(
            limit=limit, days=days, source=source, tags=tag_values, ticker=ticker
        )
    except Exception as exc:
        _LOGGER.exception("api_news query failed", exc_info=exc)
        return (
            jsonify(
                {
                    "error": "news_unavailable",
                    "message": "News is temporarily unavailable.",
                }
            ),
            500,
        )

    return (
        jsonify(
            {
                "items": items,
                "meta": {
                    "count": len(items),
                    "limit": effective_limit,
                    "has_more": has_more,
                },
            }
        ),
        200,
    )


@app.route("/api/news/admin/items", methods=["POST"])
def api_news_admin_items():
    auth = _api_auth_or_unauthorized()
    if auth is not None:
        return auth

    try:
        payload = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "bad_request", "message": "Invalid JSON payload."}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "bad_request", "message": "Payload must be an object."}), 400

    try:
        item = create_curated_news_item(payload)
    except ValueError as exc:
        return jsonify({"error": "bad_request", "message": str(exc)}), 400
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("api_news_admin_items failed", exc_info=exc)
        return jsonify({"error": "news_unavailable", "message": "Unable to create news item."}), 500

    return jsonify({"item": item}), 201


@app.route("/ask", methods=["POST"])
def ask():
    try:
        body = request.get_json(force=True) or {}
        q = (body.get("question") or body.get("q") or "").strip()
        if not q: return jsonify({"error":"question is required"}), 400
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE))
        ans = rows[0]["chunk_text"] if rows else "No context found."
        if RETURN_TOP_AS_ANSWER:
            return jsonify({"answer": ans, "contexts": rows, "mode":"simple"})
        else:
            return jsonify({"answer": "No generator configured.", "contexts": rows, "mode":"simple"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Install middlewares
app.wsgi_app = NormalizeMiddleware(app.wsgi_app)
app.wsgi_app = OrchestrateMiddleware(app.wsgi_app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)


# --- Multi-Hit Orchestrator (RRF+MMR, in-process) ---
import io as _io, json as _json, re as _re, time as _time
from collections import defaultdict as _dd

def _norm_q(q: str) -> str:
    q = (q or "").strip()
    # strip UI prompt/buttons
    q = _re.sub(r'(?is)what would you like to explore\?.*$', '', q).strip()
    # strip scaffold spillovers
    q = _re.sub(r'(?m)^(ROLE|TASK|PREVIOUS ANSWER|QUESTION TYPE).*$', '', q).strip()
    # collapse whitespace
    q = _re.sub(r'\s+', ' ', q)
    return q[:400]

def _intent(q: str) -> str:
    s = q.lower()
    if any(w in s for w in ('member','constituent','included','in the tech100','in tech 100')): return 'membership'
    if 'rank' in s: return 'rank'
    if any(w in s for w in ('compare','versus','vs ')): return 'compare'
    if any(w in s for w in ('what is','define','definition')): return 'definition'
    return 'general'

def _variants(q: str):
    s = q.strip()
    v = [s]
    # light paraphrases (cheap)
    v.append(_re.sub(r'\bmember(ship)?\b', 'constituent', s, flags=_re.I))
    v.append(_re.sub(r'\btech ?100\b', 'TECH100', s, flags=_re.I))
    v = [x for i,x in enumerate(v) if x and x not in v[:i]]
    return v[:4]

def _call_downstream_wsgipost(app, body: bytes, extra_headers=None):
    # Build a fresh WSGI environ and call the next app in stack directly (no network)
    env = {
        'REQUEST_METHOD':'POST',
        'PATH_INFO':'/ask',
        'SERVER_NAME':'localhost','SERVER_PORT':'8080','SERVER_PROTOCOL':'HTTP/1.1',
        'wsgi.version':(1,0),'wsgi.url_scheme':'http','wsgi.input':_io.BytesIO(body),
        'CONTENT_TYPE':'application/json','CONTENT_LENGTH':str(len(body)),
    }
    if extra_headers:
        for k,v in extra_headers.items():
            env['HTTP_'+k.upper().replace('-','_')] = v
    status_headers = {}
    def _start_response(status, headers, exc_info=None):
        status_headers['status']=status; status_headers['headers']=headers; return lambda x: None
    chunks = []
    for chunk in app(env, _start_response):
        chunks.append(chunk)
    raw = b''.join(chunks)
    try:
        data = _json.loads(raw.decode('utf-8','ignore'))
    except Exception:
        data = {'raw': raw[:4096].decode('utf-8','ignore')}
    headers = list(status_headers.get('headers', []) or [])
    return status_headers.get('status', '200 OK'), headers, data

def _rrf(fused_lists):
    # fused_lists: [ [ctx, ctx, ...], [ctx...], ... ]
    scores = _dd(float)
    keyf  = lambda c: (c.get('doc_id'), c.get('chunk_ix'))
    for lst in fused_lists:
        for rank, ctx in enumerate(lst, start=1):
            scores[keyf(ctx)] += 1.0/(60.0 + rank)  # RRF with k=60 (stable)
    # unique by (doc_id, chunk_ix)
    seen=set(); fused=[]
    for lst in fused_lists:
        for ctx in lst:
            k=keyf(ctx)
            if k in seen: continue
            seen.add(k); ctx = dict(ctx); ctx['_rrf']=scores[k]; fused.append(ctx)
    fused.sort(key=lambda c: c.get('_rrf',0.0), reverse=True)
    return fused

def _mmr_select(candidates, max_n=12, lam=0.7):
    # diversity by title/doc_id; use rrf score as relevance; jaccard on titles for diversity
    def toks(t): 
        return set(_re.findall(r'[a-z0-9]+', (t or '').lower()))
    selected=[]; selected_toks=[]
    pool = list(candidates)
    while pool and len(selected)<max_n:
        best=None; best_score=-1
        for c in pool:
            rel = c.get('_rrf',0.0)
            ct  = toks(c.get('title') or '')
            if not selected:
                score = rel
            else:
                sim = max((len(ct & st)/(len(ct|st) or 1) for st in selected_toks), default=0.0)
                score = lam*rel - (1-lam)*sim
            if score>best_score: best_score=score; best=c
        selected.append(best); selected_toks.append(toks(best.get('title') or ''))
        pool = [c for c in pool if c is not best]
    return selected

def _compose(q, intent, picks):
    # cheap, deterministic scaffold + tiny quote-then-summarize from the chosen chunks
    def cite(i): return f"[S{i+1}]"
    bullets=[]
    for i,c in enumerate(picks[:4]):
        txt=(c.get('chunk_text') or '').strip()
        # short slice (<=120 chars)
        snippet = _re.sub(r'\s+',' ', txt)[:120].rstrip(' ,.;:')
        if snippet: bullets.append(f"{cite(i)} {snippet}")
    sources=[]
    for i,c in enumerate(picks):
        t = (c.get('title') or '').strip()
        su = (c.get('source_url') or '').strip()
        if t or su: sources.append(f"{cite(i)} {t or su}")
    head=""
    s = q.lower()
    if intent=='membership':
        found = any('membership' in (c.get('title') or '').lower() or 'index' in (c.get('title') or '').lower() for c in picks)
        head = ("Yes — appears in the TECH100 AI Governance & Ethics Index." if found else
                "Not found in the retrieved TECH100 membership set.")
    elif intent=='rank':
        head = "Latest TECH100 rank: see sources below."
    elif intent=='definition':
        head = "Here’s the concise definition from SustainaCore’s corpus."
    else:
        head = "Here’s the best supported answer from the retrieved sources."
    out = head
    if bullets:
        out += "\n" + "\n".join(f"- {b}" for b in bullets[:4])
    if sources:
        out += "\nSources: " + "; ".join(sources[:6])
    return out

class MultiHitOrchestrator:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        if (environ.get('HTTP_X_ORCH') or '').lower() == 'bypass':
            return self.app(environ, start_response)
        if environ.get('PATH_INFO') != '/ask' or (environ.get('REQUEST_METHOD') or '').upper() != 'POST':
            return self.app(environ, start_response)

        try:
            size = int(environ.get('CONTENT_LENGTH') or '0')
        except Exception:
            size = 0
        body = environ.get('wsgi.input', _io.BytesIO()).read(size) if size > 0 else b'{}'
        environ['wsgi.input'] = _io.BytesIO(body)

        try:
            payload = _json.loads(body.decode('utf-8', 'ignore')) if body else {}
        except Exception:
            payload = {}

        question_raw = _norm_q(str(payload.get('question') or ''))
        if not question_raw:
            return self._forward(body, start_response, label='pass')

        try:
            return self._handle_multihit(question_raw, payload, body, start_response)
        except Exception as exc:  # pragma: no cover - defensive fallback
            _MULTI_LOGGER.exception("MultiHit orchestrator error", exc_info=exc)
            return self._forward(body, start_response, label='error')

    def _forward(self, body: bytes, start_response, label: str):
        status, headers, data = _call_downstream_wsgipost(self.app, body, {'X-Orch': 'bypass'})
        header_list = [(k, v) for (k, v) in (headers or []) if str(k).lower() != 'content-length']
        header_list.append(('X-Orch', label))
        resp = _json.dumps(data, ensure_ascii=False).encode('utf-8')
        header_list.append(('Content-Length', str(len(resp))))
        start_response(status, header_list)
        return [resp]

    def _handle_multihit(self, question_raw: str, payload: Dict[str, Any], body: bytes, start_response):
        intent = _intent(question_raw)
        variants = _variants(question_raw)
        k_plan = [8, 16, 24]
        fused_lists = []
        total_hits = 0
        budget_ms = int(os.environ.get('ORCH_BUDGET_MS', '1200'))
        started = _time.time()

        for variant in variants:
            for k in k_plan:
                if (_time.time() - started) * 1000 > budget_ms:
                    break
                hit_payload = {'question': variant, 'top_k': k}
                raw = _json.dumps(hit_payload).encode('utf-8')
                status, headers, data = _call_downstream_wsgipost(self.app, raw, {'X-Orch': 'bypass'})
                if not status.startswith('2'):
                    raise RuntimeError(f"downstream status {status}")
                ctxs = data.get('contexts') if isinstance(data, dict) else None
                if isinstance(ctxs, list) and ctxs:
                    fused_lists.append(ctxs[:k])
                total_hits += 1
            if (_time.time() - started) * 1000 > budget_ms:
                break

        if not fused_lists:
            fallback = {'question': question_raw, 'top_k': payload.get('top_k', 8)}
            raw = _json.dumps(fallback).encode('utf-8')
            status, headers, data = _call_downstream_wsgipost(self.app, raw, {'X-Orch': 'bypass'})
            header_list = [(k, v) for (k, v) in (headers or []) if str(k).lower() != 'content-length']
            header_list.extend([('X-Intent', intent), ('X-Orch', 'fallback'), ('X-Hits', str(total_hits))])
            resp = _json.dumps(data, ensure_ascii=False).encode('utf-8')
            header_list.append(('Content-Length', str(len(resp))))
            start_response(status, header_list)
            return [resp]

        fused = _rrf(fused_lists)
        picks = _mmr_select(fused, max_n=12, lam=0.7)
        answer = _compose(question_raw, intent, picks)

        out = {'answer': answer, 'contexts': picks, 'mode': 'simple'}
        latency_ms = int((_time.time() - started) * 1000)
        headers = [
            ('Content-Type', 'application/json'),
            ('X-Intent', intent),
            ('X-RRF', 'on'),
            ('X-MMR', '0.7'),
            ('X-Hits', str(total_hits)),
            ('X-BudgetMs', str(latency_ms)),
        ]
        resp = _json.dumps(out, ensure_ascii=False).encode('utf-8')
        headers.append(('Content-Length', str(len(resp))))
        start_response('200 OK', headers)
        return [resp]

# Install orchestrator at the very top of the stack
try:
    app.wsgi_app = MultiHitOrchestrator(app.wsgi_app)
except Exception as _e:
    # If something unexpected happens, do nothing (safe no-op)
    pass
# --- End Multi-Hit Orchestrator ---




# --- multihit orchestrator loader (idempotent) ---
try:
    import importlib
    om = importlib.import_module("orchestrator_mw")

    # Get classes defensively
    Normalize = getattr(om, "NormalizeMiddleware", None) or getattr(om, "NormalizeMid", None)
    Orchestrator = getattr(om, "OrchestratorMiddleware", None) or getattr(om, "OrchestrateMiddleware", None)

    if Normalize and Orchestrator:
        # Order: Normalize OUTSIDE (runs first), then Orchestrator
        app.wsgi_app = Orchestrator(app.wsgi_app)
        app.wsgi_app = Normalize(app.wsgi_app)
        try:
            app.config["SMART_ORCH"] = True
        except Exception:
            pass
except Exception as _e:
    # Fail closed if anything goes wrong
    try:
        import logging
        logging.getLogger(__name__).exception(_e)
    except Exception:
        pass

# --- auto-appended by APPLY_NOSUGGEST.sh ---
try:
    from nosuggest_mw import NoSuggestMiddleware  # noqa: E402
    app.wsgi_app = NoSuggestMiddleware(app.wsgi_app)
except Exception as _e:
    import logging as _lg  # noqa: E402
    _lg.getLogger(__name__).exception("NoSuggestMiddleware install error: %s", _e)
# --- end auto append ---

# --- Smalltalk WSGI wrapper (auto-added v4) ---
try:
    from smalltalk_wsgi import SmalltalkMiddleware
    app.wsgi_app = SmalltalkMiddleware(app.wsgi_app)
except Exception:
    pass
# --- End Smalltalk WSGI wrapper ---

# --- ask2 CORS wrapper ---
try:
    from ask2_cors_mw import CORSReflectMiddleware
    _ASK2_ALLOWED_ORIGINS = [
        "https://sustainacore.org",
        "https://www.sustainacore.org",
        "https://ai.sustainacore.org",
        "https://apex.oracle.com",
    ]
    app.wsgi_app = CORSReflectMiddleware(app.wsgi_app, allowed_origins=_ASK2_ALLOWED_ORIGINS)
except Exception as _e:
    # Fail-safe: never break the API if CORS wrapper import fails
    pass
# --- end ask2 CORS wrapper ---










# --- ask2 LLM orchestrator wrapper ---
try:
    # Ask2 orchestrator disabled (Gemini-first path)
    pass
except Exception:
    pass
# --- end ask2 LLM orchestrator wrapper ---

# register ask2_direct route
try:
    import route_direct
    route_direct.register(app)
except Exception as e:
    print("route_direct register failed:", e)

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

def _is_insufficient(shaped: Optional[Dict[str, Any]]) -> bool:
    """Return True when the pipeline output needs legacy fallback."""

    if not isinstance(shaped, dict):
        return True

    answer = (shaped.get("answer") or "").strip()
    contexts = shaped.get("contexts")
    if not isinstance(contexts, list):
        contexts = []

    if contexts and answer:
        fallback_markers = {
            DEFAULT_EMPTY_ANSWER.strip().lower(),
            INSUFFICIENT_CONTEXT_MESSAGE.strip().lower(),
        }
        router_fallback = (_ASK2_ROUTER_FALLBACK or "").strip().lower()
        if router_fallback:
            fallback_markers.add(router_fallback)
        if answer.strip().lower() not in fallback_markers:
            return False
    return True


@app.route('/ask2', methods=['GET', 'POST'])
def ask2():
    request_started = time.perf_counter()

    def _respond(payload, contexts, *, provider: str, confidence, status: int = 200, headers: Optional[Dict[str, str]] = None):
        compat_payload = normalize_response(
            payload,
            contexts,
            provider=provider,
            confidence=confidence,
        )
        latency_ms = int((time.perf_counter() - request_started) * 1000)
        compat_contexts = compat_payload.get('contexts') if isinstance(compat_payload.get('contexts'), list) else []
        max_score = None
        for entry in compat_contexts:
            if not isinstance(entry, dict):
                continue
            score_val = entry.get('score')
            if score_val is None:
                continue
            try:
                score_float = float(score_val)
            except (TypeError, ValueError):
                continue
            max_score = score_float if max_score is None else max(max_score, score_float)

        log_record = {
            'route': '/ask2',
            'provider': compat_payload.get('provider'),
            'ok': compat_payload.get('ok'),
            'answered': compat_payload.get('answered'),
            'n_contexts': len(compat_contexts),
            'max_score': max_score,
            'confidence': compat_payload.get('confidence'),
            'lat_ms': latency_ms,
        }
        try:
            _METRIC_LOGGER.info(json.dumps(log_record, separators=(',', ':')))
        except Exception:  # pragma: no cover - defensive logging
            _METRIC_LOGGER.info(log_record)

        response = jsonify(compat_payload)
        if headers:
            for key, value in headers.items():
                response.headers[key] = value
        return response, status

    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        q_raw = data.get('q') or data.get('question') or data.get('text')
        k_value = data.get('k') or data.get('top_k') or data.get('limit')
    else:
        args = request.args
        q_raw = args.get('q') or args.get('question') or args.get('text')
        k_value = args.get('k') or args.get('top_k') or args.get('limit')

    question = q_raw.strip() if isinstance(q_raw, str) else ''
    try:
        k_eff = int(k_value) if k_value is not None else 4
    except Exception:
        k_eff = 4
    if k_eff < 1:
        k_eff = 1
    if k_eff > 10:
        k_eff = 10

    header_hints = _collect_request_hints(request)

    if (
        _ASK2_ENABLE_SMALLTALK
        and callable(_router_is_smalltalk)
        and callable(_route_ask2)
        and question
    ):
        try:
            if _router_is_smalltalk(question):
                routed = _route_ask2(question, k_eff)
                if isinstance(routed, dict):
                    routed_meta = routed.get('meta') if isinstance(routed.get('meta'), dict) else {}
                    meta = dict(routed_meta)
                    meta.setdefault('routing', 'smalltalk')
                    meta.setdefault('provider', 'smalltalk')
                    if header_hints:
                        meta.setdefault('request_hints', header_hints)
                    meta.setdefault(
                        'compat_thresholds',
                        {'min_score': SC_RAG_MIN_SCORE, 'min_contexts': SC_RAG_MIN_CONTEXTS},
                    )
                    answer_text = _strip_source_sections((routed.get('answer') or '').strip())
                    payload = {
                        'answer': answer_text,
                        'sources': [],
                        'contexts': [],
                        'meta': meta,
                    }
                    return _respond(
                        payload,
                        payload.get('contexts'),
                        provider=meta.get('provider', 'smalltalk'),
                        confidence=None,
                    )
        except Exception as exc:  # pragma: no cover - defensive logging
            _LOGGER.debug('smalltalk routing failed; continuing pipeline', exc_info=exc)

    forwarded = request.headers.get('X-Forwarded-For', '')
    client_ip = (
        forwarded.split(',')[0].strip()
        if forwarded
        else (request.remote_addr or 'unknown')
    )

    try:
        pipeline_payload, _status = ask2_pipeline_first(question, k_eff, client_ip=client_ip)
    except Exception as exc:  # pragma: no cover - defensive log
        _LOGGER.exception('ask2_pipeline_first_failed', exc_info=exc)
        pipeline_payload = {}

    shaped = pipeline_payload if isinstance(pipeline_payload, dict) else {}
    answer_val = _strip_source_sections(str(shaped.get('answer') or '').strip())
    contexts_raw = shaped.get('contexts') if isinstance(shaped.get('contexts'), list) else []
    meta_val = shaped.get('meta') if isinstance(shaped.get('meta'), dict) else {}
    if header_hints:
        meta_val.setdefault('request_hints', header_hints)
    meta_val.setdefault('routing', 'gemini_first')
    meta_val.setdefault('k', k_eff)
    if isinstance(meta_val, dict):
        meta_val.setdefault('provider', 'gemini')
        meta_val.setdefault(
            'compat_thresholds',
            {'min_score': SC_RAG_MIN_SCORE, 'min_contexts': SC_RAG_MIN_CONTEXTS},
        )

    normalized_contexts: list = []
    for entry in contexts_raw:
        if not isinstance(entry, dict):
            continue
        candidate = dict(entry)
        url_value = candidate.get('source_url') or candidate.get('url') or ''
        if isinstance(url_value, str):
            candidate.setdefault('source_url', url_value)
        normalized_contexts.append(candidate)

    deduped_contexts = dedupe_contexts(normalized_contexts) if normalized_contexts else []
    top_similarity = _extract_top_similarity(deduped_contexts)
    meta_val['top_score'] = top_similarity

    debug_block = meta_val.setdefault('debug', {})
    if 'capability' not in debug_block:
        try:
            from app.retrieval import oracle_retriever as _oracle_module

            debug_block['capability'] = _oracle_module.capability_snapshot()
        except Exception:  # pragma: no cover - defensive
            debug_block['capability'] = {}

    if deduped_contexts:
        floor_triggered = _below_similarity_floor(top_similarity)
        if floor_triggered:
            meta_val.update({'routing': 'low_conf', 'floor_warning': True})
            if not SC_RAG_FAIL_OPEN:
                guard_payload = {
                    'answer': _LOW_CONFIDENCE_MESSAGE,
                    'sources': [],
                    'contexts': [],
                    'meta': meta_val,
                }
                return _respond(
                    guard_payload,
                    guard_payload.get('contexts'),
                    provider='gemini',
                    confidence=top_similarity,
                    headers={'X-Orch': 'pass', 'X-Orig': '1'},
                )

        if callable(_format_router_sources):
            sources = _format_router_sources(
                deduped_contexts,
                max_sources=_ASK2_MAX_SOURCES,
                label_mode=_ASK2_SOURCE_LABEL_MODE,
            )
        else:
            raw_sources = shaped.get('sources') if isinstance(shaped.get('sources'), list) else []
            sources = [str(item).strip() for item in raw_sources[:_ASK2_MAX_SOURCES] if str(item).strip()]
            if not sources:
                sources = _build_default_sources(deduped_contexts)

        payload = {
            'answer': answer_val,
            'sources': sources,
            'contexts': deduped_contexts,
            'meta': meta_val,
        }
        return _respond(
            payload,
            deduped_contexts,
            provider='gemini',
            confidence=top_similarity,
            headers={'X-Orch': 'pass', 'X-Orig': '1'},
        )

    shaped_fallback, status = _call_route_ask2_facade(
        question, k_eff, client_ip=client_ip, header_hints=header_hints
    )
    shaped_fallback = shaped_fallback if isinstance(shaped_fallback, dict) else {}
    answer_fb = _strip_source_sections((shaped_fallback.get('answer') or '').strip())
    sources_fb = shaped_fallback.get('sources') if isinstance(shaped_fallback.get('sources'), list) else []
    contexts_fb = shaped_fallback.get('contexts') if isinstance(shaped_fallback.get('contexts'), list) else []
    meta_fb = shaped_fallback.get('meta') if isinstance(shaped_fallback.get('meta'), dict) else {}
    if header_hints:
        meta_fb.setdefault('request_hints', header_hints)
    if isinstance(meta_fb, dict):
        meta_fb.setdefault('provider', meta_fb.get('routing', 'router'))
        meta_fb.setdefault(
            'compat_thresholds',
            {'min_score': SC_RAG_MIN_SCORE, 'min_contexts': SC_RAG_MIN_CONTEXTS},
        )
    fallback_conf = _extract_top_similarity(contexts_fb)
    payload = {'answer': answer_fb, 'sources': sources_fb, 'contexts': contexts_fb, 'meta': meta_fb}
    return _respond(
        payload,
        contexts_fb,
        provider=meta_fb.get('provider', meta_fb.get('routing', 'router')),
        confidence=fallback_conf,
        status=status,
        headers={'X-Orch': 'pass', 'X-Orig': '1'},
    )


@app.route("/api/ask2", methods=["POST"])
def api_ask2():
    auth = _api_auth_or_unauthorized()
    if auth is not None:
        return auth

    body = request.get_json(silent=True) or {}
    question_val = (
        body.get("user_message")
        or body.get("question")
        or body.get("q")
        or body.get("text")
    )
    question_text = question_val.strip() if isinstance(question_val, str) else ""
    raw_top_k = body.get("top_k") or body.get("k") or body.get("limit")
    k_eff = _api_coerce_k(raw_top_k, default=4)

    if not question_text:
        return jsonify({"error": "question is required"}), 400

    forwarded_for = request.headers.get("X-Forwarded-For") or request.remote_addr or "unknown"
    passthrough_headers = {}
    for header_name in ("X-Ask2-Hints", "X-Ask2-Meta", "X-Ask2-Routing", "X-Forwarded-For"):
        value = request.headers.get(header_name)
        if value:
            passthrough_headers[header_name] = value
    passthrough_headers.setdefault("X-Forwarded-For", forwarded_for)

    try:
        with app.test_request_context(
            path="/ask2",
            method="POST",
            json={"question": question_text, "k": k_eff},
            headers=passthrough_headers,
        ):
            upstream = ask2()
    except Exception as exc:
        _LOGGER.exception("api_ask2 delegation failed", exc_info=exc)
        return (
            jsonify(
                {
                    "error": "backend_failure",
                    "message": "Ask2 backend failed while processing the request.",
                    "hint": "Check VM1 logs for details.",
                }
            ),
            502,
        )

    if isinstance(upstream, tuple):
        upstream_response, upstream_status = upstream
    else:
        upstream_response, upstream_status = upstream, getattr(upstream, "status_code", 200)

    upstream_json = {}
    try:
        upstream_json = upstream_response.get_json(silent=True) or {}
    except Exception as exc:
        _LOGGER.exception("api_ask2 response parse failed", exc_info=exc)
        return (
            jsonify(
                {
                    "error": "backend_failure",
                    "message": "Ask2 backend failed while processing the request.",
                    "hint": "Check VM1 logs for details.",
                }
            ),
            502,
        )

    if upstream_status >= 500:
        _LOGGER.warning("api_ask2 upstream error status=%s", upstream_status)
        return (
            jsonify(
                {
                    "error": "backend_failure",
                    "message": "Ask2 backend failed while processing the request.",
                    "hint": "Check VM1 logs for details.",
                }
            ),
            502,
        )

    meta = upstream_json.get("meta") if isinstance(upstream_json.get("meta"), dict) else {}
    session_id = (
        upstream_json.get("session_id")
        or upstream_json.get("trace_id")
        or meta.get("session_id")
        or meta.get("trace_id")
    )
    if not session_id:
        session_id = str(uuid.uuid4())

    reply = str(upstream_json.get("answer") or "").strip()
    shaped = {
        "session_id": session_id,
        "reply": reply,
        "meta": meta,
    }
    if "sources" in upstream_json:
        shaped["sources"] = upstream_json.get("sources")
    if "contexts" in upstream_json:
        shaped["contexts"] = upstream_json.get("contexts")
    if "answer" in upstream_json:
        shaped["answer"] = upstream_json.get("answer")

    return jsonify(shaped), upstream_status

@app.route("/ask", methods=["GET"], endpoint="ask_get_shim")
def ask_get_shim():
    from flask import request, jsonify
    q = (request.args.get("question") or request.args.get("q") or "").strip()
    if not q:
        return jsonify({"error":"q is required"}), 400

    explicit_filters = {
        key: request.args.get(key, "")
        for key in ("docset", "namespace", "ticker", "company")
        if request.args.get(key)
    }
    scope = infer_scope(q, explicit_filters=explicit_filters, entities=extract_entities(q))
    filters = scope.applied_filters or {}

    # Try the fast path; if embeddings service is down/misconfigured, fall back.
    try:
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE), filters=filters)
        ans = rows[0]["chunk_text"] if rows else "No context found."
        if RETURN_TOP_AS_ANSWER:
            return jsonify({"answer": ans, "contexts": rows, "mode":"simple"})
    except Exception:
        pass

    retrieval = retrieve(q, explicit_filters=explicit_filters)
    entities = retrieval.get("entities", [])
    chunks = retrieval.get("chunks", [])
    quotes  = extract_quotes(chunks, limit_words=60)
    intent  = detect_intent(q, entities)
    answer, shape, sources_out = compose_answer(intent, q, entities, chunks, quotes)
    return jsonify({"answer": answer, "contexts": chunks, "mode":"simple", "sources": sources_out})




# --- compat: safe_top_k_by_vector wrapper ---
try:
    import inspect as _inspect

    _orig_tkbv = _top_k_by_vector

    if callable(_orig_tkbv):

        def top_k_by_vector_compat(question, k, *args, **kwargs):
            if "filters" in kwargs:
                try:
                    sig = _inspect.signature(_orig_tkbv)
                    if "filters" not in sig.parameters:
                        kwargs.pop("filters", None)
                except Exception:
                    kwargs.pop("filters", None)
            return _orig_tkbv(question, k, *args, **kwargs)

        _top_k_by_vector = top_k_by_vector_compat
except Exception:
    pass


# --- BEGIN: APEX POST /ask compatibility (no URL change) ---------------------
import os, time

def __sc__build_sources(rows):
    out = []
    for r in rows or []:
        doc_id = str(r.get("doc_id",""))
        chunk_ix = r.get("chunk_ix", 0)
        rid = f"{doc_id}-{chunk_ix}" if doc_id else str(chunk_ix)
        dist = r.get("dist")
        try:
            score = round(1.0 - float(dist), 4) if dist is not None else None
        except Exception:
            score = None
        snippet = (r.get("chunk_text") or "")[:400]
        out.append({
            "id": rid,
            "score": score,
            "snippet": snippet,
            "title": r.get("title"),
            "url": r.get("source_url"),
        })
    return out

@app.before_request
def __sc__apex_post_ask_compat():
    # keep a single path (/ask), adjust only POST behavior for APEX
    from flask import request, jsonify
    if request.path != "/ask" or request.method != "POST":
        return  # don't touch anything else

    t0 = time.perf_counter()

    # Accept q from args OR JSON body (both cases APEX might use)
    body = {}
    if request.is_json:
        body = request.get_json(silent=True) or {}
    q = (request.args.get("q") or request.args.get("question")
         or body.get("q") or body.get("question") or "").strip()
    if not q:
        return jsonify({"error": "q is required"}), 400

    try:
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE))
        # Stable 'answer' for APEX: if your main chain returns text elsewhere,
        # we still guarantee a non-empty answer by falling back to top snippet.
        ans = (rows[0].get("chunk_text") if rows else None) or "No context found."

        took_ms = int((time.perf_counter() - t0) * 1000)
        resp = {
            "answer": ans,
            "sources": __sc__build_sources(rows),
            "meta": {
                "k": len(rows or []),
                "took_ms": took_ms,
                "model_info": {"embed": EMBED_MODEL_NAME},
            },
        }
        return jsonify(resp), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# --- END: APEX POST /ask compatibility ---------------------------------------
