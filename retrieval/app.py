
# SustainaCore app.py — SMART v2
import base64
import importlib
import importlib.util
import json
import logging
import os
import re
import subprocess
import time
import zlib
from pathlib import Path
from typing import Any, Dict, Optional

# Allow this module to expose the sibling package under app/
__path__ = [str(Path(__file__).resolve().parent / "app")]

import requests
from collections import defaultdict
from flask import Flask, request, jsonify

from embedder_settings import (
    EmbedParityError,
    get_embed_settings,
    run_startup_parity_check,
)
from embedding_client import embed_text
from nosuggest_mw import _strip_scaffold
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
from retrieval.utils.entity_match import (
    _canonicalize_question_entity,
    _fuzzy_contains,
    _normalize_company_name,
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
    _ASK2_ROUTER_FALLBACK = getattr(
        _ask2_routing,
        "NO_HIT_FALLBACK",
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look.",
    )
else:  # pragma: no cover - import fallback
    _route_ask2 = None
    _ASK2_ROUTER_FALLBACK = (
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look."
    )


_LOGGER = logging.getLogger("app.ask2")
_READINESS_LOGGER = logging.getLogger("app.readyz")
_MULTI_LOGGER = logging.getLogger("app.multihit")
_STARTUP_LOGGER = logging.getLogger("app.startup")


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


def _sources_from_contexts(contexts: list, limit: int = 5) -> list:
    seen = set()
    collected = []
    for ctx in contexts:
        title = (ctx.get("title") or "").strip()
        url = (ctx.get("source_url") or "").strip()
        if not title and not url:
            continue
        key = (title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        collected.append({"title": title or (url or "Source"), "url": url})
        if len(collected) >= limit:
            break
    return collected


def _summarize_contexts(contexts: list, limit: int = 3) -> str:
    lines = []
    for ctx in contexts[:limit]:
        snippet = (ctx.get("chunk_text") or "").strip()
        if not snippet:
            continue
        sentence = snippet.split("\n")[0].strip()
        if not sentence:
            continue
        if len(sentence) > 220:
            sentence = sentence[:217].rsplit(" ", 1)[0].strip() + "…"
        lines.append(f"- {sentence}")
    if not lines:
        return ""
    return "Here’s what the retrieved Sustainacore sources highlight:\n" + "\n".join(lines)


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

        if contexts:
            if not answer_value or answer_value.strip() == INSUFFICIENT_CONTEXT_MESSAGE:
                synthesized = _summarize_contexts(contexts)
                if synthesized:
                    answer_value = synthesized
        if contexts and not sources_list:
            sources_list = _sources_from_contexts(contexts)

        meta_raw = data.get("meta") if isinstance(data, dict) else None
        meta_dict = dict(meta_raw) if isinstance(meta_raw, dict) else {}
        if extra_meta:
            meta_dict.update(extra_meta)
        meta_dict.setdefault("inline_sources_enabled", _INLINE_SOURCES_ENABLED)
        meta_dict.setdefault("small_talk_enabled", _SMALL_TALK_ENABLED)
        if "routing" not in meta_dict:
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
                    if SIMILARITY_FLOOR_MODE == "enforce":
                        allow_contexts = False
                        sources_list = []
                        answer_value_local = _ensure_non_empty_answer(
                            answer_override
                            or INSUFFICIENT_CONTEXT_MESSAGE,
                            fallback=INSUFFICIENT_CONTEXT_MESSAGE,
                        )
                        answer_value = answer_value_local
                else:
                    floor_decision = "ok"
            else:
                if SIMILARITY_FLOOR_MODE == "enforce":
                    floor_decision = "below_floor"
                    allow_contexts = False
                    sources_list = []
                    answer_value = _ensure_non_empty_answer(
                        answer_override or INSUFFICIENT_CONTEXT_MESSAGE,
                        fallback=INSUFFICIENT_CONTEXT_MESSAGE,
                    )

        normalized_answer = _ensure_non_empty_answer(
            answer_value, fallback=DEFAULT_EMPTY_ANSWER
        )
        normalized_answer = _strip_source_sections(normalized_answer)
        try:
            cleaned_answer, scaffold_flag = _strip_scaffold(normalized_answer)
            if scaffold_flag and cleaned_answer:
                normalized_answer = cleaned_answer
        except Exception:
            pass

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
        if not (path=="/ask2" and method=="POST"):
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
            payload={
                "answer": INSUFFICIENT_CONTEXT_MESSAGE,
                "contexts": chunks,
                "mode": "simple",
                "sources": [],
            }
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
        'PATH_INFO':'/ask2',
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
        if environ.get('PATH_INFO') != '/ask2' or (environ.get('REQUEST_METHOD') or '').upper() != 'POST':
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

@app.route('/ask2', methods=['GET', 'POST'])
def ask2():
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        q_raw = data.get('q') or data.get('question') or data.get('text')
        k_value = data.get('k') or data.get('top_k') or data.get('limit')
    else:
        args = request.args
        q_raw = args.get('q') or args.get('question') or args.get('text')
        k_value = args.get('k') or args.get('top_k') or args.get('limit')

    question = q_raw.strip() if isinstance(q_raw, str) else ''
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        client_ip = forwarded.split(',')[0].strip()
    else:
        client_ip = request.remote_addr or 'unknown'
    header_hints = _collect_request_hints(request)
    shaped, status = _call_route_ask2_facade(
        question, k_value, client_ip=client_ip, header_hints=header_hints
    )
    return jsonify(shaped), status

