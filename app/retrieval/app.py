import json
import logging
import os
import subprocess
import time
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from anyio import to_thread
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.auth.login_codes import (
    TOKEN_TTL_SECONDS,
    is_valid_email,
    normalize_email,
    request_login_code,
    verify_login_code,
)
from app.news_service import create_curated_news_item, fetch_news_items, fetch_news_item_detail
from app.persona import apply_persona
from app.request_normalizer import BAD_INPUT_ERROR, normalize_request

try:  # Import lazily to avoid hard failures when optional deps are missing.
    from app.retrieval.settings import settings as _SETTINGS
except Exception:  # pragma: no cover - defensive fallback when settings fails.
    _SETTINGS = None

# Router is optional; app must still boot if it’s missing or broken.
try:
    from app.rag.routing import route_ask2 as _route_ask2
except Exception:
    _route_ask2 = None

try:
    from app.retrieval.service import (
        GeminiUnavailableError,
        RateLimitError,
        run_pipeline,
    )
except Exception:  # pragma: no cover - fall back to facade-only mode
    run_pipeline = None

    class RateLimitError(Exception):
        """Placeholder when the service layer failed to import."""

    class GeminiUnavailableError(Exception):
        """Placeholder when the service layer failed to import."""

FALLBACK = (
    "I couldn’t find a grounded answer in the indexed docs yet. "
    "Try adding a company/topic (e.g., Microsoft, TECH100) or a specific field."
)

PERSONA_ENABLED = os.getenv("PERSONA_V1") == "1"
REQUEST_NORMALIZE_ENABLED = os.getenv("REQUEST_NORMALIZE") == "1"
CI_EVAL_FIXTURES_ENABLED = os.getenv("CI_EVAL_FIXTURES") == "1"

_CI_FIXTURE_PATH = Path(__file__).resolve().parents[2] / "eval" / "fixtures" / "ci_stub.json"

LOGGER = logging.getLogger("ask2")
_API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")
_API_AUTH_WARNED = False
_AUTH_TOKEN_SIGNING_KEY = os.getenv("AUTH_TOKEN_SIGNING_KEY")
if not _AUTH_TOKEN_SIGNING_KEY:
    raise RuntimeError("AUTH_TOKEN_SIGNING_KEY is required for auth token signing")


def _resolve_git_sha() -> str:
    """Best effort resolution of the git SHA for observability surfaces."""

    env_candidates = [
        os.getenv("SUSTAINACORE_GIT_SHA"),
        os.getenv("GIT_SHA"),
    ]
    for candidate in env_candidates:
        if candidate:
            stripped = candidate.strip()
            if stripped:
                return stripped

    file_candidates = [
        os.getenv("SUSTAINACORE_GIT_SHA_FILE"),
        os.getenv("GIT_SHA_FILE"),
    ]

    repo_root = Path(__file__).resolve().parents[2]
    file_candidates.extend(
        [
            str(repo_root / "config" / "git-sha"),
            str(repo_root / "config" / "git_sha"),
        ]
    )

    for file_candidate in file_candidates:
        if not file_candidate:
            continue
        path = Path(file_candidate)
        if not path.exists():
            continue
        try:
            contents = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if contents:
            return contents

    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        candidate = output.decode("utf-8").strip()
        return candidate or "unknown"
    except Exception:  # pragma: no cover - git unavailable in some deployments
        return "unknown"


def _resolve_provider() -> str:
    """Determine which LLM provider is active for reporting."""

    explicit = os.getenv("ASK2_PROVIDER")
    if explicit:
        explicit = explicit.strip().upper()
        if explicit in {"GEMINI", "OLLAMA"}:
            return explicit

    if _SETTINGS and getattr(_SETTINGS, "gemini_first_enabled", False):
        return "GEMINI"

    if run_pipeline is not None:
        return "GEMINI"

    return "OLLAMA"


def _resolve_model(provider: str) -> str:
    if provider == "GEMINI":
        if _SETTINGS and getattr(_SETTINGS, "gemini_model_answer", None):
            return _SETTINGS.gemini_model_answer
        for name in ("GEMINI_ANSWER_MODEL", "GEMINI_MODEL"):
            value = os.getenv(name)
            if value:
                return value
        return "gemini"

    for name in ("SCAI_OLLAMA_MODEL", "OLLAMA_MODEL", "OLLAMA_EMBED_MODEL"):
        value = os.getenv(name)
        if value:
            return value
    return "ollama"


_GIT_SHA = _resolve_git_sha()
_PROVIDER = _resolve_provider()
_MODEL_NAME = _resolve_model(_PROVIDER)

ASK_EMPTY = "Please provide a question so I can help."  # Friendly guardrail


def _api_auth_guard(request: Request) -> Optional[JSONResponse]:
    """Return a JSONResponse when the Authorization header is invalid."""

    global _API_AUTH_WARNED

    if not _API_AUTH_TOKEN:
        if not _API_AUTH_WARNED:
            LOGGER.warning("API_AUTH_TOKEN not set; denying /api/* requests")
            _API_AUTH_WARNED = True
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    header = request.headers.get("authorization", "")
    if not isinstance(header, str) or not header.lower().startswith("bearer "):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    candidate = header.split(" ", 1)[1].strip()
    if not candidate or candidate != _API_AUTH_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    return None


def _format_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    return None


def _format_weight(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        weight = float(value)
    except Exception:
        try:
            weight = float(str(value).strip())
        except Exception:
            return None
    weight = weight * 100 if abs(weight) <= 1 else weight
    return round(weight, 2)


def _format_timestamp(value: Any) -> Optional[str]:
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


def _resolve_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip() or "unknown"
    return request.client.host if request.client and request.client.host else "unknown"


def _sanitize_k(value: Any, default: int = 4) -> int:
    try:
        k = int(value)
    except Exception:
        k = default
    if k < 1:
        k = 1
    if k > 10:
        k = 10
    return k


def _shape_sources_and_contexts(
    raw_sources: Any, raw_contexts: Any = None
) -> Tuple[List[str], List[Dict[str, Any]]]:
    urls: List[str] = []
    contexts: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def _clean(value: Any) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
        return None

    def _register(url_value: Optional[str], title_value: Optional[str]) -> None:
        url_clean = _clean(url_value)
        if not url_clean or url_clean in seen:
            return
        seen.add(url_clean)
        urls.append(url_clean)
        context: Dict[str, Any] = {"source_url": url_clean}
        title_clean = _clean(title_value)
        if title_clean is not None:
            context["title"] = title_clean
        contexts.append(context)

    if isinstance(raw_contexts, list):
        for item in raw_contexts:
            if not isinstance(item, dict):
                continue
            url_candidate = (
                item.get("source_url")
                or item.get("url")
                or item.get("link")
                or item.get("href")
            )
            title_candidate = item.get("title") or item.get("source_title") or item.get("name")
            _register(url_candidate, title_candidate)

    if isinstance(raw_sources, list):
        for item in raw_sources:
            url_candidate: Optional[str] = None
            title_candidate: Optional[str] = None
            if isinstance(item, dict):
                title_candidate = item.get("title") or item.get("source_title") or item.get("name")
                url_candidate = (
                    item.get("url")
                    or item.get("source_url")
                    or item.get("link")
                    or item.get("href")
                )
            elif isinstance(item, str):
                url_candidate = item
            _register(url_candidate, title_candidate)

    return urls, contexts


def _build_payload(
    *,
    answer: str,
    raw_sources: Any,
    top_k: int,
    note: str,
    meta: Optional[Dict[str, Any]] = None,
    raw_contexts: Any = None,
    limit_sources: Optional[int] = None,
) -> Dict[str, Any]:
    sources, contexts = _shape_sources_and_contexts(raw_sources, raw_contexts)
    # Always cap sources to keep payloads tight and stable for the UI.
    # Keep this independent of `note` so "ok" responses can't balloon.
    if limit_sources is None:
        limit_sources = 5
    limit_sources = min(int(limit_sources), 5) if limit_sources is not None else 5
    if limit_sources is not None:
        sources = sources[:limit_sources]
        contexts = contexts[:limit_sources]

    persona_sources: Optional[List[str]] = None
    if PERSONA_ENABLED and note == "ok":
        answer, persona_sources = apply_persona(answer, contexts)
    payload_meta: Dict[str, Any] = {}
    if isinstance(meta, dict):
        payload_meta.update(meta)
    payload_meta["provider"] = payload_meta.get("provider") or "oracle"
    payload_meta["note"] = note
    payload_meta["k"] = top_k
    sources_payload: List[Any]
    if persona_sources is not None:
        sources_payload = persona_sources
    else:
        sources_payload = sources
    return {
        "answer": answer,
        "contexts": contexts,
        "sources": sources_payload,
        "meta": payload_meta,
    }

_START_TS = time.time()


@lru_cache(maxsize=1)
def _load_ci_fixture() -> Dict[str, Any]:
    """Load the CI stub fixture when stub mode is enabled."""

    try:
        with _CI_FIXTURE_PATH.open("r", encoding="utf-8") as handle:
            fixture = json.load(handle)
    except FileNotFoundError:
        fixture = {}
    except json.JSONDecodeError:
        LOGGER.warning("ci_stub fixture file is not valid JSON; falling back to defaults")
        fixture = {}

    answer_prefix = fixture.get("answer_prefix") if isinstance(fixture, dict) else None
    if not isinstance(answer_prefix, str) or not answer_prefix.strip():
        answer_prefix = (
            "This is a stubbed SustainaCore answer for \"{question}\". "
            "It validates the Persona eval contract in CI."
        )

    contexts = fixture.get("contexts") if isinstance(fixture, dict) else None
    if not isinstance(contexts, list) or not contexts:
        contexts = [
            "CI fixture context {n}: sustainability commitments snapshot for \"{question}\".",
            "CI fixture context {n}: governance summary touching climate risk around \"{question}\".",
            "CI fixture context {n}: emissions disclosure excerpt relevant to \"{question}\".",
        ]

    sources = fixture.get("sources") if isinstance(fixture, dict) else None
    normalized_sources: List[Dict[str, str]] = []
    if isinstance(sources, list):
        for idx, source in enumerate(sources, start=1):
            if not isinstance(source, dict):
                continue
            title = source.get("title") or source.get("name")
            url = source.get("url") or source.get("href") or source.get("source_url")
            if not isinstance(title, str) or not title.strip():
                title = f"CI Fixture Document {idx}"
            if not isinstance(url, str) or not url.strip():
                url = f"fixture://doc{idx}"
            normalized_sources.append({"title": title.strip(), "url": url.strip()})
            if len(normalized_sources) >= 3:
                break

    if not normalized_sources:
        normalized_sources = [
            {"title": "CI Fixture Document 1", "url": "fixture://doc1"},
            {"title": "CI Fixture Document 2", "url": "fixture://doc2"},
            {"title": "CI Fixture Document 3", "url": "fixture://doc3"},
        ]

    return {
        "answer_prefix": answer_prefix,
        "contexts": contexts,
        "sources": normalized_sources,
    }


def _render_ci_stub(question_text: str, requested_top_k: int) -> Dict[str, Any]:
    """Return a deterministic stub response for CI eval jobs."""

    fixture = _load_ci_fixture()
    effective_question = question_text.strip() or "this query"
    effective_k = max(3, _sanitize_k(requested_top_k))

    contexts: List[str] = []
    templates = fixture["contexts"]
    if not templates:
        templates = [
            "CI fixture context {n}: sustainability commitments snapshot for \"{question}\".",
            "CI fixture context {n}: governance summary touching climate risk around \"{question}\".",
            "CI fixture context {n}: emissions disclosure excerpt relevant to \"{question}\".",
        ]
    for idx in range(effective_k):
        template = templates[idx % len(templates)]
        if not isinstance(template, str) or not template:
            template = "CI fixture context {n}: supporting detail for \"{question}\"."
        contexts.append(
            template.format(question=effective_question, n=idx + 1, index=idx + 1)
        )

    sources = [dict(item) for item in fixture["sources"]]
    lines: List[str] = []
    for idx, source in enumerate(sources, start=1):
        title = source.get("title", "").strip()
        url = source.get("url", "").strip()
        lines.append(f"{idx}) {title} — {url}")
    sources_lines = "\n".join(lines)

    answer_prefix = fixture["answer_prefix"].format(question=effective_question)
    answer = f"{answer_prefix}\n\nSources:\n{sources_lines}".strip()

    meta = {
        "note": "ci_stub",
        "k": effective_k,
        "mode": "ci_stub",
        "persona": PERSONA_ENABLED,
        "normalize": REQUEST_NORMALIZE_ENABLED,
    }

    return {
        "answer": answer,
        "contexts": contexts,
        "sources": sources,
        "meta": meta,
    }

app = FastAPI(title="SustainaCore Retrieval Facade", version="1.0")


def _finalize_response(payload: Dict[str, Any], started: float) -> JSONResponse:
    answer_value = payload.get("answer")
    if not isinstance(answer_value, str):
        payload["answer"] = "" if answer_value is None else str(answer_value)

    contexts_value = payload.get("contexts")
    if not isinstance(contexts_value, list):
        contexts: List[Dict[str, Any]] = []
    else:
        contexts = contexts_value
    payload["contexts"] = contexts

    sources_value = payload.get("sources")
    if not isinstance(sources_value, list):
        payload["sources"] = []

    if "meta" not in payload or not isinstance(payload["meta"], dict):
        payload["meta"] = {}

    latency_ms = int((time.perf_counter() - started) * 1000)

    top_titles: List[str] = []
    for context in contexts[:3]:
        if not isinstance(context, dict):
            continue
        candidate = context.get("title") or context.get("source_title") or context.get("source_url")
        if isinstance(candidate, str):
            stripped = candidate.strip()
            if stripped:
                top_titles.append(stripped)
    LOGGER.info(
        "ask2.response %s",
        json.dumps(
            {
                "contexts_count": len(contexts),
                "top_titles": top_titles,
                "lat_ms": latency_ms,
                "persona": PERSONA_ENABLED,
                "normalize": REQUEST_NORMALIZE_ENABLED,
            },
            ensure_ascii=False,
        ),
    )
    return JSONResponse(payload, media_type="application/json")


@app.get("/health")
async def health() -> JSONResponse:
    payload = {
        "ok": True,
        "git": _GIT_SHA,
        "provider": _PROVIDER,
        "model": _MODEL_NAME,
        "flags": {
            "PERSONA_V1": PERSONA_ENABLED,
            "REQUEST_NORMALIZE": REQUEST_NORMALIZE_ENABLED,
        },
    }
    return JSONResponse(payload, media_type="application/json")


@app.get("/healthz")
def healthz() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Dict[str, float]:
    uptime = time.time() - _START_TS
    if uptime < 0:
        uptime = 0.0
    return {"uptime": float(uptime)}


@app.post("/api/auth/request-code")
async def api_auth_request_code(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            {"error": "bad_request", "message": "Invalid JSON payload."},
            status_code=400,
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            {"error": "bad_request", "message": "Payload must be a JSON object."},
            status_code=400,
        )

    email_raw = payload.get("email")
    if not isinstance(email_raw, str):
        return JSONResponse(
            {"error": "bad_request", "message": "Email is required."},
            status_code=400,
        )

    email = email_raw.strip()
    if not is_valid_email(email):
        return JSONResponse(
            {"error": "bad_request", "message": "Invalid email format."},
            status_code=400,
        )

    email_normalized = normalize_email(email)
    client_ip = _resolve_client_ip(request)
    request_login_code(email_normalized, client_ip)

    return JSONResponse({"ok": True}, media_type="application/json")


@app.post("/api/auth/verify-code")
async def api_auth_verify_code(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            {"error": "bad_request", "message": "Invalid JSON payload."},
            status_code=400,
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            {"error": "bad_request", "message": "Payload must be a JSON object."},
            status_code=400,
        )

    email_raw = payload.get("email")
    code_raw = payload.get("code")
    if not isinstance(email_raw, str) or code_raw is None:
        return JSONResponse(
            {"error": "invalid_code", "message": "Invalid code."},
            status_code=400,
        )

    email = email_raw.strip()
    if not is_valid_email(email):
        return JSONResponse(
            {"error": "invalid_code", "message": "Invalid code."},
            status_code=400,
        )

    code = str(code_raw).strip()
    if not code.isdigit() or len(code) != 6:
        return JSONResponse(
            {"error": "invalid_code", "message": "Invalid code."},
            status_code=400,
        )

    email_normalized = normalize_email(email)
    token = verify_login_code(email_normalized, code, _AUTH_TOKEN_SIGNING_KEY)
    if not token:
        return JSONResponse(
            {"error": "invalid_code", "message": "Invalid code."},
            status_code=400,
        )

    return JSONResponse(
        {"token": token, "expires_in_seconds": TOKEN_TTL_SECONDS},
        media_type="application/json",
    )


@app.get("/api/tech100")
async def api_tech100(request: Request) -> JSONResponse:
    auth = _api_auth_guard(request)
    if auth is not None:
        return auth

    try:
        from db_helper import _to_plain, get_connection  # type: ignore
    except Exception as exc:
        LOGGER.exception("api_tech100 helper import failed", exc_info=exc)
        return JSONResponse(
            {"error": "backend_failure", "message": "Unable to load TECH100 data."},
            status_code=500,
        )

    sql = (
        "SELECT rank_index, company_name, gics_sector, port_date, port_weight, "
        "aiges_composite_average, transparency, governance_structure, "
        "region, country, location "
        "FROM tech11_ai_gov_eth_index "
        "WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index) "
        "ORDER BY rank_index FETCH FIRST 100 ROWS ONLY"
    )

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0].lower() for desc in cur.description]
            items: List[Dict[str, Any]] = []
            for raw in cur.fetchall():
                row = {col: _to_plain(val) for col, val in zip(columns, raw)}
                items.append(
                    {
                        "company": row.get("company_name"),
                        "sector": row.get("gics_sector"),
                        "region": row.get("region")
                        or row.get("country")
                        or row.get("location"),
                        "overall": row.get("aiges_composite_average"),
                        "transparency": row.get("transparency"),
                        "accountability": row.get("governance_structure"),
                        "updated_at": _format_date(row.get("port_date")),
                        "port_weight": _format_weight(row.get("port_weight")),
                        "weight": _format_weight(row.get("port_weight")),
                    }
                )
    except Exception as exc:
        LOGGER.exception("api_tech100 query failed", exc_info=exc)
        return JSONResponse(
            {"error": "backend_failure", "message": "Unable to load TECH100 data."},
            status_code=500,
        )

    return JSONResponse({"items": items}, media_type="application/json")


@app.get("/api/news")
async def api_news(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    days: Optional[int] = Query(None, ge=1, le=365),
    source: Optional[str] = Query(None),
    tag: Optional[List[str]] = Query(None),
    ticker: Optional[str] = Query(None),
) -> JSONResponse:
    header = request.headers.get("authorization", "")
    if header:
        auth = _api_auth_guard(request)
        if auth is not None:
            return auth

    try:
        items, has_more, effective_limit = fetch_news_items(
            limit=limit, days=days, source=source, tags=tag, ticker=ticker
        )
    except Exception as exc:
        LOGGER.exception("api_news query failed", exc_info=exc)
        return JSONResponse(
            {
                "error": "news_unavailable",
                "message": "News is temporarily unavailable.",
            },
            status_code=500,
        )

    response = {
        "items": items,
        "meta": {
            "count": len(items),
            "limit": effective_limit,
            "has_more": has_more,
        },
    }
    return JSONResponse(response, media_type="application/json")


@app.get("/api/news/{news_id}")
async def api_news_detail(request: Request, news_id: str) -> JSONResponse:
    header = request.headers.get("authorization", "")
    if header:
        auth = _api_auth_guard(request)
        if auth is not None:
            return auth

    try:
        item = fetch_news_item_detail(news_id)
    except Exception as exc:
        LOGGER.exception("api_news_detail query failed", exc_info=exc)
        return JSONResponse(
            {"error": "news_unavailable", "message": "News is temporarily unavailable."},
            status_code=500,
        )

    if not item:
        return JSONResponse(
            {"error": "not_found", "message": "News item not found."},
            status_code=404,
        )

    return JSONResponse({"item": item}, media_type="application/json")


@app.post("/api/news/admin/items")
async def api_news_admin_items(request: Request) -> JSONResponse:
    auth = _api_auth_guard(request)
    if auth is not None:
        return auth

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            {"error": "bad_request", "message": "Invalid JSON payload."},
            status_code=400,
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            {"error": "bad_request", "message": "Payload must be a JSON object."},
            status_code=400,
        )

    try:
        item = create_curated_news_item(payload)
    except ValueError as exc:
        return JSONResponse(
            {"error": "bad_request", "message": str(exc)}, status_code=400
        )
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("api_news_admin_items failed", exc_info=exc)
        return JSONResponse(
            {"error": "news_unavailable", "message": "Unable to create news item."},
            status_code=500,
        )

    return JSONResponse({"item": item}, status_code=201, media_type="application/json")


@app.post("/ask2")
async def ask2_post(request: Request) -> JSONResponse:
    return await _handle_ask2_request(request, require_auth=False)


@app.post("/api/ask2")
async def api_ask2_post(request: Request) -> JSONResponse:
    """Authenticated Ask2 endpoint used by VM2 proxy (/ask2/api/ -> VM1 /api/ask2)."""
    return await _handle_ask2_request(request, require_auth=True)


async def _handle_ask2_request(request: Request, *, require_auth: bool) -> JSONResponse:
    started = time.perf_counter()
    if require_auth:
        auth = _api_auth_guard(request)
        if auth is not None:
            return auth

    raw_body = await request.body()
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()

    payload: Any
    if content_type == "text/plain":
        decoded = raw_body.decode("utf-8", "ignore") if raw_body else ""
        payload = {"question": decoded}
    elif raw_body:
        try:
            decoded = raw_body.decode("utf-8", "ignore")
            payload = json.loads(decoded or "{}")
        except ValueError:
            payload = {}
    else:
        payload = {}

    effective_payload: Dict[str, Any]
    if REQUEST_NORMALIZE_ENABLED:
        normalized, error = await normalize_request(request, payload, raw_body=raw_body)
        if error:
            top_k = _sanitize_k((normalized or {}).get("top_k"))
            response = _build_payload(
                answer="",
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "bad_input"},
            )
            response["error"] = BAD_INPUT_ERROR
            response.setdefault("contexts", [])
            response.setdefault("sources", [])
            return _finalize_response(response, started)
        effective_payload = normalized
    elif isinstance(payload, dict):
        effective_payload = dict(payload)
    else:
        effective_payload = {}

    question_value = effective_payload.get("question")
    if question_value is None:
        question_value = effective_payload.get("query")
    if question_value is None:
        question_value = effective_payload.get("q")
    if question_value is None:
        question_value = effective_payload.get("text")
    if question_value is None:
        question_value = effective_payload.get("user_message")
    if question_value is None:
        question_value = effective_payload.get("message")
    question_text = question_value.strip() if isinstance(question_value, str) else ""

    raw_top_k = effective_payload.get("top_k")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("topK")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("k")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("limit")
    top_k = _sanitize_k(raw_top_k)

    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip() or "unknown"
    else:
        client_ip = request.client.host if request.client and request.client.host else "unknown"

    if not question_text:
        return _finalize_response(
            _build_payload(
                answer=ASK_EMPTY,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "empty_question"},
            ),
            started,
        )

    if CI_EVAL_FIXTURES_ENABLED:
        LOGGER.info("ask2.ci_stub %s", json.dumps({"k": top_k, "question_len": len(question_text)}))
        return _finalize_response(_render_ci_stub(question_text, top_k), started)

    if run_pipeline is None:
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "pipeline_unavailable"},
            ),
            started,
        )

    try:
        # anyio.to_thread.run_sync does not accept kwargs in all supported anyio versions.
        # Wrap the call so we can pass keyword arguments safely.
        result = await to_thread.run_sync(
            lambda: run_pipeline(question_text, k=top_k, client_ip=client_ip)
        )
    except RateLimitError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    except GeminiUnavailableError:
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "gemini_unavailable"},
            ),
            started,
        )
    except Exception as exc:
        # This is the primary production failure mode when Oracle/vector plumbing
        # is broken. Log the exception so VM1 operators can see the root cause
        # in journald without enabling debug flags.
        LOGGER.exception("ask2.pipeline_error", exc_info=exc)
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "pipeline_error"},
            ),
            started,
        )

    answer_text = str(result.get("answer") or "").strip()
    raw_sources = result.get("sources") or []
    raw_contexts = result.get("contexts")
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

    if not answer_text:
        fallback_meta = dict(meta)
        fallback_meta["reason"] = "empty_answer"
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=raw_sources,
                raw_contexts=raw_contexts,
                top_k=top_k,
                note="fallback",
                meta=fallback_meta,
            ),
            started,
        )

    return _finalize_response(
        _build_payload(
            answer=answer_text,
            raw_sources=raw_sources,
            raw_contexts=raw_contexts,
            top_k=top_k,
            note="ok",
            meta=meta,
        ),
        started,
    )


# Backwards-compatible aliases (VM2 + historical clients).
# VM2's /ask2/api/ proxy historically forwarded to VM1 /api/ask2. Keep it working.
@app.post("/api/ask2")
async def api_ask2_post(request: Request) -> JSONResponse:
    return await ask2_post(request)


@app.post("/ask2_direct")
async def ask2_direct_post(request: Request) -> JSONResponse:
    return await ask2_post(request)

@app.get("/ask2")
def ask2(q: str = Query("", alias="q"), k: int = Query(4, alias="k")) -> Dict[str, Any]:
    q = (q or "").strip()
    k = _sanitize_k(k)
    if not q:
        return {"answer": "Please provide a question (q).",
                "sources": [], "meta": {"routing": "empty", "k": k, "intent": "EMPTY", "latency_ms": 0, "show_debug_block": False}}

    if _route_ask2 is None:
        return {"answer": FALLBACK, "sources": [],
                "meta": {"routing": "fallback", "k": k, "note": "router_missing", "intent": "FALLBACK", "latency_ms": 0, "show_debug_block": False}}

    try:
        shaped = _route_ask2(q, k)
    except Exception:
        return {"answer": FALLBACK, "sources": [],
                "meta": {"routing": "error_fallback", "k": k, "intent": "FALLBACK", "latency_ms": 0, "show_debug_block": False}}

    # Contract guard: ensure non-empty answer and required keys
    if not isinstance(shaped, dict):
        shaped = {}
    ans = str((shaped.get("answer") or "")).strip()
    if not ans:
        meta = shaped.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        meta.update({"note": "nonempty_guard", "k": k})
        meta.setdefault("intent", "FALLBACK")
        meta.setdefault("latency_ms", 0)
        meta.setdefault("show_debug_block", False)
        shaped = {"answer": FALLBACK, "sources": [], "meta": meta}
    else:
        shaped.setdefault("sources", [])
        shaped.setdefault("meta", {})
        shaped["meta"].setdefault("k", k)
        shaped["meta"].setdefault("intent", "UNKNOWN")
        shaped["meta"].setdefault("latency_ms", 0)
        shaped["meta"].setdefault("show_debug_block", False)
    return shaped
