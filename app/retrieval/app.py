import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from .service import GeminiUnavailableError, RateLimitError, run_pipeline
from .settings import settings


LOGGER = logging.getLogger("ask2")
app = FastAPI()


class Answer(BaseModel):
    answer: str
    sources: list[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


def _sanitize_k(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):  # pragma: no cover - FastAPI enforces type
        parsed = 4
    parsed = max(1, parsed)
    return min(parsed, 10)


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "gemini_first": settings.gemini_first_enabled}


@app.get("/ask2", response_model=Answer)
async def ask2(request: Request, q: str = Query(""), k: int = Query(4)) -> Answer:
    question = (q or "").strip()
    client_ip = request.client.host if request.client else "unknown"
    sanitized_k = _sanitize_k(k)

    try:
        payload = run_pipeline(question, k=sanitized_k, client_ip=client_ip)
    except RateLimitError as exc:
        raise HTTPException(status_code=429, detail=exc.detail) from exc
    except GeminiUnavailableError:
        return Answer(
            answer="Gemini-first orchestration is temporarily disabled. Please retry soon.",
            sources=[],
            meta={"intent": "DISABLED", "k": sanitized_k, "show_debug_block": settings.show_debug_block},
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Gemini-first pipeline failed", exc_info=exc)
        raise HTTPException(status_code=500, detail="gemini_pipeline_failure") from exc

    answer_text = str(payload.get("answer") or "").strip()
    sources_list = payload.get("sources") or []
    if not isinstance(sources_list, list):
        sources_list = []
    meta = payload.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("k", sanitized_k)

    return Answer(answer=answer_text, sources=sources_list, meta=meta)
