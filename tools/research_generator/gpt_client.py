"""GPT client wrapper for research generator."""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from . import config

LOGGER = logging.getLogger("research_generator.gpt_client")


class GPTClientError(RuntimeError):
    def __init__(self, message: str, *, status: Optional[int] = None, payload: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.status is not None:
            parts.append(f"status={self.status}")
        if self.payload:
            parts.append(f"payload_tail={self.payload[-400:]}")
        return " ".join(parts)


def _api_key() -> str:
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        raise GPTClientError("openai_api_key_missing")
    return key


def run_gpt_json(messages: list[dict[str, str]], *, timeout: float = 60.0) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "model": config.GPT_MODEL_NAME,
        "messages": messages,
        "temperature": config.GPT_TEMPERATURE,
        "response_format": {"type": "json_object"},
        "max_tokens": config.GPT_MAX_TOKENS,
    }
    req = urllib.request.Request(
        config.GPT_API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {_api_key()}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
        raise GPTClientError("gpt_http_error", status=exc.code, payload=body_text) from exc
    except urllib.error.URLError as exc:
        raise GPTClientError("gpt_network_error") from exc

    content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {"text": content}


def run_gpt_text(messages: list[dict[str, str]], *, timeout: float = 60.0) -> str:
    body: Dict[str, Any] = {
        "model": config.GPT_MODEL_NAME,
        "messages": messages,
        "temperature": config.GPT_TEMPERATURE,
        "max_tokens": config.GPT_MAX_TOKENS,
    }
    req = urllib.request.Request(
        config.GPT_API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {_api_key()}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
        raise GPTClientError("gpt_http_error", status=exc.code, payload=body_text) from exc
    except urllib.error.URLError as exc:
        raise GPTClientError("gpt_network_error") from exc

    return payload.get("choices", [{}])[0].get("message", {}).get("content", "")
