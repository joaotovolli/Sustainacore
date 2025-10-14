#!/usr/bin/env python3
"""Lightweight regression harness for the /ask2 persona and normalization flows."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_PATH = REPO_ROOT / "eval" / "eval.jsonl"
FLAG_ENV = os.getenv("EVAL_FLAGS", "")
FLAG_LIST = [flag.strip() for flag in FLAG_ENV.split(",") if flag.strip()]
LATENCY_BUDGET_SECONDS = float(os.getenv("ASK2_LATENCY_BUDGET", "5"))


class EvalFailure(Exception):
    """Raised when an evaluation case fails."""


class EvalSkip(Exception):
    """Raised when evaluation should be skipped without error."""


def _load_cases() -> Iterable[Dict[str, Any]]:
    if not EVAL_PATH.exists():
        raise EvalFailure(f"Missing eval pack at {EVAL_PATH}")
    with EVAL_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _canonicalize_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the payload uses canonical ask2 field names."""

    payload: Dict[str, Any] = dict(raw_payload)

    question_value = ""
    for key in ("question", "query", "q", "text"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            question_value = value.strip()
            break
    if question_value:
        payload["question"] = question_value

    top_k_value: Any = None
    for key in ("top_k", "topK", "topk", "k", "limit"):
        if key in payload:
            top_k_value = payload[key]
            break
    if top_k_value is not None:
        try:
            top_k = int(top_k_value)
        except Exception:
            top_k = None
        if top_k is not None:
            if top_k < 1:
                top_k = 1
            elif top_k > 10:
                top_k = 10
            payload["top_k"] = top_k

    return payload


def _post_case(case: Dict[str, Any], ask2_url: str) -> requests.Response:
    headers: Dict[str, str] = {}
    if FLAG_LIST:
        headers["X-Eval-Flags"] = ",".join(FLAG_LIST)
    if case.get("content_type") == "text/plain":
        body = case.get("raw", "")
        headers["Content-Type"] = "text/plain"
        return requests.post(ask2_url, data=body, headers=headers, timeout=15)
    payload = case.get("payload", {})
    if isinstance(payload, dict):
        payload = _canonicalize_payload(payload)
    return requests.post(ask2_url, json=payload, headers=headers, timeout=15)


def _validate_core(case_id: str, body: Dict[str, Any], latency: float, failures: List[str]) -> None:
    answer = (body.get("answer") or "").strip()
    contexts = body.get("contexts") or []
    sources = body.get("sources") or []

    if not answer:
        failures.append(f"{case_id}: answer is empty")
    if "Sources:" not in answer:
        failures.append(f"{case_id}: answer missing Sources section")
    if not isinstance(contexts, list) or len(contexts) < 3:
        failures.append(f"{case_id}: expected at least 3 contexts, got {len(contexts) if isinstance(contexts, list) else 'non-list'}")
    if not isinstance(sources, list) or not sources:
        failures.append(f"{case_id}: sources list missing or empty")
    if latency >= LATENCY_BUDGET_SECONDS:
        failures.append(f"{case_id}: latency {latency:.2f}s exceeds budget {LATENCY_BUDGET_SECONDS:.2f}s")


def _validate_malformed(case_id: str, response: requests.Response, latency: float, failures: List[str]) -> None:
    if response.status_code != 200:
        failures.append(f"{case_id}: expected 200, got {response.status_code}")
        return
    try:
        body = response.json()
    except ValueError:
        failures.append(f"{case_id}: malformed response body")
        return
    if not isinstance(body.get("contexts"), list):
        failures.append(f"{case_id}: contexts should be a list")
    if "answer" not in body:
        failures.append(f"{case_id}: missing answer field")
    if "sources" not in body:
        failures.append(f"{case_id}: missing sources field")
    if latency >= LATENCY_BUDGET_SECONDS:
        failures.append(f"{case_id}: latency {latency:.2f}s exceeds budget {LATENCY_BUDGET_SECONDS:.2f}s")


def _is_absolute_url(candidate: str) -> bool:
    parsed = urlparse(candidate)
    return bool(parsed.scheme) and bool(parsed.netloc)


def _resolve_url(cli_url: str | None) -> str:
    env_url = os.getenv("ASK2_URL", "").strip()
    if env_url:
        if not _is_absolute_url(env_url):
            raise EvalSkip("SKIP: ASK2_URL not set to an absolute URL; skipping network eval.")
        return env_url

    if cli_url:
        candidate = cli_url.strip()
        if not candidate:
            raise EvalFailure("Provided --url value is empty; supply an absolute URL.")
        if not _is_absolute_url(candidate):
            raise EvalFailure(
                "The --url argument must be an absolute URL, e.g., https://example.org/ask2."
            )
        return candidate

    raise EvalSkip("SKIP: ASK2_URL not set; skipping network eval.")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run SustainaCore ask2 eval pack")
    parser.add_argument("--url", help="Absolute /ask2 URL to target", default=None)
    args = parser.parse_args(argv)

    try:
        ask2_url = _resolve_url(args.url)
    except EvalSkip as exc:
        print(str(exc))
        return 0
    except EvalFailure as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(f"Running eval against {ask2_url}")

    try:
        cases = list(_load_cases())
    except EvalFailure as exc:
        print(str(exc), file=sys.stderr)
        return 1

    failures: List[str] = []
    for case in cases:
        case_id = case.get("id", "unknown")
        start = time.perf_counter()
        try:
            response = _post_case(case, ask2_url)
        except requests.RequestException as exc:
            failures.append(f"{case_id}: request failed ({exc})")
            continue
        latency = time.perf_counter() - start

        try:
            body = response.json()
        except ValueError:
            failures.append(f"{case_id}: response is not valid JSON (status {response.status_code})")
            continue

        case_type = case.get("type")
        if case_type == "core":
            if response.status_code != 200:
                failures.append(f"{case_id}: expected 200, got {response.status_code}")
                continue
            _validate_core(case_id, body, latency, failures)
            if not body.get("sources"):
                failures.append(f"{case_id}: missing sources array")
        elif case_type == "malformed":
            _validate_malformed(case_id, response, latency, failures)
        else:
            failures.append(f"{case_id}: unknown case type {case_type}")

    if failures:
        print("Eval failures detected:", file=sys.stderr)
        for failure in failures:
            print(f" - {failure}", file=sys.stderr)
        return 1

    print("All eval cases passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
