#!/usr/bin/env python3
"""Lightweight regression harness for the /ask2 persona and normalization flows."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_PATH = REPO_ROOT / "eval" / "eval.jsonl"
ASK2_URL = os.getenv("ASK2_URL", "http://127.0.0.1:8080/ask2")
LATENCY_BUDGET_SECONDS = float(os.getenv("ASK2_LATENCY_BUDGET", "5"))


class EvalFailure(Exception):
    """Raised when an evaluation case fails."""


def _load_cases() -> Iterable[Dict[str, Any]]:
    if not EVAL_PATH.exists():
        raise EvalFailure(f"Missing eval pack at {EVAL_PATH}")
    with EVAL_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _post_case(case: Dict[str, Any]) -> requests.Response:
    headers: Dict[str, str] = {}
    if case.get("content_type") == "text/plain":
        body = case.get("raw", "")
        headers["Content-Type"] = "text/plain"
        return requests.post(ASK2_URL, data=body, headers=headers, timeout=10)
    payload = case.get("payload", {})
    return requests.post(ASK2_URL, json=payload, headers=headers, timeout=10)


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
    if response.status_code >= 500:
        failures.append(f"{case_id}: received server error {response.status_code}")
    if latency >= LATENCY_BUDGET_SECONDS:
        failures.append(f"{case_id}: latency {latency:.2f}s exceeds budget {LATENCY_BUDGET_SECONDS:.2f}s")


def main() -> int:
    failures: List[str] = []
    for case in _load_cases():
        case_id = case.get("id", "unknown")
        start = time.perf_counter()
        response = _post_case(case)
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
