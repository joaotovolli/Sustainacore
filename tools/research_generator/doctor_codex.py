"""Codex CLI doctor for research generator."""
from __future__ import annotations

import logging
import shutil

from .codex_cli_runner import CodexCLIError, call_codex

LOGGER = logging.getLogger("research_generator.doctor_codex")


def run_doctor() -> bool:
    if not shutil.which("npx"):
        LOGGER.error("codex_not_found")
        return False
    prompt = (
        "Return JSON only: {\"ok\": true}. "
        "Do not run tools or commands."
    )
    try:
        payload = call_codex(prompt, purpose="doctor", expect_json=True)
    except CodexCLIError as exc:
        LOGGER.error("codex_doctor_failed=%s", exc)
        return False
    return bool(payload.get("ok"))


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    ok = run_doctor()
    if ok:
        print("PASS codex_cli_ready")
        return 0
    print("FAIL codex_cli_unavailable")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
