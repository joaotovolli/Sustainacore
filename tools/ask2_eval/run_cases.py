#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import importlib.util
import requests

GEMINI_CLI_PATH = REPO_ROOT / "app" / "rag" / "gemini_cli.py"
_gemini_spec = importlib.util.spec_from_file_location("gemini_cli", GEMINI_CLI_PATH)
if _gemini_spec is None or _gemini_spec.loader is None:
    raise RuntimeError("Could not load gemini_cli module.")
gemini_cli = importlib.util.module_from_spec(_gemini_spec)
_gemini_spec.loader.exec_module(gemini_cli)

QUESTIONS = [
    "Is Microsoft in the TECH100 index?",
    "How is the TECH100 index built and rebalanced?",
    "What does the AI Governance & Ethics Score measure?",
    "Show me the top companies by AI Governance & Ethics Score.",
    "Summarize the latest AI governance headlines on SustainaCore.",
    "who created sustainacore.org?",
    "who owns sustainacore.org?",
]

DEFAULT_K = 6
DEFAULT_TIMEOUT = 8


class CallTimeout(Exception):
    pass


def _with_timeout(seconds: int, func, *args, **kwargs):
    if seconds <= 0:
        return func(*args, **kwargs)

    def _handler(_signum, _frame):
        raise CallTimeout(f"timed out after {seconds}s")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def _timestamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _post_http(url: str, payload: Dict[str, Any], timeout_s: int) -> Tuple[int, Dict[str, Any]]:
    try:
        resp = requests.post(url, json=payload, timeout=timeout_s)
    except Exception:
        return 0, {}
    try:
        data = resp.json() if resp.content else {}
    except Exception:
        data = {}
    return resp.status_code, data


def _url_reachable(url: str, timeout_s: int = 1) -> bool:
    try:
        from urllib.parse import urlparse
    except Exception:
        return False
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return False
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except Exception:
        return False


def _extract_contexts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    contexts = payload.get("contexts")
    if isinstance(contexts, list):
        return [item for item in contexts if isinstance(item, dict)]
    sources = payload.get("sources")
    if isinstance(sources, list):
        return [item for item in sources if isinstance(item, dict)]
    return []


def _first_sentence(text: str, max_len: int = 240) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = " ".join(text.strip().split())
    for sep in (". ", "? ", "! "):
        if sep in collapsed:
            sentence = collapsed.split(sep, 1)[0].strip()
            break
    else:
        sentence = collapsed
    if len(sentence) > max_len:
        sentence = sentence[: max_len - 3].rstrip() + "..."
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence


def _shorten(text: str, max_len: int = 220) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = " ".join(text.strip().split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3].rstrip() + "..."


def _render_raw_retrieval(contexts: List[Dict[str, Any]]) -> str:
    if not contexts:
        return "No retrieval contexts returned."
    lines = []
    for idx, ctx in enumerate(contexts, start=1):
        title = str(ctx.get("title") or "Untitled").strip()
        url = str(ctx.get("source_url") or ctx.get("url") or "").strip()
        doc_id = str(ctx.get("id") or ctx.get("doc_id") or ctx.get("citation_id") or "").strip()
        score = ctx.get("score")
        snippet = _shorten(str(ctx.get("snippet") or ctx.get("chunk_text") or ""))
        lines.append(f"{idx}. {title}")
        if doc_id:
            lines.append(f"   - id: {doc_id}")
        if url:
            lines.append(f"   - url: {url}")
        if score is not None:
            lines.append(f"   - score: {score}")
        if snippet:
            lines.append(f"   - snippet: {snippet}")
    return "\n".join(lines).strip()


def _vector_only_answer(question: str, contexts: List[Dict[str, Any]]) -> str:
    if not contexts:
        return "\n".join(
            [
                "**Answer**",
                "I could not find enough SustainaCore context to answer this question.",
                "",
                "**Key facts**",
                "- No high-confidence facts were retrieved for this query.",
                "",
                "**Sources**",
                "1. Sources not available in the retrieved results.",
            ]
        )

    snippets = [ctx.get("snippet") or ctx.get("chunk_text") or "" for ctx in contexts]
    snippets = [s for s in snippets if isinstance(s, str) and s.strip()]
    key_facts = []
    for snippet in snippets[:5]:
        sentence = _first_sentence(snippet)
        if sentence:
            key_facts.append(sentence)

    if not key_facts:
        key_facts = ["No high-confidence facts were retrieved for this query."]

    sources = []
    for idx, ctx in enumerate(contexts[:5], start=1):
        title = str(ctx.get("title") or ctx.get("doc_id") or f"Source {idx}").strip()
        url = str(ctx.get("source_url") or ctx.get("url") or "").strip()
        if url.startswith(("local://", "file://", "internal://")) or not url:
            url = f"/sources/{title.lower().replace(' ', '-')}"
        sources.append(f"{title} — {url}")

    if not sources:
        sources = ["Sources not available in the retrieved results."]

    answer_paragraph = " ".join(_first_sentence(s) for s in snippets[:3] if s)
    if not answer_paragraph:
        answer_paragraph = "Retrieved snippets do not provide enough information for a confident answer."

    lines = []
    lines.append("**Answer**")
    lines.append(answer_paragraph)
    lines.append("")
    lines.append("**Key facts**")
    lines.extend([f"- {fact}" for fact in key_facts[:5]])
    lines.append("")
    lines.append("**Sources**")
    lines.extend([f"{idx}. {item}" for idx, item in enumerate(sources[:5], start=1)])
    return "\n".join(lines).strip()


def _is_structured(text: str) -> Tuple[bool, List[str]]:
    missing: List[str] = []
    if "**Answer**" not in text:
        missing.append("Answer heading")
    if "**Key facts**" not in text:
        missing.append("Key facts heading")
    if "**Sources**" not in text:
        missing.append("Sources heading")
    if "\n\n**Key facts**\n" not in text:
        missing.append("Blank line before Key facts")
    if "\n\n**Sources**\n" not in text:
        missing.append("Blank line before Sources")
    bullet_lines = [line for line in text.splitlines() if line.strip().startswith("- ")]
    if len(bullet_lines) < 2:
        missing.append("Bullet lines")
    if "(ID:" in text:
        missing.append("Internal IDs")
    if "..." in text:
        missing.append("Ellipses")
    answer_block = ""
    if "**Answer**" in text:
        answer_block = text.split("**Answer**", 1)[1]
    key_block = ""
    if "**Key facts**" in text:
        key_block = text.split("**Key facts**", 1)[1]
    if "•" in answer_block or "•" in key_block:
        missing.append("Unicode bullets")
    return (len(missing) == 0), missing


def _log_gemini_cmd(output_dir: Path) -> None:
    if not gemini_cli.gemini_cli_available():
        return
    try:
        gemini_cli.detect_gemini_flags()
        flag_plan = gemini_cli._get_flag_plan()  # type: ignore[attr-defined]
        binary = os.getenv("GEMINI_BIN", "gemini")
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

        cmd = [binary]
        if flag_plan.get("model_long"):
            cmd.extend([flag_plan["model_long"], model])
        elif flag_plan.get("model_short"):
            cmd.extend([flag_plan["model_short"], model])

        if flag_plan.get("json_long"):
            cmd.append(flag_plan["json_long"])
        elif flag_plan.get("json_short"):
            cmd.append(flag_plan["json_short"])

        if flag_plan.get("prompt_long"):
            cmd.extend([flag_plan["prompt_long"], "<PROMPT>"])
        elif flag_plan.get("prompt_short"):
            cmd.extend([flag_plan["prompt_short"], "<PROMPT>"])

        log_path = output_dir / "gemini_cmd.log"
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(" ".join(cmd) + "\n")
    except Exception:
        pass


def run_cases(out_root: Path, k: int, timeout_s: int, ask2_url: str, ask2_direct_url: str) -> Path:
    stamp = _timestamp()
    out_dir = out_root / stamp
    _ensure_dir(out_dir)

    summary_lines = ["# Ask2 QA Harness Summary", ""]

    gemini_available = gemini_cli.gemini_cli_available()
    _log_gemini_cmd(out_dir)
    reachable = _url_reachable(ask2_url) or _url_reachable(ask2_direct_url)

    failed_cases: List[str] = []
    for idx, question in enumerate(QUESTIONS, start=1):
        payload = {"question": question, "k": k}
        direct_payload = {}
        raw_retrieval = "endpoint unreachable"
        vector_only = "endpoint unreachable"
        if reachable:
            try:
                _, direct_payload = _with_timeout(timeout_s, _post_http, ask2_direct_url, payload, timeout_s)
                contexts = _extract_contexts(direct_payload)
                raw_retrieval = _render_raw_retrieval(contexts)
                vector_only = _vector_only_answer(question, contexts)
            except CallTimeout:
                contexts = []
                raw_retrieval = "timed out"
                vector_only = "timed out"
            except Exception:
                contexts = []
                raw_retrieval = "error"
                vector_only = "error"

        gemini_output = "not configured"
        if gemini_available and reachable:
            try:
                _, gemini_payload = _with_timeout(timeout_s, _post_http, ask2_url, payload, timeout_s)
                gemini_output = str(gemini_payload.get("answer") or "").strip() or "empty response"
            except CallTimeout:
                gemini_output = "timed out"
            except Exception:
                gemini_output = "error"

        ollama_output = "not configured"

        for label, output in (("GEMINI_FINAL", gemini_output), ("OLLAMA_FINAL", ollama_output)):
            if output in {"not configured", "timed out", "error", "empty response", "endpoint unreachable"}:
                continue
            ok, missing = _is_structured(output)
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {label} Q{idx}")
            if not ok:
                failed_cases.append(f"{label} Q{idx}: {', '.join(missing)}")

        case_path = out_dir / f"case_{idx:02d}.md"
        case_lines = [
            f"# Question: {question}",
            "",
            "## RAW_RETRIEVAL",
            raw_retrieval,
            "",
            "## VECTOR_ONLY",
            vector_only,
            "",
            "## OLLAMA_FINAL",
            ollama_output,
            "",
            "## GEMINI_FINAL",
            gemini_output,
            "",
        ]
        case_path.write_text("\n".join(case_lines).strip() + "\n", encoding="utf-8")

        summary_lines.extend(
            [
                f"## Question {idx}",
                question,
                "",
                "### RAW_RETRIEVAL",
                raw_retrieval,
                "",
                "### VECTOR_ONLY",
                vector_only,
                "",
                "### OLLAMA_FINAL",
                ollama_output,
                "",
                "### GEMINI_FINAL",
                gemini_output,
                "",
            ]
        )

    summary_path = out_dir / "summary.md"
    summary_path.write_text("\n".join(summary_lines).strip() + "\n", encoding="utf-8")
    if failed_cases:
        print("Format sanity check failed:")
        for item in failed_cases:
            print(f"- {item}")
        raise SystemExit(1)
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Ask2 evaluation cases.")
    parser.add_argument("--out-root", default="tools/ask2_eval/out", help="Output root directory.")
    parser.add_argument("--k", type=int, default=DEFAULT_K, help="Top-k contexts to retrieve.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Per-call timeout in seconds.")
    parser.add_argument("--ask2-url", default=os.getenv("ASK2_URL", "http://127.0.0.1:8080/ask2"))
    parser.add_argument("--ask2-direct-url", default=os.getenv("ASK2_DIRECT_URL", "http://127.0.0.1:8080/ask2_direct"))
    args = parser.parse_args()

    out_root = Path(args.out_root)
    out_dir = run_cases(out_root, args.k, args.timeout, args.ask2_url, args.ask2_direct_url)
    print(f"Wrote Ask2 eval outputs to {out_dir}")


if __name__ == "__main__":
    main()
