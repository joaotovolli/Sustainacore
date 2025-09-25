import os, time, requests

# Feature flags / knobs
REFINE       = os.getenv("REFINE", "off").lower() == "on"
OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
TAILOR_MODEL = os.getenv("TAILOR_MODEL", os.getenv("LLM_MODEL", "mistral:instruct"))

MIN_PASSES = int(os.getenv("SELF_REFINE_MIN", "3") or 3)
MAX_PASSES = int(os.getenv("SELF_REFINE_MAX", "10") or 10)
BUDGET_MS  = int(os.getenv("TAILOR_BUDGET_MS", "7000") or 7000)
CONN_TO    = float(os.getenv("CONNECT_TIMEOUT", "1.0") or 1.0)
READ_TO    = float(os.getenv("READ_TIMEOUT", "4.0") or 4.0)

def _ctx_titles(ctxs, cap=10):
    if not ctxs: return "(no titles)"
    ts=[]
    for c in ctxs:
        t = (c.get("title") or c.get("source_title") or "").strip()
        if t:
            ts.append(t)
            if len(ts) >= cap: break
    return "- " + "\n- ".join(ts) if ts else "(no titles)"

def _ollama(prompt, num_predict=256):
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": TAILOR_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": num_predict},
            },
            timeout=(CONN_TO, READ_TO),
        )
        r.raise_for_status()
        return (r.json().get("response") or "").strip()
    except Exception:
        return ""

def _rewrite(answer, titles):
    prompt = (
        "ROLE: DRAFTER (polish, no new facts)\n"
        f"CONTEXT TITLES:\n{titles}\n\n"
        "Rewrite the ANSWER to be concise and structured: first sentence answers directly; "
        "then 3–6 bullets with concrete specifics; end with 'Why this matters: ...'. "
        "Do not invent facts. Do not add URLs.\n\n"
        f"ANSWER:\n{answer}\n"
    )
    return _ollama(prompt)

def _verify(answer, titles):
    prompt = (
        "ROLE: VERIFIER\n"
        "Given the CONTEXT TITLES, if the ANSWER is faithful and well-structured, reply exactly <APPROVE>.\n"
        "Otherwise, return a corrected ANSWER with the same structure.\n\n"
        f"CONTEXT TITLES:\n{titles}\n\nANSWER:\n{answer}\n"
    )
    out = _ollama(prompt)
    if not out:
        return answer
    return answer if out.strip() == "<APPROVE>" else out.strip()

def refine_maybe(answer: str, contexts):
    """Return refined answer if REFINE=on; otherwise return original."""
    if not REFINE:
        return answer
    if not isinstance(answer, str) or not answer.strip():
        return answer

    titles = _ctx_titles(contexts)
    out = answer.strip()
    passes = 0
    start = time.time()
    def budget_ok(): return (time.time() - start)*1000 < BUDGET_MS

    target = max(MIN_PASSES, 0)
    maxp   = max(MAX_PASSES, target)

    while passes < maxp and budget_ok():
        if passes % 2 == 0:
            t = _rewrite(out, titles)
            out = t or out
        else:
            out = _verify(out, titles)
        passes += 1
        if passes >= target and not budget_ok():
            break
    return out
