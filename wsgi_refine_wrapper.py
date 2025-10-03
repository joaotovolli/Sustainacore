import os, json, time, requests
from app import app as downstream

# Feature flag & knobs (all env-controlled)
REFINE = os.environ.get("REFINE","off").lower() == "on"
OLLAMA_URL = os.environ.get("OLLAMA_URL","http://127.0.0.1:11434")
TAILOR_MODEL = os.environ.get("TAILOR_MODEL","mistral:instruct")
MIN_PASSES = int(os.environ.get("SELF_REFINE_MIN","3") or 3)
MAX_PASSES = int(os.environ.get("SELF_REFINE_MAX","6") or 6)
BUDGET_MS  = int(os.environ.get("TAILOR_BUDGET_MS","9000") or 9000)
CONN_TO    = float(os.environ.get("CONNECT_TIMEOUT","1.5") or 1.5)
READ_TO    = float(os.environ.get("READ_TIMEOUT","6") or 6)

def _display_sources(ctxs, cap=5):
    seen=set(); out=[]
    for c in (ctxs or []):
        key=(c.get("title") or "")+"|"+(c.get("source_url") or c.get("url") or "")
        if key in seen: continue
        seen.add(key)
        out.append({"title": c.get("title") or c.get("source_url") or c.get("url") or "source"})
        if len(out)>=cap: break
    return out

def _ctx_titles(ctxs, cap=10):
    ts=[(c.get("title") or "").strip() for c in (ctxs or [])]
    ts=[t for t in ts if t][:cap]
    return "- " + "\n- ".join(ts) if ts else "(no titles)"
def _ollama(prompt, num_predict=512):
    """
    Gemini-first generator with Ollama fallback.
    Keeps signature for compatibility with existing callers.
    """
    import os, subprocess, json, textwrap, requests

    model = os.environ.get("LLM_MODEL", "gemini-1.5-pro")
    gem_key = os.environ.get("GEMINI_API_KEY")  # systemd loads from /etc/sustainacore/gemini.env
    ollama_url  = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    tailor_model= os.environ.get("TAILOR_MODEL", os.environ.get("LLM_MODEL", "mistral:instruct"))

    # --- Try Gemini CLI (non-interactive) ---
    if gem_key and model.startswith("gemini"):
        try:
            # We use stdin for the full prompt; -p is appended; keep -p minimal to avoid duplication.
            cp = subprocess.run(
                ["gemini", "-m", model, "-p", "Answer clearly based on the input. If unsure, say so."],
                input=prompt,
                text=True,
                capture_output=True,
                timeout=int(os.environ.get("LLM_READ_TIMEOUT", "20")),
                env=dict(os.environ, GEMINI_API_KEY=gem_key),
            )
            out = (cp.stdout or "").strip()
            if cp.returncode == 0 and out:
                return out
        except Exception:
            pass  # fall back to Ollama
    # --- Fallback: Ollama /api/generate (existing behavior) ---
    try:
        r = requests.post(
            f"{ollama_url}/api/generate",
            json={"model": tailor_model, "prompt": prompt, "stream": False},
            timeout=(int(os.environ.get("LLM_CONNECT_TIMEOUT", "2")), int(os.environ.get("LLM_READ_TIMEOUT", "8")))
        )
        r.raise_for_status()
        data = r.json()
        # Ollama returns {"response": "..."} in non-stream mode
        txt = (data.get("response") or "").strip()
        if txt:
            return txt
        # some builds return a flat string in older versions
        if isinstance(data, str) and data.strip():
            return data.strip()
        raise RuntimeError("ollama returned empty response")
    except Exception as e:
        # Final safety: return a terse string so the app never crashes
        return f"[generator unavailable] {str(e)[:120]}"

def _tailor(answer, ctx_titles):
    start = time.time()
    def budget_ok(): return (time.time() - start)*1000 < BUDGET_MS
    out = (answer or "").strip()
    passes = 0

    if len(out) < 400 and budget_ok():
        t = _ollama(
            "ROLE: DRAFTER (Grounded rewrite)\n"
            f"CONTEXT TITLES:\n{ctx_titles}\n\n"
            f"ANSWER:\n{out}\n\n"
            "Rules: Answer-first sentence; 3–6 short bullets; "
            "end with 'Why this matters: ...'; no new facts; no URLs."
        )
        if t: out = t; passes += 1

    if budget_ok():
        t = _ollama(
            "ROLE: VERIFIER\n"
            "Given context titles, check the answer. If fully faithful, "
            "reply exactly <APPROVE>. Otherwise, return a corrected answer "
            "with the same structure.\n\n"
            f"CONTEXT TITLES:\n{ctx_titles}\n\nANSWER:\n{out}"
        )
        if t and t.strip() != "<APPROVE>":
            out = t.strip()
        passes += 1

    if budget_ok():
        t = _ollama(
            "ROLE: POLISHER\n"
            "Rewrite for clarity and brevity; keep facts; no new facts.\n\n"
            f"ANSWER:\n{out}"
        )
        if t and t.strip() not in ("<NOCHANGE>", out):
            out = t.strip()
        passes += 1

    while passes < MAX_PASSES and budget_ok():
        t = _ollama(
            "ROLE: REFINER\n"
            "Improve flow without adding facts. If no improvement, return <NOCHANGE>.\n\n"
            f"ANSWER:\n{out}"
        )
        passes += 1
        if not t or t.strip() in ("<NOCHANGE>", out):
            if passes >= MIN_PASSES: break
            else: continue
        out = t.strip()
        if len(out) > 650 and passes >= MIN_PASSES:
            break

    return out

def application(environ, start_response):
    # Call the real app first
    captured = {}
    def cap_start(status, headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = headers
        return lambda x: None

    body = b"".join(downstream(environ, cap_start))
    status = captured.get("status","200 OK")
    headers = captured.get("headers",[])

    # Only touch POST /ask2 JSON 200 responses when REFINE=on
    path_ok   = environ.get("PATH_INFO","") in ("/ask2",)
    method_ok = environ.get("REQUEST_METHOD","") == "POST"
    if not (REFINE and path_ok and method_ok and status.startswith("200")):
        start_response(status, headers); return [body]

    try:
        ctype = next((v for (k,v) in headers if k.lower()=="content-type"),"application/json")
        if "json" not in ctype.lower():
            start_response(status, headers); return [body]

        payload = json.loads(body.decode("utf-8","ignore") or "{}")
        ans = (payload.get("answer") or "").strip()
        ctxs = payload.get("contexts") or payload.get("context") or []
        if not ans:
            start_response(status, headers); return [body]

        refined = _tailor(ans, _ctx_titles(ctxs))
        if refined:
            payload["answer"] = refined
            payload["contexts"] = _display_sources(ctxs)

            new_body = json.dumps(payload).encode("utf-8")
            # fix Content-Length
            new_headers = [(k,v) for (k,v) in headers if k.lower()!="content-length"]
            new_headers.append(("Content-Length", str(len(new_body))))
            start_response(status, new_headers)
            return [new_body]
    except Exception:
        pass  # on any error, fall back to the original response

    start_response(status, headers)
    return [body]

# Gunicorn entry point
app = application
