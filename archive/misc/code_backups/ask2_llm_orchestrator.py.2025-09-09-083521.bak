# ask2_llm_orchestrator.py (Ollama "micro" edition)
import io, os, re, json, time, urllib.request
from typing import List, Dict, Any

def _env(n, d=None): 
    v = os.environ.get(n)
    return v if v is not None and v != "" else d

def _post_json(url: str, payload: dict, timeout=120):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _ollama_chat(messages, *, json_mode=True):
    base = _env("OLLAMA_URL", "http://127.0.0.1:11434")
    model = _env("SCAI_OLLAMA_MODEL", "tinyllama")
    num_ctx     = int(_env("SCAI_OLLAMA_NUM_CTX", "512"))
    num_predict = int(_env("SCAI_OLLAMA_NUM_PREDICT", "64"))
    num_thread  = int(_env("SCAI_OLLAMA_NUM_THREAD", "1"))
    num_batch   = int(_env("SCAI_OLLAMA_NUM_BATCH", "8"))

    sys_content = None
    user_messages = []
    for m in messages:
        if m.get("role") == "system":
            sys_content = m["content"]
        else:
            user_messages.append(m)
    prompt_msgs = []
    if sys_content:
        prompt_msgs.append({"role":"system","content":sys_content})
    prompt_msgs.extend(user_messages)

    body = {
        "model": model,
        "messages": prompt_msgs,
        "stream": False,
        "options": {
            "num_ctx": num_ctx,
            "num_predict": num_predict,
            "num_thread": num_thread,
            "num_batch": num_batch
        }
    }
    out = _post_json(f"{base}/api/chat", body, timeout=180)
    content = out.get("message", {}).get("content","").strip()
    if json_mode:
        try: return json.loads(content)
        except Exception: return {"answer": content}
    return {"answer": content}

_MEM = {}
def _mem_key(env): return f"{env.get('HTTP_X_FORWARDED_FOR') or env.get('REMOTE_ADDR') or ''}|{env.get('HTTP_USER_AGENT') or ''}"
def _mem_get(env): 
    r = _MEM.get(_mem_key(env), {})
    if r and time.time()-r.get('ts',0) > 600: r = {}
    return r or {}
def _mem_set(env, entity=None, intent=None):
    k = _mem_key(env); r = _MEM.get(k, {"ts": time.time()})
    if entity: r["entity"] = entity
    if intent: r["intent"] = intent
    r["ts"] = time.time(); _MEM[k] = r

GREETING_RE = re.compile(r"^\s*(hi|hello|hey|hola|ol[aá]|oi|howdy|yo)[\s!?.]*$", re.I)
META_RE     = re.compile(r"(who are you|what (is|are) (this|sustaina?core)|what can you do|help|how to use)", re.I)
BRACKET_REF = re.compile(r"\[(?:S|s)\d+\]")
INLINE_NUM  = re.compile(r"\[\d+\]")

def _is_greeting(q): return bool(GREETING_RE.match(q or ""))
def _is_meta(q): return bool(META_RE.search(q or ""))

def _json_or_none(b: bytes):
    try: return json.loads(b.decode("utf-8"))
    except Exception: return None

def _dedup_sources(sources, limit=3, registry=None):
    seen, out = set(), []
    for s in sources or []:
        title, url = "", ""
        if isinstance(s, dict):
            title = (s.get("title") or s.get("name") or s.get("id") or "").strip()
            url   = (s.get("url") or s.get("link") or "").strip()
        elif isinstance(s, str):
            title = s.strip()
        if registry and title in registry:
            m = registry[title]; title = m.get("title", title); url = m.get("url", url)
        key = (title.lower(), url.lower())
        if key in seen: continue
        seen.add(key); out.append({"title": title or (url or "Source"), "url": url})
        if len(out) >= limit: break
    return out

def _extract_evidence(payload):
    ev = {"snippets": [], "sources": []}
    for k in ("snippets","chunks","evidence","fragments","items"):
        v = payload.get(k)
        if isinstance(v, list):
            for it in v:
                if isinstance(it, dict) and it.get("text"): ev["snippets"].append(str(it["text"]))
                elif isinstance(it, dict) and it.get("snippet"): ev["snippets"].append(str(it["snippet"]))
                elif isinstance(it, dict) and it.get("source"): ev["sources"].append({"title": it.get("source"), "url": it.get("url","")})
                elif isinstance(it, str): ev["snippets"].append(it)
    if isinstance(payload.get("sources"), list):
        for s in payload["sources"]:
            if isinstance(s, dict): ev["sources"].append({"title": s.get("title") or s.get("name") or "", "url": s.get("url") or ""})
            elif isinstance(s, str): ev["sources"].append({"title": s, "url": ""})
    return ev

def _prioritize_snippets(snips, intent):
    ranked = []
    for s in snips or []:
        ss = (s or "").strip()
        if not ss or ss.startswith("- [S") or BRACKET_REF.search(ss): continue
        score = 0; low = ss.lower()
        if "tech100" in low: score += 3
        if any(w in low for w in ("membership","included","constituent","part of")): score += 3
        if "rank" in low: score += 2
        if any(w in low for w in ("equal-weight","portfolio")): score += 1
        if any(w in low for w in ("founded","headquartered","company profile")): score -= 2
        ranked.append((score, ss))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in ranked[:10]]

def _strip_inline_refs(text: str) -> str:
    text = BRACKET_REF.sub("", text)
    text = INLINE_NUM.sub("", text)
    return re.sub(r"\s{2,}", " ", text).strip()

def _chat(messages, *, json_mode=True): 
    return _ollama_chat(messages, json_mode=json_mode)

class Ask2LLMOrchestratorMiddleware:
    def __init__(self, app):
        self.app = app
        self.strict = _env("SCAI_LLM_STRICT","0") == "1"
        self.style = _env("SCAI_GUIDE_STYLE","concise, human, professional; first sentence answers directly; 90-140 words")

    def _greet_meta(self, q: str) -> str:
        sys = {"role":"SustainaCore Assistant","style":self.style,"goals":[
            "Greet or briefly explain what SustainaCore/TECH100 is and how to use it.",
            "Offer ONE relevant example question.",
            "Do not include a Sources footer."
        ],"format":"Return JSON { answer: string }"}
        out = _chat([{"role":"system","content":json.dumps(sys)}, {"role":"user","content":q or "hi"}], json_mode=True)
        return out.get("answer","").strip()

    def __call__(self, environ, start_response):
        if environ.get("PATH_INFO","") != "/ask2" or environ.get("REQUEST_METHOD","GET").upper() == "OPTIONS":
            return self.app(environ, start_response)

        try: length = int(environ.get("CONTENT_LENGTH") or "0")
        except Exception: length = 0
        body = environ["wsgi.input"].read(length) if length > 0 else b"{}"
        req = _json_or_none(body) or {}
        question = (req.get("question") or req.get("q") or "").strip()

        if not question:
            start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
            return [json.dumps({"answer":"", "sources":[]}).encode("utf-8")]

        if _is_greeting(question) or _is_meta(question):
            try: ans = self._greet_meta(question)
            except Exception: ans = "" if self.strict else ""
            start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
            return [json.dumps({"answer": ans, "sources": []}).encode("utf-8")]

        captured_body = []
        def cap(status, headers, exc_info=None): return captured_body.append
        inner_env = environ.copy()
        j = json.dumps(req).encode("utf-8")
        inner_env["wsgi.input"] = io.BytesIO(j)
        inner_env["CONTENT_LENGTH"] = str(len(j))
        try:
            inner_iter = self.app(inner_env, cap)
            for chunk in inner_iter: captured_body.append(chunk)
            if hasattr(inner_iter,"close"): inner_iter.close()
            payload = _json_or_none(b"".join(captured_body)) or {}
        except Exception:
            payload = {}

        ev = _extract_evidence(payload)
        curated = _prioritize_snippets(ev.get("snippets",[]), "general")
        citations = _dedup_sources(payload.get("sources",[]) if isinstance(payload, dict) else ev.get("sources",[]), limit=3)

        sys1 = {"role":"Write a unified answer using ONLY the provided evidence; no hallucinations; no inline refs.","style":self.style,"format":"Return JSON { answer: string }"}
        draft = _chat([{"role":"system","content":json.dumps(sys1)},
                       {"role":"user","content":json.dumps({"question":question,"evidence_snippets":curated})}], json_mode=True)
        sys2 = {"role":"Improve tone/clarity; keep human; remove repetition; no inline refs.","style":self.style,"format":"Return JSON { answer: string }"}
        improved = _chat([{"role":"system","content":json.dumps(sys2)},
                          {"role":"user","content":json.dumps({"question":question,"draft":draft})}], json_mode=True)
        sys3 = {"role":"Validate against evidence; compress to ~120 words; prep for Sources footer.","style":self.style,"format":"Return JSON { answer: string }"}
        final = _chat([{"role":"system","content":json.dumps(sys3)},
                       {"role":"user","content":json.dumps({"question":question,"answer":improved,"evidence_snippets":curated})}], json_mode=True)

        answer = _strip_inline_refs(final.get("answer",""))
        if citations and answer:
            answer += "\n\nSources:\n" + "\n".join("• " + c["title"] + (f" — {c['url']}" if c.get("url") else "") for c in citations)

        start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
        return [json.dumps({"answer": answer, "sources": citations}).encode("utf-8")]
