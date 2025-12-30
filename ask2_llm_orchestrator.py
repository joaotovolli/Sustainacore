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

def _strip_section_markers(text: str) -> str:
    if not isinstance(text, str):
        return ""
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        lowered = line.lower().strip("*").strip()
        if lowered in {"answer", "key facts (from sustainacore)", "key facts", "evidence", "sources"}:
            continue
        if lowered.startswith("key facts"):
            continue
        if lowered.startswith("evidence"):
            continue
        lines.append(raw)
    return "\n".join(lines).strip()

def _normalize_bullet_runs(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    if text.count(" - ") >= 2:
        return text.replace(" - ", "\n- ")
    return text

def _split_paragraphs(text: str):
    if not isinstance(text, str) or not text.strip():
        return []
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return paragraphs or [text.strip()]

def _first_sentence(text: str, max_len=240):
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", collapsed)
    sentence = parts[0].strip() if parts else collapsed
    if len(sentence) > max_len:
        sentence = sentence[: max_len - 3].rstrip() + "..."
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence

def _shorten_snippet(text: str, max_len=220):
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3].rstrip() + "..."

def _build_key_facts_from_snippets(snips, max_bullets=5):
    bullets = []
    seen = set()
    for snippet in snips or []:
        sentence = _first_sentence(snippet)
        if not sentence:
            continue
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        bullets.append(sentence)
        if len(bullets) >= max_bullets:
            break
    return bullets

def _build_evidence(snips, citations, max_bullets=5):
    bullets = []
    for idx, snippet in enumerate(snips or []):
        if len(bullets) >= max_bullets:
            break
        short = _shorten_snippet(snippet)
        if not short:
            continue
        label = "Source"
        if citations and idx < len(citations):
            label = citations[idx].get("title") or citations[idx].get("url") or "Source"
        bullets.append(f'{label}: "{short}"')
    return bullets

def _format_structured_answer(answer_text, snips, citations):
    cleaned = _normalize_bullet_runs(_strip_section_markers(_strip_inline_refs(answer_text or "")))
    paragraphs = _split_paragraphs(cleaned)
    if not paragraphs:
        paragraphs = []
    if not paragraphs and snips:
        paragraphs = [" ".join(_build_key_facts_from_snippets(snips, max_bullets=3))]
    if not paragraphs:
        paragraphs = ["I could not find enough SustainaCore context to answer this question."]

    if len(snips or []) < 2:
        paragraphs.append("Coverage looks thin based on the retrieved snippets.")

    key_facts = _build_key_facts_from_snippets(snips)
    if not key_facts:
        key_facts = ["No high-confidence facts were retrieved for this query."]

    evidence = _build_evidence(snips, citations)
    if not evidence:
        evidence = ["No evidence snippets were returned by the retriever."]

    answer_lines = ["**Answer**"] + paragraphs
    key_fact_lines = ["", "**Key facts (from SustainaCore)**"] + [f"- {item}" for item in key_facts[:5]]
    evidence_lines = ["", "**Evidence**"] + [f"- {item}" for item in evidence[:5]]

    cap = int(os.getenv("ASK2_ANSWER_CHAR_CAP", "1200"))
    if cap > 0:
        evidence_text = "\n".join(evidence_lines).strip()
        remaining = max(cap - len(evidence_text) - 2, 0)
        content = "\n".join(answer_lines + key_fact_lines).strip()
        if remaining and len(content) > remaining:
            content = content[:remaining].rstrip()
        combined = "\n".join([content, evidence_text]).strip()
    else:
        combined = "\n".join(answer_lines + key_fact_lines + evidence_lines).strip()
    return combined

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
            try:
                ans = self._greet_meta(question)
            except Exception:
                ans = "" if self.strict else ""
            start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
            return [json.dumps({"answer": ans, "sources": []}).encode("utf-8")]

        captured = {"status": "200 OK", "headers": []}
        captured_body = []

        def cap(status, headers, exc_info=None):
            captured["status"] = status
            captured["headers"] = headers

            def _write(data):
                if isinstance(data, bytes):
                    captured_body.append(data)
                else:
                    captured_body.append(str(data).encode("utf-8"))

            return _write

        inner_env = environ.copy()
        j = json.dumps(req).encode("utf-8")
        inner_env["wsgi.input"] = io.BytesIO(j)
        inner_env["CONTENT_LENGTH"] = str(len(j))
        try:
            inner_iter = self.app(inner_env, cap)
            for chunk in inner_iter:
                if isinstance(chunk, bytes):
                    captured_body.append(chunk)
                else:
                    captured_body.append(str(chunk).encode("utf-8"))
            if hasattr(inner_iter,"close"):
                inner_iter.close()
            body_bytes = b"".join(captured_body)
            payload = _json_or_none(body_bytes) or {}
        except Exception:
            body_bytes = b""
            payload = {}

        status = captured.get("status", "200 OK")
        headers = captured.get("headers") or []

        if not status.startswith("200"):
            start_response(status, headers or [("Content-Type","application/json; charset=utf-8")])
            return [body_bytes]

        meta = payload.get("meta") if isinstance(payload, dict) else None
        if isinstance(meta, dict) and meta.get("gemini_first"):
            shaped = {
                "answer": str(payload.get("answer") or ""),
                "sources": payload.get("sources") if isinstance(payload.get("sources"), list) else [],
                "meta": meta,
            }
            start_response(status, [("Content-Type","application/json; charset=utf-8")])
            return [json.dumps(shaped, ensure_ascii=False).encode("utf-8")]

        ev = _extract_evidence(payload)
        curated = _prioritize_snippets(ev.get("snippets",[]), "general")
        citations = _dedup_sources(payload.get("sources",[]) if isinstance(payload, dict) else ev.get("sources",[]), limit=3)

        sys1 = {
            "role":"Write a unified answer using ONLY the provided evidence; no hallucinations; no inline refs.",
            "style":self.style,
            "format":"Return JSON { answer: string }"
        }
        draft = _chat([{"role":"system","content":json.dumps(sys1)},
                       {"role":"user","content":json.dumps({"question":question,"evidence_snippets":curated})}], json_mode=True)
        sys2 = {
            "role":"Improve tone/clarity; keep human; remove repetition; no inline refs.",
            "style":self.style,
            "format":"Return JSON { answer: string }"
        }
        improved = _chat([{"role":"system","content":json.dumps(sys2)},
                          {"role":"user","content":json.dumps({"question":question,"draft":draft})}], json_mode=True)
        sys3 = {
            "role":"Validate against evidence; compress to ~120 words; format for final response.",
            "style":self.style,
            "format":"Return JSON { answer: string }",
            "output_format":[
                "Title line (optional)",
                "",
                "**Answer**",
                "1-3 short paragraphs",
                "",
                "**Key facts (from SustainaCore)**",
                "- max 5 bullets, one sentence each",
                "",
                "**Evidence**",
                "- 2-5 bullets with short snippets"
            ]
        }
        final = _chat([{"role":"system","content":json.dumps(sys3)},
                       {"role":"user","content":json.dumps({"question":question,"answer":improved,"evidence_snippets":curated})}], json_mode=True)

        answer = _format_structured_answer(final.get("answer",""), curated, citations)

        start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
        return [json.dumps({"answer": answer, "sources": citations}).encode("utf-8")]
