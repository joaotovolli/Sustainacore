# stdlib-only Gemini router used by SustainaCore app when GEMINI_ROUTER=on
import os, json, urllib.request

DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-pro")
TIMEOUT_MS    = int(os.getenv("GEMINI_TIMEOUT_MS", "120000"))
FAIL_OPEN     = os.getenv("GEMINI_FAIL_OPEN", "1") == "1"

def _key():
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or ""

def _post(url, payload, timeout_s):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))

def draft_answer(question, snippets, config_dir):
    model = DEFAULT_MODEL
    key = _key()
    if not key:
        if FAIL_OPEN and snippets:
            s = snippets[0]
            return {"answer": (s.get("text") or s.get("chunk") or "")[:800], "citations":[str(s.get("id") or s.get("chunk_id") or "")]}
        raise RuntimeError("Missing GOOGLE_API_KEY / GEMINI_API_KEY")

    def load(path, default):
        try:
            with open(path,"r",encoding="utf-8") as f: return json.load(f)
        except Exception: return default

    router = load(os.path.join(config_dir,"router.json"), {"temperature":0.2,"max_output_tokens":512,"top_p":0.95,"top_k":40})
    facts  = load(os.path.join(config_dir,"site_facts.json"), {"system_prompt":"You are SustainaCore assistant. Output strict JSON."})

    ctx = []
    fallback = []
    for sn in snippets or []:
        sid = sn.get("id") or sn.get("chunk_id") or ""
        ttl = sn.get("title") or sn.get("source_url") or "source"
        txt = sn.get("text") or sn.get("chunk") or ""
        ctx.append(f"[{sid}] {ttl}\n{txt}\n")
        fallback.append(str(sid))
    user = f"Question: {question}\n\nContext:\n" + ("\n".join(ctx) if ctx else "No context") + """
Instructions:
- Answer concisely and factually.
- Only claim facts supported by the context or site_facts.
- Cite sources by their IDs like ["docId-1"].
- Max 8 sentences.
- Return STRICT JSON: {"answer": "...", "citations": ["id1","id2"]}.
"""
    payload = {
        "system_instruction": {"parts":[{"text": facts.get("system_prompt","")}]},
        "contents": [{"parts":[{"text": user}]}],
        "generationConfig": {
            "temperature": router.get("temperature", 0.2),
            "maxOutputTokens": router.get("max_output_tokens", 512),
            "topP": router.get("top_p", 0.95),
            "topK": router.get("top_k", 40)
        }
    }
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    timeout_s = max(1, TIMEOUT_MS // 1000)
    try:
        resp = _post(url, payload, timeout_s)
        text = ""
        try:
            text = resp["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            text = ""
        text = (text or "").strip()
        data = None
        if text.startswith("{"):
            try: data = json.loads(text)
            except Exception: data = None
        if not data:
            l = text.find("{"); r = text.rfind("}")
            if l!=-1 and r!=-1 and r>l:
                try: data = json.loads(text[l:r+1])
                except Exception: data = None
        if not data or "answer" not in data:
            raise RuntimeError("Malformed Gemini response")
        return {"answer": data.get("answer","").strip(),
                "citations": data.get("citations", fallback)}
    except Exception:
        if FAIL_OPEN and snippets:
            s = snippets[0]
            return {"answer": (s.get("text") or s.get("chunk") or "")[:800], "citations":[str(s.get("id") or s.get("chunk_id") or "")]}
        raise
