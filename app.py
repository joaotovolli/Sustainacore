
# SustainaCore app.py — SMART v2
import importlib
import importlib.util
import json
import logging
import os
import re
import time
from pathlib import Path

# Allow this module to expose the sibling package under app/
__path__ = [str(Path(__file__).resolve().parent / "app")]

import requests
from collections import defaultdict
from flask import Flask, request, jsonify
try:
    from smalltalk import smalltalk_response
except Exception:
    smalltalk_response = lambda _q: None
try:
    from db_helper import top_k_by_vector as _top_k_by_vector
except Exception:
    _top_k_by_vector = None

try:
    from gemini_adapter import generate as _gemini_generate  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _gemini_generate = None

def _load_ask2_routing_module():
    """Best-effort loader for ``app.rag.routing`` despite the module/package clash."""

    try:
        return importlib.import_module("app.rag.routing")
    except ModuleNotFoundError as exc:
        # When ``app`` refers to this module, importing ``app.rag`` fails. Fall back to
        # loading from the package path manually. Re-raise for unrelated missing deps.
        if exc.name not in {"app", "app.rag", "app.rag.routing"}:
            raise
    except Exception:
        return None

    pkg_root = Path(__file__).resolve().parent / "app"
    candidate = pkg_root / "rag" / "routing.py"
    if not candidate.exists():
        return None

    spec = importlib.util.spec_from_file_location("app_rag_routing", candidate)
    if spec is None or spec.loader is None:
        return None

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[arg-type]
    except Exception:
        return None
    return module


_ask2_routing = _load_ask2_routing_module()

try:  # Gemini-first shared service (optional)
    from app.retrieval.service import (
        GeminiUnavailableError as _GeminiUnavailableError,
        RateLimitError as _GeminiRateLimitError,
        run_pipeline as _gemini_run_pipeline,
    )
    from app.retrieval.settings import settings as _gemini_settings
except Exception:  # pragma: no cover - optional dependency
    class _GeminiRateLimitError(Exception):  # type: ignore[no-redef]
        detail = "rate_limited"

    class _GeminiUnavailableError(Exception):  # type: ignore[no-redef]
        pass

    _gemini_run_pipeline = None
    _gemini_settings = None


def _sanitize_meta_k(value, default=4):
    """Best-effort coercion of the ``k`` parameter for /ask2."""

    try:
        k_val = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        k_val = default
    if k_val < 1:
        k_val = 1
    if k_val > 10:
        k_val = 10
    return k_val


if _ask2_routing is not None:
    _route_ask2 = getattr(_ask2_routing, "route_ask2", None)
    _sanitize_meta_k = getattr(_ask2_routing, "_sanitize_k", _sanitize_meta_k)
    _ASK2_ROUTER_FALLBACK = getattr(
        _ask2_routing,
        "NO_HIT_FALLBACK",
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look.",
    )
else:  # pragma: no cover - import fallback
    _route_ask2 = None
    _ASK2_ROUTER_FALLBACK = (
        "I couldn’t find Sustainacore documents for that yet. If you can share the "
        "organization or company name, the ESG or TECH100 topic, and the report or "
        "year you care about, I can take another look."
    )


_LOGGER = logging.getLogger("app.ask2")


def _call_route_ask2_facade(question: str, k_value, *, client_ip: str | None = None):
    """Invoke the smart router with graceful fallbacks.

    This mirrors the WSGI facade logic so running ``app.py`` directly (e.g. via
    ``flask --app app run``) behaves the same as the production entrypoint.
    """

    sanitized_k = _sanitize_meta_k(k_value)
    question_text = (question or "").strip()

    if (
        _gemini_run_pipeline is not None
        and _gemini_settings is not None
        and _gemini_settings.gemini_first_enabled
    ):
        try:
            payload = _gemini_run_pipeline(
                question_text, k=sanitized_k, client_ip=client_ip or "unknown"
            )
            meta = payload.get("meta") if isinstance(payload, dict) else None
            if not isinstance(meta, dict):
                meta = {}
            meta.setdefault("k", sanitized_k)
            meta.setdefault("routing", "gemini_first")
            if meta.get("intent") == "SMALL_TALK":
                meta["routing"] = "smalltalk"
            payload = {
                "answer": str(payload.get("answer") or ""),
                "sources": payload.get("sources") or [],
                "meta": meta,
            }
            return payload, 200
        except _GeminiRateLimitError as exc:  # type: ignore[arg-type]
            return (
                {
                    "answer": "You’ve hit the current rate limit. Please retry in a few seconds.",
                    "sources": [],
                    "meta": {
                        "error": getattr(exc, "detail", "rate_limited"),
                        "intent": "RATE_LIMIT",
                        "k": sanitized_k,
                        "routing": "gemini_first",
                        "show_debug_block": False,
                    },
                },
                429,
            )
        except _GeminiUnavailableError:  # type: ignore[arg-type]
            pass  # fall back to legacy router
        except Exception as exc:  # pragma: no cover - defensive logging
            _LOGGER.exception("Gemini-first pipeline failed; using legacy router", exc_info=exc)

    if callable(_route_ask2):
        try:
            shaped = _route_ask2(question_text, sanitized_k)
            if isinstance(shaped, dict):
                meta = shaped.get("meta")
                if isinstance(meta, dict):
                    meta.setdefault("k", sanitized_k)
                else:
                    shaped["meta"] = {"k": sanitized_k}
                return shaped, 200
        except Exception as exc:  # pragma: no cover - defensive logging
            _LOGGER.exception("Legacy /ask2 router failed", exc_info=exc)

    fallback = {
        "answer": _ASK2_ROUTER_FALLBACK,
        "sources": [],
        "meta": {
            "routing": "router_unavailable",
            "top_score": None,
            "gemini_used": False,
            "k": sanitized_k,
            "error": "router_unavailable",
        },
    }
    return fallback, 200

EMBED_DIM = int(os.getenv("EMBED_DIM", "384"))
OLLAMA = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "all-minilm")

FUSION_TOPK_BASE = int(os.getenv("FUSION_TOPK_BASE", "8"))
FUSION_TOPK_MAX  = int(os.getenv("FUSION_TOPK_MAX", "24"))
RRF_K            = int(os.getenv("RRF_K", "60"))
MMR_LAMBDA       = float(os.getenv("MMR_LAMBDA", "0.7"))
DOC_CAP          = int(os.getenv("DOC_CAP", "3"))
CHUNKS_MAX       = int(os.getenv("CHUNKS_MAX", "12"))
CITES_MAX        = int(os.getenv("CITES_MAX", "6"))
LATENCY_BUDGET_MS= int(os.getenv("LATENCY_BUDGET_MS", "1200"))
RETURN_TOP_AS_ANSWER = os.getenv("RETURN_TOP_AS_ANSWER","1") == "1"

app = Flask(__name__)

def embed(text: str):
    text = (text or "").strip()
    if not text: return [0.0]*EMBED_DIM
    url = f"{OLLAMA}/api/embeddings"
    resp = requests.post(url, json={"model": OLLAMA_EMBED_MODEL, "prompt": text}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    vec = data.get("embedding") or (data.get("data") or [{}])[0].get("embedding")
    if not isinstance(vec, list): return [0.0]*EMBED_DIM
    if len(vec) > EMBED_DIM: vec = vec[:EMBED_DIM]
    if len(vec) < EMBED_DIM: vec = vec + [0.0]*(EMBED_DIM-len(vec))
    return vec

META_KEYS = ("ROLE:", "TASK:", "PREVIOUS ANSWER", "QUESTION TYPE", "CONTEXT TITLES:", "buttons to click", "What would you like to explore?")
SOURCE_TAG_RE = re.compile(r"\[(?:S|s)\d+\]")

def normalize_question(q: str):
    q0 = (q or "").strip()
    changed = False
    if any(k in q0 for k in META_KEYS):
        changed = True
        m = re.findall(r'([A-Z][^?]{3,}\?)', q0, flags=re.S)
        if m:
            q1 = m[-1].strip()
        else:
            q1 = re.sub(r'(?mi)^(ROLE|TASK|PREVIOUS ANSWER|QUESTION TYPE|CONTEXT TITLES).*$','',q0)
            q1 = re.sub(r'buttons to click.*','',q1, flags=re.I)
            q1 = re.sub(r'\s+',' ', q1).strip()
        q0 = q1 or "help"
    return q0, changed

ALIAS = {
    "tech 100": "TECH100", "tech-100": "TECH100", "ai governance & ethics index": "TECH100",
    "msft":"Microsoft","csco":"Cisco","aapl":"Apple","googl":"Alphabet","goog":"Alphabet","meta":"Meta",
    "google":"Alphabet","microsoft":"Microsoft","cisco":"Cisco","apple":"Apple","alphabet":"Alphabet","ibm":"IBM",
}

def detect_intent(q: str, ents):
    ql = q.lower()
    if re.search(r'\b(rank|ranking)\b', ql): return "rank"
    if re.search(r'\bmember(ship)?\b|\bpart of\b', ql): return "membership"
    if re.search(r'\bcompare|vs\.|versus|difference\b', ql): return "comparison"
    if re.search(r'\btrend|over time|history\b', ql): return "trend"
    if re.search(r'\bhow\b|\bwhy\b|\bsteps?\b', ql): return "howwhy"
    if re.search(r'\bpolicy|regulat|law|directive|act\b', ql): return "policy"
    if len(q.strip())<=2 and ents: return "overview"
    if (len(ents)==1) and (len(q.split())<=4): return "overview"
    if re.search(r'what is this website|what information is available', ql): return "about"
    return "general"

def extract_entities(q: str):
    ents = []
    for m in re.finditer(r'\b([A-Z][A-Za-z0-9&\.\-]{1,}(?: [A-Z][A-Za-z0-9&\.\-]{1,}){0,3})\b', q):
        s = m.group(1).strip()
        if s: ents.append(ALIAS.get(s.lower(), s))
    if "tech 100" in q.lower() or "tech-100" in q.lower(): ents.append("TECH100")
    seen=set(); out=[]
    for e in ents:
        if e not in seen:
            seen.add(e); out.append(e)
    return out[:5]

def make_variants(q: str, ents):
    vs=[q.strip()]
    qn=re.sub(r'\btech[-\s]?100\b','TECH100',q,flags=re.I)
    if qn!=q: vs.append(qn)
    vs.append(re.sub(r'[^\w\s]',' ',qn))
    seen=set(); out=[]
    for s in vs:
        s=s.strip()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out[:5]

def rrf_fuse(variant_results, k=RRF_K):
    from collections import defaultdict
    scores=defaultdict(float); bykey={}
    for res in variant_results:
        for rank,item in enumerate(res,1):
            key=(item.get("doc_id"),item.get("chunk_ix"))
            bykey[key]=item; scores[key]+=1.0/(k+rank)
    ranked=sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    return [bykey[k] for k,_ in ranked]

def _tok(s): 
    import re
    return set(re.findall(r'[A-Za-z0-9]{2,}', (s or "").lower()))

def mmr_select(cands, max_k=CHUNKS_MAX, lambda_=MMR_LAMBDA, per_doc=DOC_CAP):
    sel=[]; used=defaultdict(int)
    cand_tokens=[(_tok(c.get("chunk_text","")),i) for i,c in enumerate(cands)]
    while len(sel)<max_k and cands:
        best=None;best_score=-1;best_idx=None
        for idx,c in enumerate(cands):
            if used[c.get("doc_id")]>=per_doc: continue
            rel=1.0/(1.0+float(c.get("dist",0.0) or 0.0))
            if not sel: score=rel
            else:
                t_c=cand_tokens[idx][0]
                sim=max((len(t_c & _tok(s.get("chunk_text","")))/max(1,len(t_c|_tok(s.get("chunk_text","")))) for s in sel), default=0.0)
                score=lambda_*rel-(1.0-lambda_)*sim
            if score>best_score: best_score=score; best=c; best_idx=idx
        if best is None: break
        sel.append(best); used[best.get("doc_id")]+=1
        cands.pop(best_idx); cand_tokens.pop(best_idx)
    return sel

def retrieve(q: str):
    ents=extract_entities(q)
    variants=make_variants(q, ents)
    topk=FUSION_TOPK_BASE
    per_variant=[]
    for v in variants:
        vec=embed(v)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, topk))
        per_variant.append(rows)
    fused=rrf_fuse(per_variant, k=RRF_K)
    if len(fused)<CHUNKS_MAX//2 and topk<FUSION_TOPK_MAX:
        topk=min(FUSION_TOPK_MAX, topk*2)
        per_variant=[]
        for v in variants:
            vec=embed(v)
            if _top_k_by_vector is None:
                rows = []
            else:
                rows = _top_k_by_vector(vec, max(1, topk))
            per_variant.append(rows)
        fused=rrf_fuse(per_variant, k=RRF_K)
    fused=mmr_select(fused[:max(32,FUSION_TOPK_MAX)], max_k=CHUNKS_MAX, lambda_=MMR_LAMBDA, per_doc=DOC_CAP)
    return {"entities": ents, "variants": variants, "chunks": fused, "k": topk}

def extract_quotes(chunks, limit_words=60):
    quotes=[]; total=0
    for i,c in enumerate(chunks,1):
        txt=(c.get("chunk_text") or "").strip()
        if not txt: continue
        parts=re.split(r'(?<=[.!?])\s+', txt)
        for p in parts[:2]:
            w=len(p.split())
            if w==0: continue
            if total + w > limit_words: break
            quotes.append((i, p.strip())); total+=w
        if total>=limit_words: break
    return quotes

def find_first(chunks, key):
    for c in chunks:
        t=(c.get("title") or "").lower()
        if key in t: return c
    return None

def parse_rank(chunk):
    if not chunk: return None
    t=(chunk.get("chunk_text") or "") + " " + (chunk.get("title") or "")
    m=re.search(r'\brank\s*(?:is\s*)?(?:#\s*)?(\d{1,3})\b', t, flags=re.I)
    if m: return m.group(1)
    if re.search(r'\branks?\s*first\b', t, flags=re.I): return "1"
    if re.search(r'\branks?\s*second\b', t, flags=re.I): return "2"
    if re.search(r'\branks?\s*third\b', t, flags=re.I): return "3"
    return None

def parse_asof(chunk):
    if not chunk: return None
    txt=(chunk.get("chunk_text") or "")
    m=re.search(r'(20\d{2}[-/]\d{2}|20\d{2}-\d{2}-\d{2}|[A-Za-z]{3,9}\s+20\d{2})', txt)
    return m.group(1) if m else None

def _collect_sources(chunks, maxn=CITES_MAX):
    seen=set(); collected=[]
    for c in chunks:
        title=(c.get("title") or "").strip()
        url=(c.get("source_url") or "").strip()
        if not title and not url:
            continue
        key=(title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        collected.append({
            "title": title or (url or "Source"),
            "url": url,
        })
        if len(collected) >= maxn:
            break
    return collected

def sources_block(chunks, maxn=CITES_MAX):
    return [c["title"] for c in _collect_sources(chunks, maxn)]

def sources_detailed(chunks, maxn=CITES_MAX):
    return _collect_sources(chunks, maxn)

def _strip_source_refs(text: str) -> str:
    cleaned = SOURCE_TAG_RE.sub("", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def _chunk_snippets_for_llm(chunks, limit=5, max_chars=360):
    prepared=[]
    for idx, c in enumerate(chunks):
        raw=(c.get("chunk_text") or "").strip()
        if not raw:
            continue
        snippet=re.sub(r"\s+", " ", raw)
        if max_chars and len(snippet) > max_chars:
            snippet=snippet[:max_chars].rsplit(" ", 1)[0].strip()
        title=(c.get("title") or "").strip()
        label=title if title else f"Snippet {idx+1}"
        prepared.append((label, snippet))
        if len(prepared) >= limit:
            break
    return prepared

def _tailor_with_gemini(question, intent, baseline, chunks, sources):
    if not baseline or not _gemini_generate:
        return baseline
    snippets=_chunk_snippets_for_llm(chunks)
    if not snippets:
        return baseline
    context_blob="\n".join(f"- {label}: {text}" for label, text in snippets)
    context_payload=[{"title": label, "snippet": text} for label, text in snippets]
    source_summary=", ".join(s.get("title", "") for s in sources[:3] if s.get("title")) if sources else ""
    prompt_parts = [
        "You are the SustainaCore Assistant. Rewrite the baseline answer so it stays factual but sounds conversational and helpful.\n",
        f"Question: {question.strip()}\n",
        f"Intent: {intent}\n",
        "Baseline answer (preserve facts and direct conclusions):\n",
        _strip_source_refs(baseline) + "\n\n",
        "Evidence excerpts:\n",
        context_blob + "\n\n",
    ]
    if source_summary:
        prompt_parts.append(f"Key sources: {source_summary}\n")
    prompt_parts.extend([
        "Guidelines:\n",
        "- Start with a sentence that answers the question directly.\n",
        "- Refer to the evidence naturally; do not use bracketed citations like [S1].\n",
        "- Keep the tone professional, friendly, and under 130 words.\n",
        "- Admit when information is missing.\n",
        "- Do not fabricate content beyond the excerpts.\n",
        "- End before the Sources section; the system will append sources.",
    ])
    prompt="".join(prompt_parts)
    try:
        candidate=_gemini_generate(prompt, context=context_payload) or ""
    except Exception:
        return baseline
    candidate=candidate.strip()
    if not candidate:
        return baseline
    return candidate

def compose_overview(entity, chunks, quotes):
    mem=find_first(chunks,"membership")
    rank=find_first(chunks,"rank")
    rnk=parse_rank(rank)
    asof=parse_asof(mem) or parse_asof(rank)
    lines=[]
    if mem: lines.append(f"TECH100 membership: Yes{f' (as of {asof})' if asof else ''}.")
    else:   lines.append("TECH100 membership: Not found in retrieved membership list.")
    if rnk: lines.append(f"Latest rank: {rnk}.")
    why=[f"Source {i}: {q}" for i,q in quotes[:4]]
    src=sources_detailed(chunks)
    out=[]
    out.append(f"{entity}: overview from SustainaCore’s knowledge base")
    out.extend("- "+w for w in lines)
    if why:
        out.append("Why this answer:")
        out.extend("- "+w for w in why)
    return "\n".join(out), src

def compose_answer_baseline(intent, q, entities, chunks, quotes):
    if intent=="about":
        text=("SustainaCore Assistant helps you explore the TECH100 AI Governance & Ethics Index and related ESG/AI sources.\n"
              "- Ask about a company’s TECH100 membership or latest rank.\n"
              "- Request a quick AI & ESG snapshot for any TECH100 company.\n"
              "- Data comes from Oracle Autonomous DB + Vector Search.\n")
        return text, "about", []
    if intent=="overview" and entities:
        overview_text, overview_sources = compose_overview(entities[0], chunks, quotes)
        return overview_text, "overview", overview_sources
    if intent in ("membership","rank") and entities:
        mem=find_first(chunks,"membership") if intent=="membership" else None
        rank=find_first(chunks,"rank")
        rnk=parse_rank(rank)
        asof=parse_asof(mem) or parse_asof(rank)
        lines=[]
        if intent=="membership":
            if mem: lines.append(f"Yes — {entities[0]} is in TECH100{f' (as of {asof})' if asof else ''}.")
            else:   lines.append(f"No evidence of {entities[0]} in TECH100 from retrieved membership context.")
        if intent=="rank":
            if rnk: lines.append(f"{entities[0]} latest TECH100 rank: {rnk}.")
            else:   lines.append("No clear rank found in retrieved context.")
        why=[f"Source {i}: {q}" for i,q in quotes[:4]]
        body="\n".join(lines + (["Why this answer:"]+[f"- {w}" for w in why] if why else []))
        return body, intent, sources_detailed(chunks)
    bullets=[f"Source {i}: {q}" for i,q in quotes[:6]]
    head="Here’s the best supported answer from the retrieved sources."
    body="\n".join([head] + (["Why this answer:"]+[f"- {b}" for b in bullets] if bullets else []))
    return body, "general", sources_detailed(chunks)

def compose_answer(intent, q, entities, chunks, quotes):
    baseline, shape, sources = compose_answer_baseline(intent, q, entities, chunks, quotes)
    tailored = _tailor_with_gemini(q, intent, baseline, chunks, sources) if intent != "about" else baseline
    answer = tailored or baseline
    if sources:
        footer_lines = ["Sources:"] + ["• " + s["title"] + (f" — {s['url']}" if s.get("url") else "") for s in sources]
        footer = "\n".join(footer_lines)
        if answer:
            answer = answer.rstrip() + "\n\n" + footer
        else:
            answer = footer
    return answer, shape, sources
class NormalizeMiddleware:
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        # No-op (Ask2 orchestrator removed)
        return self.app(environ, start_response)

class OrchestrateMiddleware:
    def __init__(self, app): self.app=app
    def __call__(self, environ, start_response):
        path=environ.get("PATH_INFO",""); method=(environ.get("REQUEST_METHOD") or "GET").upper()
        if not (path=="/ask" and method=="POST"):
            return self.app(environ, start_response)

        import io as _io
        try:
            size=int(environ.get("CONTENT_LENGTH") or "0")
            # Ask2 orchestrator disabled
            pass
        except Exception:
            size=0
            pass
        body=environ["wsgi.input"].read(size) if size>0 else b""
        try:
            req=json.loads(body.decode("utf-8") or "{}")
            # Ask2 orchestrator disabled
            pass
        except Exception:
            req={}
            pass
        q=(req.get("question") or req.get("q") or "").strip()
        if not q:
            return self.app(environ, start_response)

        t0=time.time()
        retrieval=retrieve(q)
        entities=retrieval["entities"]
        chunks=retrieval["chunks"]
        quotes=extract_quotes(chunks, limit_words=60)
        intent=detect_intent(q, entities)

        if (not chunks) or (entities and not any(entities[0].lower() in ( (c.get("title") or "").lower() + (c.get("chunk_text") or "").lower() ) for c in chunks)):
            ans=("I couldn’t find that in SustainaCore’s knowledge base.\n"
                 "- Scope: TECH100 companies and ESG/AI governance sources.\n"
                 "- Tip: try a company name or ask about TECH100 membership or latest rank.")
            payload={"answer": ans, "contexts": chunks, "mode":"simple", "sources": []}
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
            hdrs=[("Content-Type","application/json"),
                  ("X-Intent", intent), ("X-K", str(retrieval["k"])),
                  ("X-RRF","on"), ("X-MMR", str(MMR_LAMBDA)),
                  ("X-Loops","1"), ("X-BudgetMs", str(int((time.time()-t0)*1000))),
                  ("X-Answer-Shape","not-found"), ("Content-Length", str(len(data)))]
            start_response("200 OK", hdrs)
            return [data]

        answer, shape, sources_out = compose_answer(intent, q, entities, chunks, quotes)
        payload={"answer": answer, "contexts": chunks, "mode":"simple", "sources": sources_out}
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
        hdrs=[("Content-Type","application/json"),
              ("X-Intent", intent), ("X-K", str(retrieval["k"])),
              ("X-RRF","on"), ("X-MMR", str(MMR_LAMBDA)),
              ("X-Loops","1"), ("X-BudgetMs", str(int((time.time()-t0)*1000))),
              ("X-Answer-Shape", shape), ("Content-Length", str(len(data)))]
        start_response("200 OK", hdrs)
        return [data]

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": time.time()})

@app.route("/ask", methods=["POST"])
def ask():
    try:
        body = request.get_json(force=True) or {}
        q = (body.get("question") or body.get("q") or "").strip()
        if not q: return jsonify({"error":"question is required"}), 400
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE))
        ans = rows[0]["chunk_text"] if rows else "No context found."
        if RETURN_TOP_AS_ANSWER:
            return jsonify({"answer": ans, "contexts": rows, "mode":"simple"})
        else:
            return jsonify({"answer": "No generator configured.", "contexts": rows, "mode":"simple"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Install middlewares
app.wsgi_app = NormalizeMiddleware(app.wsgi_app)
app.wsgi_app = OrchestrateMiddleware(app.wsgi_app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)


# --- Multi-Hit Orchestrator (RRF+MMR, in-process) ---
import io as _io, json as _json, re as _re, time as _time
from collections import defaultdict as _dd

def _norm_q(q: str) -> str:
    q = (q or "").strip()
    # strip UI prompt/buttons
    q = _re.sub(r'(?is)what would you like to explore\?.*$', '', q).strip()
    # strip scaffold spillovers
    q = _re.sub(r'(?m)^(ROLE|TASK|PREVIOUS ANSWER|QUESTION TYPE).*$', '', q).strip()
    # collapse whitespace
    q = _re.sub(r'\s+', ' ', q)
    return q[:400]

def _intent(q: str) -> str:
    s = q.lower()
    if any(w in s for w in ('member','constituent','included','in the tech100','in tech 100')): return 'membership'
    if 'rank' in s: return 'rank'
    if any(w in s for w in ('compare','versus','vs ')): return 'compare'
    if any(w in s for w in ('what is','define','definition')): return 'definition'
    return 'general'

def _variants(q: str):
    s = q.strip()
    v = [s]
    # light paraphrases (cheap)
    v.append(_re.sub(r'\bmember(ship)?\b', 'constituent', s, flags=_re.I))
    v.append(_re.sub(r'\btech ?100\b', 'TECH100', s, flags=_re.I))
    v = [x for i,x in enumerate(v) if x and x not in v[:i]]
    return v[:4]

def _call_downstream_wsgipost(app, body: bytes, extra_headers=None):
    # Build a fresh WSGI environ and call the next app in stack directly (no network)
    env = {
        'REQUEST_METHOD':'POST',
        'PATH_INFO':'/ask',
        'SERVER_NAME':'localhost','SERVER_PORT':'8080','SERVER_PROTOCOL':'HTTP/1.1',
        'wsgi.version':(1,0),'wsgi.url_scheme':'http','wsgi.input':_io.BytesIO(body),
        'CONTENT_TYPE':'application/json','CONTENT_LENGTH':str(len(body)),
    }
    if extra_headers:
        for k,v in extra_headers.items():
            env['HTTP_'+k.upper().replace('-','_')] = v
    status_headers = {}
    def _start_response(status, headers, exc_info=None):
        status_headers['status']=status; status_headers['headers']=headers; return lambda x: None
    chunks = []
    for chunk in app(env, _start_response):
        chunks.append(chunk)
    raw = b''.join(chunks)
    try:
        data = _json.loads(raw.decode('utf-8','ignore'))
    except Exception:
        data = {'raw': raw[:4096].decode('utf-8','ignore')}
    return status_headers.get('status','200 OK'), dict(status_headers.get('headers',[])), data

def _rrf(fused_lists):
    # fused_lists: [ [ctx, ctx, ...], [ctx...], ... ]
    scores = _dd(float)
    keyf  = lambda c: (c.get('doc_id'), c.get('chunk_ix'))
    for lst in fused_lists:
        for rank, ctx in enumerate(lst, start=1):
            scores[keyf(ctx)] += 1.0/(60.0 + rank)  # RRF with k=60 (stable)
    # unique by (doc_id, chunk_ix)
    seen=set(); fused=[]
    for lst in fused_lists:
        for ctx in lst:
            k=keyf(ctx)
            if k in seen: continue
            seen.add(k); ctx = dict(ctx); ctx['_rrf']=scores[k]; fused.append(ctx)
    fused.sort(key=lambda c: c.get('_rrf',0.0), reverse=True)
    return fused

def _mmr_select(candidates, max_n=12, lam=0.7):
    # diversity by title/doc_id; use rrf score as relevance; jaccard on titles for diversity
    def toks(t): 
        return set(_re.findall(r'[a-z0-9]+', (t or '').lower()))
    selected=[]; selected_toks=[]
    pool = list(candidates)
    while pool and len(selected)<max_n:
        best=None; best_score=-1
        for c in pool:
            rel = c.get('_rrf',0.0)
            ct  = toks(c.get('title') or '')
            if not selected:
                score = rel
            else:
                sim = max((len(ct & st)/(len(ct|st) or 1) for st in selected_toks), default=0.0)
                score = lam*rel - (1-lam)*sim
            if score>best_score: best_score=score; best=c
        selected.append(best); selected_toks.append(toks(best.get('title') or ''))
        pool = [c for c in pool if c is not best]
    return selected

def _compose(q, intent, picks):
    # cheap, deterministic scaffold + tiny quote-then-summarize from the chosen chunks
    def cite(i): return f"[S{i+1}]"
    bullets=[]
    for i,c in enumerate(picks[:4]):
        txt=(c.get('chunk_text') or '').strip()
        # short slice (<=120 chars)
        snippet = _re.sub(r'\s+',' ', txt)[:120].rstrip(' ,.;:')
        if snippet: bullets.append(f"{cite(i)} {snippet}")
    sources=[]
    for i,c in enumerate(picks):
        t = (c.get('title') or '').strip()
        su = (c.get('source_url') or '').strip()
        if t or su: sources.append(f"{cite(i)} {t or su}")
    head=""
    s = q.lower()
    if intent=='membership':
        found = any('membership' in (c.get('title') or '').lower() or 'index' in (c.get('title') or '').lower() for c in picks)
        head = ("Yes — appears in the TECH100 AI Governance & Ethics Index." if found else
                "Not found in the retrieved TECH100 membership set.")
    elif intent=='rank':
        head = "Latest TECH100 rank: see sources below."
    elif intent=='definition':
        head = "Here’s the concise definition from SustainaCore’s corpus."
    else:
        head = "Here’s the best supported answer from the retrieved sources."
    out = head
    if bullets:
        out += "\n" + "\n".join(f"- {b}" for b in bullets[:4])
    if sources:
        out += "\nSources: " + "; ".join(sources[:6])
    return out

class MultiHitOrchestrator:
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        # bypass on internal calls
        if environ.get('HTTP_X_ORCH') == 'bypass':
            return self.app(environ, start_response)
        if environ.get('PATH_INFO') != '/ask' or (environ.get('REQUEST_METHOD') or '').upper()!='POST':
            return self.app(environ, start_response)

        # parse incoming
        try:
            size = int(environ.get('CONTENT_LENGTH') or '0')
        except Exception:
            size = 0
        body = environ['wsgi.input'].read(size) if size>0 else b'{}'
        try:
            payload = _json.loads(body.decode('utf-8','ignore'))
        except Exception:
            payload = {}
        q_in = _norm_q(str(payload.get('question') or ''))
        if not q_in:
            # fall back to downstream as-is
            payload2 = payload; raw2 = _json.dumps(payload2).encode('utf-8')
            status, headers, data = _call_downstream_wsgipost(self.app, raw2, {'X-Orch':'bypass'})
            headers = [(k,v) for (k,v) in headers if k.lower()!='content-length']
            headers.append(('X-Orch','pass'))
            resp = _json.dumps(data, ensure_ascii=False).encode('utf-8')
            headers.append(('Content-Length', str(len(resp))))
            start_response(status, headers)
            return [resp]

        intent = _intent(q_in)
        vs = _variants(q_in)

        # Build and run hits (in-process): k=8 → 16 → 24
        k_plan = [8,16,24]
        fused_lists=[]
        total_hits=0
        budget_ms=int(os.environ.get('ORCH_BUDGET_MS','1200'))
        t0=_time.time()
        for v in vs:
            for k in k_plan:
                if ( _time.time()-t0 )*1000 > budget_ms: break
                hit = {'question': v, 'top_k': k}
                raw = _json.dumps(hit).encode('utf-8')
                status, headers, data = _call_downstream_wsgipost(self.app, raw, {'X-Orch':'bypass'})
                ctxs = data.get('contexts') or []
                if isinstance(ctxs, list) and ctxs:
                    fused_lists.append(ctxs[:k])
                total_hits += 1
            if ( _time.time()-t0 )*1000 > budget_ms: break

        if not fused_lists:
            raw2 = _json.dumps({'question': q_in, 'top_k': payload.get('top_k', 8)}).encode('utf-8')
            status, headers, data = _call_downstream_wsgipost(self.app, raw2, {'X-Orch':'bypass'})
            headers = [(k,v) for (k,v) in headers if k.lower()!='content-length']
            headers.extend([('X-Intent', intent), ('X-Orch','fallback'), ('X-Hits', str(total_hits))])
            resp = _json.dumps(data, ensure_ascii=False).encode('utf-8')
            headers.append(('Content-Length', str(len(resp))))
            start_response(status, headers)
            return [resp]

        fused = _rrf(fused_lists)
        picks = _mmr_select(fused, max_n=12, lam=0.7)
        answer = _compose(q_in, intent, picks)

        out = {'answer': answer, 'contexts': picks, 'mode': 'simple'}

        hdrs = [('Content-Type','application/json'),
                ('X-Intent', intent), ('X-RRF','on'), ('X-MMR','0.7'),
                ('X-Hits', str(total_hits)), ('X-BudgetMs', str(int(( _time.time()-t0 )*1000)))]
        resp = _json.dumps(out, ensure_ascii=False).encode('utf-8')
        hdrs.append(('Content-Length', str(len(resp))))
        start_response('200 OK', hdrs)
        return [resp]

# Install orchestrator at the very top of the stack
try:
    app.wsgi_app = MultiHitOrchestrator(app.wsgi_app)
except Exception as _e:
    # If something unexpected happens, do nothing (safe no-op)
    pass
# --- End Multi-Hit Orchestrator ---




# --- multihit orchestrator loader (idempotent) ---
try:
    import importlib
    om = importlib.import_module("orchestrator_mw")

    # Get classes defensively
    Normalize = getattr(om, "NormalizeMiddleware", None) or getattr(om, "NormalizeMid", None)
    Orchestrator = getattr(om, "OrchestratorMiddleware", None) or getattr(om, "OrchestrateMiddleware", None)

    if Normalize and Orchestrator:
        # Order: Normalize OUTSIDE (runs first), then Orchestrator
        app.wsgi_app = Orchestrator(app.wsgi_app)
        app.wsgi_app = Normalize(app.wsgi_app)
        try:
            app.config["SMART_ORCH"] = True
        except Exception:
            pass
except Exception as _e:
    # Fail closed if anything goes wrong
    try:
        import logging
        logging.getLogger(__name__).exception(_e)
    except Exception:
        pass

# --- auto-appended by APPLY_NOSUGGEST.sh ---
try:
    from nosuggest_mw import NoSuggestMiddleware  # noqa: E402
    app.wsgi_app = NoSuggestMiddleware(app.wsgi_app)
except Exception as _e:
    import logging as _lg  # noqa: E402
    _lg.getLogger(__name__).exception("NoSuggestMiddleware install error: %s", _e)
# --- end auto append ---

# --- Smalltalk WSGI wrapper (auto-added v4) ---
try:
    from smalltalk_wsgi import SmalltalkMiddleware
    app.wsgi_app = SmalltalkMiddleware(app.wsgi_app)
except Exception:
    pass
# --- End Smalltalk WSGI wrapper ---

# --- ask2 CORS wrapper ---
try:
    from ask2_cors_mw import CORSReflectMiddleware
    _ASK2_ALLOWED_ORIGINS = [
        "https://sustainacore.org",
        "https://www.sustainacore.org",
        "https://ai.sustainacore.org",
        "https://apex.oracle.com",
    ]
    app.wsgi_app = CORSReflectMiddleware(app.wsgi_app, allowed_origins=_ASK2_ALLOWED_ORIGINS)
except Exception as _e:
    # Fail-safe: never break the API if CORS wrapper import fails
    pass
# --- end ask2 CORS wrapper ---










# --- ask2 LLM orchestrator wrapper ---
try:
    # Ask2 orchestrator disabled (Gemini-first path)
    pass
except Exception:
    pass
# --- end ask2 LLM orchestrator wrapper ---

# register ask2_direct route
try:
    import route_direct
    route_direct.register(app)
except Exception as e:
    print("route_direct register failed:", e)

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

# register ask2_simple route\ntry:\n    import route_simple; route_simple.register(app)\n    print("ask2_simple route registered")\nexcept Exception as e:\n    print("route_simple register failed:", e)\n

@app.route('/ask2', methods=['GET', 'POST'])
def ask2():
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        q_raw = data.get('q') or data.get('question') or data.get('text')
        k_value = data.get('k') or data.get('top_k') or data.get('limit')
    else:
        args = request.args
        q_raw = args.get('q') or args.get('question') or args.get('text')
        k_value = args.get('k') or args.get('top_k') or args.get('limit')

    question = q_raw.strip() if isinstance(q_raw, str) else ''
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        client_ip = forwarded.split(',')[0].strip()
    else:
        client_ip = request.remote_addr or 'unknown'
    shaped, status = _call_route_ask2_facade(question, k_value, client_ip=client_ip)
    return jsonify(shaped), status



@app.route("/ask", methods=["GET"], endpoint="ask_get_shim")
def ask_get_shim():
    from flask import request, jsonify
    q = (request.args.get("question") or request.args.get("q") or "").strip()
    if not q:
        return jsonify({"error":"q is required"}), 400

    # Try the fast path; if embeddings service is down/misconfigured, fall back.
    try:
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE))
        ans = rows[0]["chunk_text"] if rows else "No context found."
        if RETURN_TOP_AS_ANSWER:
            return jsonify({"answer": ans, "contexts": rows, "mode":"simple"})
    except Exception:
        pass

    retrieval = retrieve(q)
    entities = retrieval.get("entities", [])
    chunks = retrieval.get("chunks", [])
    quotes  = extract_quotes(chunks, limit_words=60)
    intent  = detect_intent(q, entities)
    answer, shape, sources_out = compose_answer(intent, q, entities, chunks, quotes)
    return jsonify({"answer": answer, "contexts": chunks, "mode":"simple", "sources": sources_out})




# --- BEGIN: APEX POST /ask compatibility (no URL change) ---------------------
import os, time

def __sc__build_sources(rows):
    out = []
    for r in rows or []:
        doc_id = str(r.get("doc_id",""))
        chunk_ix = r.get("chunk_ix", 0)
        rid = f"{doc_id}-{chunk_ix}" if doc_id else str(chunk_ix)
        dist = r.get("dist")
        try:
            score = round(1.0 - float(dist), 4) if dist is not None else None
        except Exception:
            score = None
        snippet = (r.get("chunk_text") or "")[:400]
        out.append({
            "id": rid,
            "score": score,
            "snippet": snippet,
            "title": r.get("title"),
            "url": r.get("source_url"),
        })
    return out

@app.before_request
def __sc__apex_post_ask_compat():
    # keep a single path (/ask), adjust only POST behavior for APEX
    from flask import request, jsonify
    if request.path != "/ask" or request.method != "POST":
        return  # don't touch anything else

    t0 = time.perf_counter()

    # Accept q from args OR JSON body (both cases APEX might use)
    body = {}
    if request.is_json:
        body = request.get_json(silent=True) or {}
    q = (request.args.get("q") or request.args.get("question")
         or body.get("q") or body.get("question") or "").strip()
    if not q:
        return jsonify({"error": "q is required"}), 400

    try:
        vec = embed(q)
        if _top_k_by_vector is None:
            rows = []
        else:
            rows = _top_k_by_vector(vec, max(1, FUSION_TOPK_BASE))
        # Stable 'answer' for APEX: if your main chain returns text elsewhere,
        # we still guarantee a non-empty answer by falling back to top snippet.
        ans = (rows[0].get("chunk_text") if rows else None) or "No context found."

        took_ms = int((time.perf_counter() - t0) * 1000)
        resp = {
            "answer": ans,
            "sources": __sc__build_sources(rows),
            "meta": {
                "k": len(rows or []),
                "took_ms": took_ms,
                "model_info": {"embed": os.getenv("OLLAMA_EMBED_MODEL","nomic-embed-text")},
            },
        }
        return jsonify(resp), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# --- END: APEX POST /ask compatibility ---------------------------------------
