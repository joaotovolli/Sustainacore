# orchestrator_mw.py
# Multihit Orchestrator Middleware (safe append-only integration)
# Version: 2.0 (2025-08-31)
# Goals:
# - Normalize noisy UI prompts.
# - Light intent classification + entity extraction.
# - Generate 3–5 query variants.
# - Hit downstream /ask2 multiple times; fuse contexts via RRF; apply MMR diversity.
# - Compose evidence-first answer scaffold (answer → bullets with [S1..] → source map).
# - Bounded latency and chunk budgets; never touch systemd or NGINX.

from __future__ import annotations
import io, sys, time, json, re, math
from typing import List, Dict, Any, Tuple

# ====== knobs (override via env if present) ======
import os
RRF_K = int(os.getenv("SMART_RRF_K", "60"))
MMR_LAMBDA = float(os.getenv("SMART_MMR_LAMBDA", "0.7"))
VARIANTS_N = int(os.getenv("SMART_VARIANTS", "3"))  # 3–5
K_SEQ = [int(x) for x in os.getenv("SMART_K_SEQ", "8,16,24").split(",") if x.strip().isdigit()]
MAX_CONTEXTS = int(os.getenv("SMART_MAX_CONTEXTS", "12"))
LATENCY_BUDGET_MS = int(os.getenv("SMART_LATENCY_MS", "2400"))  # whole middleware wall time
INTENT_DEFAULT = "general"

TECH100_WORDS = ("tech100", "ai governance", "ethics index", "index", "rank", "membership")
BUTTON_NOISE = "what would you like to explore?"

ALIASES = {
    # tiny alias map; can be extended; case-insensitive match
    "microsoft": ["msft", "microsoft corporation"],
    "alphabet": ["google", "googl", "goog"],
    "apple": ["aapl", "apple inc"],
    "cisco": ["csco", "cisco systems"],
    "tesla": ["tsla", "tesla inc"],
}

def _jloads(b: bytes) -> Any:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return None

def _jdump(o: Any) -> bytes:
    return json.dumps(o, ensure_ascii=False).encode("utf-8")

def _now_ms() -> int:
    return int(time.time() * 1000)

def _normalize_question(q: str) -> str:
    if not q:
        return q
    s = q.strip()
    # strip common scaffolding
    if s.lower().startswith("you asked:"):
        # keep only first actual question sentence if present
        m = re.search(r"you asked:\s*(.+?\?)", s, re.S | re.I)
        if m:
            s = m.group(1).strip()
        else:
            s = re.sub(r"(?m)^(role|task|previous answer|question type).*?$", "", s, flags=re.I)
    # drop UI buttons prompt
    s = s.replace("buttons to click = |TECH100 Membership| |Rank(latest)| |AI & ESG profile|", "").strip()
    if s.lower().startswith(BUTTON_NOISE):
        s = ""
    return s

def _classify_intent(q: str) -> str:
    s = (q or "").lower()
    if not s:
        return "general"
    # heuristic keywords
    if re.search(r"\b(is|are)\b.+\b(member|constituent|in the index)\b", s) or "membership" in s:
        return "membership"
    if "rank" in s or re.search(r"\bposition\b", s):
        return "rank"
    if s.startswith("what is ") or s.startswith("define ") or "what is the" in s:
        return "definition"
    if s.startswith("how ") or s.startswith("why "):
        return "howwhy"
    if "compare" in s or "vs" in s or " versus " in s:
        return "compare"
    if "trend" in s or "over time" in s:
        return "trend"
    if "policy" in s or "regulation" in s or "regulatory" in s:
        return "policy"
    if any(w in s for w in TECH100_WORDS) and ("?" in s):
        return "membership"
    # bare company -> overview
    if len(s.split()) <= 3:
        return "overview"
    return "general"

def _extract_entities(q: str) -> List[str]:
    s = (q or "").strip()
    # naive: collect capitalized tokens & known aliases
    toks = re.findall(r"[A-Z][A-Za-z0-9&\.-]{2,}", s)
    lower = s.lower()
    out = set()
    for name, al in ALIASES.items():
        if name in lower or any(a in lower for a in al):
            out.add(name)
    # if no alias matched but we have tokens, keep first 2
    if not out and toks:
        out.add(toks[0])
    return list(out)[:2]

def _gen_variants(q: str, intent: str, entities: List[str]) -> List[str]:
    variants = []
    base = q.strip()
    if base:
        variants.append(base)
    # synthesize simplified forms
    if entities:
        e = entities[0]
        e_aliases = [e] + ALIASES.get(e.lower(), [])
        for a in e_aliases[:2]:
            if intent == "membership":
                variants.append(f"TECH100 membership: {a}")
            elif intent == "rank":
                variants.append(f"TECH100 rank: {a}")
            elif intent == "overview":
                variants.append(f"{a} TECH100 overview")
    # dedupe
    seen = set()
    out = []
    for v in variants:
        vv = v.strip()
        if vv and vv.lower() not in seen:
            seen.add(vv.lower())
            out.append(vv)
    return out[:max(1, VARIANTS_N)]

# Minimal RRF
def _rrf_merge(all_lists: List[List[Dict[str,Any]]], k: int) -> List[Dict[str,Any]]:
    scores = {}
    order = {}
    for li in all_lists:
        for rank, item in enumerate(li, start=1):
            key = (item.get("doc_id"), item.get("chunk_ix"))
            scores[key] = scores.get(key, 0.0) + 1.0 / (RRF_K + rank)
            # remember first seen item for payload
            if key not in order:
                order[key] = item
    ranked = sorted(order.keys(), key=lambda kk: (-scores.get(kk,0.0), kk))
    return [order[kk] for kk in ranked[:k]]

# Simple MMR over title text shingles
def _title_shingles(t: str) -> set:
    words = re.findall(r"[a-z0-9]+", (t or "").lower())
    return set(words)

def _sim(a: str, b: str) -> float:
    A, B = _title_shingles(a), _title_shingles(b)
    if not A or not B: 
        return 0.0
    inter = len(A & B)
    union = len(A | B)
    return inter / union if union else 0.0

def _mmr_select(candidates: List[Dict[str,Any]], k: int) -> List[Dict[str,Any]]:
    selected = []
    cand = candidates[:]
    while cand and len(selected) < k:
        best, best_score = None, -1e9
        for item in cand:
            rel = 1.0  # after RRF, treat remaining as equally relevant
            div = 0.0
            if selected:
                div = max(_sim(item.get("title",""), s.get("title","")) for s in selected)
            score = MMR_LAMBDA * rel - (1.0 - MMR_LAMBDA) * div
            if score > best_score:
                best_score, best = score, item
        selected.append(best)
        cand.remove(best)
    return selected

# helper: call downstream app internally with modified JSON
class _Cap:
    def __init__(self): 
        self.status = "200 OK"; self.headers = []; self.body = b""
    def start(self, status, headers, exc_info=None):
        self.status = status; self.headers = headers
        return self.write
    def write(self, data):
        self.body += data

def _mk_environ(body: bytes) -> dict:
    return {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": "/ask2",
        "SERVER_NAME": "127.0.0.1",
        "SERVER_PORT": "8080",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(body),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": True,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": "application/json",
    }

def _short_quote(txt: str, max_words=25) -> str:
    if not txt: return ""
    words = re.findall(r"\S+", txt.strip())
    return " ".join(words[:max_words])

def install_multihit(app):
    return MultiHitMiddleware(app)

class MultiHitMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        t0 = _now_ms()
        # only intercept POST /ask2
        if not (environ.get("PATH_INFO")=="/ask2" and (environ.get("REQUEST_METHOD") or "").upper()=="POST"):
            return self.app(environ, start_response)

        # read body once
        try:
            size = int(environ.get("CONTENT_LENGTH") or "0")
        except Exception:
            size = 0
        raw = environ["wsgi.input"].read(size) if size>0 else b""
        data = _jloads(raw) or {}
        question = data.get("question","")
        top_k = int(data.get("top_k") or 8)

        q_norm = _normalize_question(question)
        intent = _classify_intent(q_norm)
        entities = _extract_entities(q_norm)
        variants = _gen_variants(q_norm, intent, entities)

        # budget guard
        budget_ms = LATENCY_BUDGET_MS
        k_seq = K_SEQ or [8,16,24]
        fused_list: List[Dict[str,Any]] = []
        chosen_k = k_seq[0]
        all_context_lists = []
        first_resp = None

        # inner helper to call downstream for a specific variant
        def call_variant(qv: str, k: int) -> Dict[str,Any]:
            payload = dict(data)
            payload["question"] = qv
            payload["top_k"] = k
            cap = _Cap()
            try:
                resp_iter = self.app(_mk_environ(_jdump(payload)), cap.start)
                for chunk in resp_iter:
                    cap.write(chunk)
                # close iterable if it has close()
                if hasattr(resp_iter, "close"):
                    try: resp_iter.close()
                    except Exception: pass
            except Exception as e:
                cap.status = "500 INTERNAL ERROR"
                cap.body = _jdump({"answer": f"Error calling downstream: {e}", "contexts":[]})
            try:
                obj = _jloads(cap.body) or {}
            except Exception:
                obj = {}
            return obj

        # loop: start small k, expand if needed
        for i, k in enumerate(k_seq):
            chosen_k = k
            stage_start = _now_ms()
            results = []
            for vi, qv in enumerate(variants):
                obj = call_variant(qv, k)
                if i==0 and vi==0:
                    first_resp = obj
                ctx = obj.get("contexts") or []
                # ensure minimal fields
                norm_ctx = []
                for c in ctx:
                    if not isinstance(c, dict): 
                        continue
                    norm_ctx.append({
                        "doc_id": c.get("doc_id"),
                        "chunk_ix": c.get("chunk_ix"),
                        "title": c.get("title") or "",
                        "source_url": c.get("source_url") or "",
                        "chunk_text": c.get("chunk_text") or "",
                        "dist": c.get("dist"),
                    })
                results.append(norm_ctx)
            # fuse (RRF)
            fused = _rrf_merge(results, min(k, MAX_CONTEXTS))
            # diversify (MMR)
            diversified = _mmr_select(fused, min(k, MAX_CONTEXTS))
            all_context_lists = results
            fused_list = diversified

            # simple coverage check for membership/rank intents
            need_expand = False
            titles = " | ".join([c.get("title","").lower() for c in fused_list])
            if intent in ("membership","rank","overview"):
                # require at least one TECH100 membership/rank page in titles
                hints = ("membership › tech100", "rank › tech100", "index compostion", "index composition", "tech100")
                if not any(h in titles for h in hints):
                    need_expand = True
            # stop if good enough or budget exceeded
            if not need_expand or (_now_ms() - t0) > budget_ms*0.6:
                break

        # Compose answer
        def compose_answer(intent: str, q: str, fused: List[Dict[str,Any]]) -> Tuple[str, List[Dict[str,Any]], Dict[str,str]]:
            if not fused:
                # fallback: if first_resp exists, pass it through but strip any scaffolding
                if isinstance(first_resp, dict) and "answer" in first_resp:
                    ans = first_resp.get("answer") or "I couldn’t find that in SustainaCore’s knowledge base."
                    # strip long scaffolding blocks if any
                    ans = re.sub(r"(?s)You asked:.*?(?:\n\n|$)", "", ans).strip() or ans
                    return ans, first_resp.get("contexts") or [], {}
                return "I couldn’t find that in SustainaCore’s knowledge base. Try narrowing the scope (company or topic).", [], {}

            # extract up to 6 short quotes
            quotes = []
            for i, c in enumerate(fused[:6], start=1):
                qline = _short_quote(c.get("chunk_text",""))
                if qline:
                    quotes.append((f"S{i}", qline, c))
            # determine direct answer for membership intent
            direct = ""
            if intent in ("membership","overview"):
                has_member = any("membership › tech100" in (c.get("title","").lower()) for c in fused)
                if has_member:
                    # try to pull as-of from text
                    asof = ""
                    for c in fused:
                        m = re.search(r"(\d{4}-\d{2})", c.get("chunk_text",""))
                        if m: 
                            asof = m.group(1)
                            break
                    ent = (entities[0] if entities else "").title() or "The company"
                    if asof:
                        direct = f"Yes — {ent} appears in the TECH100 AI Governance & Ethics Index (as of {asof})."
                    else:
                        direct = f"Yes — {ent} appears in the TECH100 AI Governance & Ethics Index."
                else:
                    direct = "No clear evidence for TECH100 membership found in the retrieved context."
            elif intent == "rank":
                has_rank = any("rank › tech100" in (c.get("title","").lower()) for c in fused)
                if has_rank:
                    direct = "Rank information found in the TECH100 materials (see sources)."
                else:
                    direct = "No clear rank found in the retrieved context."
            else:
                # generic
                direct = q.strip() or "Here’s the best supported answer from the retrieved sources."

            # bullets referencing sources
            bullets = []
            for tag, qline, c in quotes[:4]:
                bullets.append(f"- [{tag}] {qline}")

            # sources map
            src_lines = []
            for i, c in enumerate(fused[:6], start=1):
                title = (c.get("title") or "").strip() or c.get("source_url") or f"Doc {i}"
                src_lines.append(f"[S{i}] {title}")

            body = direct + "\nWhy this answer:\n" + ("\n".join(bullets) if bullets else "- Evidence snippets unavailable.") + "\n\nSources: " + "; ".join(src_lines)
            # contexts in same order as S1..S6
            ctx_out = []
            for i, c in enumerate(fused[:6], start=1):
                d = dict(c)
                d["tag"] = f"S{i}"
                ctx_out.append(d)
            # headers
            hdr = {
                "X-Intent": intent,
                "X-K": str(chosen_k),
                "X-RRF": "on",
                "X-MMR": str(MMR_LAMBDA),
                "X-Variants": str(len(variants)),
                "X-Question-Normalized": "on" if q != question else "off"
            }
            return body, ctx_out, hdr

        body_text, ctx_list, hdrs = compose_answer(intent, q_norm or question, fused_list)

        # final JSON
        out_obj = {"answer": body_text, "contexts": ctx_list, "mode": "simple"}
        out_bytes = _jdump(out_obj)

        # set headers
        headers = []
        def _start(status, hdr_list, exc_info=None):
            headers.extend(hdr_list)
            return lambda b: None

        # call downstream ONCE to get a baseline headers/status, then replace body+headers
        cap = _Cap()
        try:
            resp_iter = self.app(_mk_environ(raw), cap.start)
            for chunk in resp_iter:
                cap.write(chunk)
            if hasattr(resp_iter, "close"):
                try: resp_iter.close()
                except Exception: pass
        except Exception:
            cap.status = "200 OK"
            cap.headers = [("Content-Type","application/json")]
            cap.body = out_bytes  # ignore

        # overwrite headers
        hdr_map = {k.lower(): v for k, v in cap.headers}
        hdr_map["content-type"] = "application/json"
        # strip existing content-length
        if "content-length" in hdr_map:
            del hdr_map["content-length"]

        # add telemetry headers
        for k, v in hdrs.items():
            hdr_map[k.lower()] = v
        hdr_map["x-elapsedms"] = str(max(0, _now_ms() - t0))

        final_headers = [(k.title(), v) for k, v in hdr_map.items()]
        start_response(cap.status, final_headers)
        return [out_bytes]
