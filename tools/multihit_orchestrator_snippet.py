SNIPPET = r'''
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

'''