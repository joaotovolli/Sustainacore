# nosuggest_mw.py — v5.0 (server-side "never mute")
import io, json, re, os
CHIP_ROW_RE = re.compile(r'^\s*\|[^|]*\|\s*$', re.I)
SCFF_RE_LIST = [
    re.compile(r'^\s*we\s+have\s+several\s+views\b', re.I),
    re.compile(r'\bwhat\s+would\s+you\s+like\??\b', re.I),
    re.compile(r'^what\s+would\s+you\s+like\s+to\s+explore\?\s*$', re.I),
    re.compile(r'^(role|task|previous\s*answer|question\s*type|answer\s*style|rules?)\s*:', re.I),
]
SOURCE_LINE_RE = re.compile(r'^\s*(sources?|why\s+this\s+answer)\s*[:：]', re.I)
ALLOW_NORESULTS_MSG = os.getenv('NOSUGGEST_ALLOW_NORESULTS_MSG', '1') not in ('0','false','off')
TOPMATCH_MAX = int(os.getenv('NOSUGGEST_MAX_MATCHES', '5'))


def _flag_enabled(value: str, *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "off", "no"}


INLINE_SOURCES_ENABLED = _flag_enabled(os.getenv('INLINE_SOURCES'), default=False)
def _strip_scaffold(text: str):
    if not isinstance(text, str): return text, False
    stripped = False; out = []
    for ln in text.splitlines():
        t = ln.strip()
        if CHIP_ROW_RE.match(t): stripped=True; continue
        if not INLINE_SOURCES_ENABLED and SOURCE_LINE_RE.match(t): stripped=True; continue
        if any(rx.search(t) for rx in SCFF_RE_LIST): stripped=True; continue
        out.append(ln)
    cleaned = "\n".join(out).strip()
    if cleaned != (text or "").strip(): stripped = True
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    return cleaned, stripped
def _get(obj, *path):
    cur=obj
    for k in path:
        if cur is None: return None
        try: cur=cur[k]
        except Exception: return None
    return cur
def _extract_answer(j):
    for p in (('answer',), ('content',), ('message',),
              ('choices',0,'message','content'),
              ('choices','0','message','content'),
              ('data','answer'), ('data','content'), ('data','message'),
              ('data','choices',0,'message','content'),
              ('result',), ('reply',), ('text',)):
        v=_get(j,*p)
        if isinstance(v,str) and v.strip(): return v
    best,best_len='',0
    def scan(n):
        nonlocal best,best_len
        if n is None: return
        if isinstance(n,str):
            s=n.strip()
            if not s: return
            if any(rx.search(s) for rx in SCFF_RE_LIST): return
            if CHIP_ROW_RE.match(s): return
            if len(s)>best_len: best,best_len=s,len(s)
            return
        if isinstance(n,list):
            for it in n: scan(it)
        elif isinstance(n,dict):
            for it in n.values(): scan(it)
    scan(j); return best
def _find_contexts(j):
    for key in ('contexts','sources','references','docs'):
        v=j.get(key)
        if isinstance(v,list) and v: return v
    if isinstance(j.get('data'),dict) and isinstance(j['data'].get('contexts'),list):
        return j['data']['contexts']
    return []
def _mk_topmatches(ctx):
    seen,uniq=set(),[]
    for c in ctx:
        t=(c.get('title') or '').strip()
        u=(c.get('source_url') or c.get('url') or '').strip()
        key=f"{t}|{u}"
        if key in seen: continue
        seen.add(key); uniq.append(t or u or 'source')
        if len(uniq)>=TOPMATCH_MAX: break
    if not uniq: return ''
    return "Top matches\n" + "\n".join(f"• {x}" for x in uniq)
def _mirror_answer(j,text):
    if 'choices' in j and isinstance(j['choices'],list) and j['choices']:
        if not isinstance(j['choices'][0],dict): j['choices'][0]={}
        msg=j['choices'][0].get('message') or {}
        msg['content']=text; j['choices'][0]['message']=msg
    j['answer']=text; j['content']=text; j['message']=text; return j
class NoSuggestMiddleware:
    def __init__(self,app): self.app=app
    def __call__(self,environ,start_response):
        path=environ.get('PATH_INFO',''); alias_info={}
        if path == '/ask2':
            environ['PATH_INFO']='/ask'
            alias_info={'X-NoSuggest-Path':'/ask2','X-NoSuggest-Rewrite':'/ask'}
        status_headers={}; body_chunks=[]
        def _cap_start(status,headers,exc_info=None):
            status_headers['status']=status; status_headers['headers']=headers[:]
            return body_chunks.append
        app_iter=self.app(environ,_cap_start)
        try:
            for chunk in app_iter: body_chunks.append(chunk)
        finally:
            if hasattr(app_iter,'close'): app_iter.close()
        status=status_headers.get('status','200 OK')
        headers_raw=status_headers.get('headers',[])
        hdrs=[]
        headers=[]
        for item in headers_raw:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            k, v = item[0], item[1]
            headers.append((k, v))
            hdrs.append((str(k).lower(), v))
        ctype=next((v for (k,v) in hdrs if k=='content-type'), '')
        body=b''.join(body_chunks)
        add_headers=[]
        if alias_info:
            for k,v in alias_info.items(): add_headers.append((k,v))
        if 'application/json' in (ctype or '').lower() and body:
            try:
                j=json.loads(body.decode('utf-8','replace'))
                ans_raw=_extract_answer(j)
                cleaned,stripped=_strip_scaffold(ans_raw or '')
                fallback='none'
                if stripped and not cleaned:
                    ctx=_find_contexts(j)
                    if ctx:
                        cleaned=_mk_topmatches(ctx); fallback='top_matches'
                    elif ALLOW_NORESULTS_MSG:
                        cleaned="No Oracle documents matched your query."; fallback='no_results'
                if stripped: add_headers.append(('X-Suggestions-Stripped','1'))
                add_headers.append(('X-Server-Fallback', fallback))
                add_headers.append(('X-Compat-Fields','content,message,choices[0].message.content'))
                if cleaned:
                    j=_mirror_answer(j, cleaned)
                    body=json.dumps(j, ensure_ascii=False).encode('utf-8')
                    headers=[(k,v) for (k,v) in headers if str(k).lower()!='content-length']
                    headers.append(('Content-Length', str(len(body))))
            except Exception:
                pass
        if add_headers:
            existing={k.lower():i for i,(k,_) in enumerate(headers)}
            for (k,v) in add_headers:
                kl=k.lower()
                if kl in existing: headers[existing[kl]]=(k,v)
                else: headers.append((k,v))
        start_response(status, headers)
        return [body]
