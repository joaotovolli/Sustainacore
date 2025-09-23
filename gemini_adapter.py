# /opt/sustainacore-ai/gemini_adapter.py (shim to CLI)
import os, json, subprocess, typing as t

GEMCLI = "/opt/sustainacore-ai/.gemini_cli_router/gemcli.py"
VENV_PY = os.environ.get("SUSTAINACORE_PY", "/opt/sustainacore-ai/.venv/bin/python")

def _run(subcmd, payload, timeout=10.0, text_out=True):
    data = json.dumps(payload).encode("utf-8")
    p = subprocess.Popen([VENV_PY, GEMCLI, subcmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate(input=data, timeout=timeout)
    if p.returncode != 0:
        return ""
    return out.decode("utf-8","ignore").strip()

def generate(prompt:str, context:t.Optional[t.List[t.Union[str,dict]]]=None, **kwargs)->str:
    q = (prompt or "").strip()
    ctx = []
    for s in (context or [])[:8]:
        if isinstance(s, dict):
            ctx.append(s.get("snippet") or s.get("text") or s.get("content") or "")
        else:
            ctx.append(str(s))
    mode = "rag" if ctx else "orientation"
    facts = []
    try:
        facts = json.load(open("/opt/sustainacore-ai/.gemini_cli_router/config/site_facts.json")).get("bullets",[])
    except Exception:
        pass
    payload = {"mode":mode,"q":q,"context":ctx,"site_facts":facts if mode=="orientation" else []}
    return _run("synth", payload, timeout=10.0, text_out=True) or ""

def ask(question:str, **kwargs)->str:
    return generate(question, **kwargs)

class LLMClient:
    def __init__(self, **kwargs): pass
    def generate(self, prompt:str, context=None, **kw): return generate(prompt, context=context, **kw)
