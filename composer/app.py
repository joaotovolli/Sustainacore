import os, subprocess, json
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

UPSTREAM_ASK2_URL = os.environ.get("UPSTREAM_ASK2_URL", "http://127.0.0.1:8080/ask2")
SCAI_GEMINI_MODEL = os.environ.get("SCAI_GEMINI_MODEL", "gemini-2.5-flash")
SCAI_COMPOSE_DEFAULT = os.environ.get("SCAI_COMPOSE_DEFAULT", "off")
GEMINI_BIN = os.environ.get("GEMINI_BIN", "gemini")

def have_gemini():
    try:
        r = subprocess.run([GEMINI_BIN, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        return r.returncode == 0
    except Exception:
        return False

@app.get("/healthz")
def healthz():
    st = {"ok": True, "deps": {}}
    try:
        rr = requests.get(UPSTREAM_ASK2_URL.replace("/ask2","/healthz"), timeout=5)
        st["deps"]["upstream"] = {"ok": rr.ok}
        if not rr.ok: st["ok"] = False
    except Exception as e:
        st["deps"]["upstream"] = {"ok": False, "err": str(e)}; st["ok"] = False
    g = have_gemini()
    st["deps"]["gemini"] = {"ok": g}
    if not g: st["ok"] = False
    return jsonify(st), (200 if st["ok"] else 503)

def build_prompt(user_q, sources):
    lines = []
    for i, s in enumerate(sources[:5], start=1):
        title = (s.get("title") or "").strip()
        snippet = (s.get("snippet") or "").strip().replace("\n", " ")
        url = s.get("url") or ""
        lines.append(f"[{i}] {title}\n{snippet}\nURL: {url}\n")
    src_block = "\n".join(lines) if lines else "No matches."
    prompt = (
        "You are SustainaCore's ESG assistant. Answer clearly using ONLY the information in the provided sources.\n"
        "If the sources are weak or irrelevant, say you don't have strong matches and stop. Do not fabricate facts.\n"
        "Cite sources inline using [#] markers matching the list below.\n\n"
        f"User question: {user_q}\n\n"
        f"Sources:\n{src_block}\n\n"
        "Now provide the answer. If no strong matches, say: \"No strong matches in ESG_DOCS for this query.\""
    )
    return prompt

@app.get("/ask2")
def ask2():
    q = request.args.get("q", "").strip()
    k = int(request.args.get("k", "5"))
    compose = request.args.get("compose", SCAI_COMPOSE_DEFAULT).lower() == "on"
    if not q:
        return jsonify({"answer":"","sources":[],"composer":{"composed":False,"backend":None},"error":"empty_query"}), 400
    # call upstream
    try:
        rr = requests.get(UPSTREAM_ASK2_URL, params={"q": q, "k": k, "refine":"off"}, timeout=30)
        upstream = rr.json()
        sources = upstream.get("sources", [])
    except Exception as e:
        return jsonify({"answer":"","sources":[],"composer":{"composed":False,"backend":None},"error":"upstream_failed","detail":str(e)}), 502
    if not compose or not have_gemini():
        return jsonify({"answer":"", "sources": sources, "composer":{"composed":False,"backend":None}})
    # compose with gemini CLI
    prompt = build_prompt(q, sources)
    try:
        proc = subprocess.run([GEMINI_BIN, "-m", SCAI_GEMINI_MODEL, "-p", prompt],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if proc.returncode != 0:
            return jsonify({"answer":"", "sources": sources, "composer":{"composed":False,"backend":"gemini_cli","stderr":proc.stderr.strip()}}), 200
        answer = proc.stdout.strip()
        return jsonify({"answer":answer, "sources": sources, "composer":{"composed":True,"backend":"gemini_cli"}})
    except Exception as e:
        return jsonify({"answer":"", "sources": sources, "composer":{"composed":False,"backend":"gemini_cli","error":str(e)}}), 200

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8088)
