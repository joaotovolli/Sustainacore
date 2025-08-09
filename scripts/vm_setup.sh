#!/usr/bin/env bash
set -e
echo "== Sustainacore VM setup =="
if [ -z "$DB_DSN" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASS" ]; then
  echo "Export DB_DSN, DB_USER, DB_PASS then re-run. Example:"
  echo 'export DB_DSN="adb_region:1522/your_adb_high"'
  echo 'export DB_USER="ESG"'
  echo 'export DB_PASS="password"'
  exit 1
fi
sudo dnf -y install python3-venv || sudo yum -y install python3-venv || true
mkdir -p ~/sustainacore && cd ~/sustainacore
python3 -m venv venv && source venv/bin/activate
pip install --upgrade pip
pip install flask cx_Oracle numpy requests sentence-transformers yfinance
sudo yum install -y oracle-instantclient-release-el8 || true
sudo yum install -y oracle-instantclient-basic oracle-instantclient-devel || true
echo 'export LD_LIBRARY_PATH=/usr/lib/oracle/23/client64/lib:$LD_LIBRARY_PATH' >> ~/.bashrc
curl -fsSL https://ollama.com/install.sh | sh
ollama pull mistral:7b-instruct-q4_K_M
cat > api_app.py <<'PY'
from flask import Flask, request, jsonify
import os, requests, numpy as np, cx_Oracle
DB_DSN=os.getenv("DB_DSN"); DB_USER=os.getenv("DB_USER","ESG"); DB_PASS=os.getenv("DB_PASS")
OLLAMA_URL=os.getenv("OLLAMA_URL","http://localhost:11434")
EMBED_MODEL=os.getenv("EMBED_MODEL","sentence-transformers/all-MiniLM-L6-v2")
_model=None
def embed(t):
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model=SentenceTransformer(EMBED_MODEL)
    v=_model.encode([t])[0]; return np.array(v, dtype=np.float32)
def conn(): return cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN, encoding="UTF-8")
def topk(q,k=5):
    e=embed(q).tolist(); vec=",".join(str(float(x)) for x in e)
    sql=f"SELECT title, source_url, chunk_text FROM ESG_DOCS ORDER BY VECTOR_DISTANCE(embedding, VECTOR[{vec}]) FETCH FIRST :k ROWS ONLY"
    with conn() as c: cur=c.cursor(); cur.execute(sql, k=k); rows=cur.fetchall()
    return [{"title":r[0],"url":r[1],"text":r[2]} for r in rows]
def call_llm(p):
    r=requests.post(f"{OLLAMA_URL}/api/generate", json={"model":"mistral:7b-instruct-q4_K_M","prompt":p,"stream":False}, timeout=120)
    r.raise_for_status(); return r.json().get("response","")
def tpl(q,ctxs):
    ctx="\n\n".join([f"[{i+1}] {c['text']}" for i,c in enumerate(ctxs)])
    return f"You are Sustainacore's assistant. Answer using ONLY the context. If unknown, say so.\nQuestion: {q}\n\nContext:\n{ctx}\n\nCite [1],[2]."
app=Flask(__name__)
@app.route("/ask", methods=["POST"])
def ask():
    d=request.get_json(force=True) or {}; q=(d.get("question") or "").strip(); k=int(d.get("top_k",5))
    if not q: return jsonify({"error":"question required"}), 400
    ctx=topk(q,k); ans=call_llm(tpl(q,ctx)); return jsonify({"answer":ans,"contexts":ctx})
if __name__=="__main__": app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")))
PY
echo "Run: source ~/sustainacore/venv/bin/activate && export DB_DSN DB_USER DB_PASS && python api_app.py"
