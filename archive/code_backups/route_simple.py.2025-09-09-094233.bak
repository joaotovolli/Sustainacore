from flask import request, jsonify
from db_helper import top_k_by_vector
import os, requests
from traceback import format_exc

def register(app):
    @app.get("/ask2_simple")
    def ask2_simple():
        q = request.args.get("q","").strip()
        k = int(request.args.get("k","5") or 5)
        if not q:
            return jsonify({"answer":"", "sources":[]})

        ollama_url  = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
        embed_model = os.getenv("OLLAMA_EMBED_MODEL", "all-minilm")

        # 1) Embed the query
        try:
            r = requests.post(f"{ollama_url}/api/embeddings",
                              json={"model": embed_model, "prompt": q},
                              timeout=15)
            r.raise_for_status()
            emb = r.json().get("embedding", [])
        except Exception as e:
            return jsonify({"answer":"", "sources":[], "error":f"embed_failed: {e}", "trace": format_exc()}), 502

        # 2) Vector search (read-only)
        try:
            rows = top_k_by_vector(emb, k=k)
        except Exception as e:
            return jsonify({"answer":"", "sources":[], "error":f"db_failed: {e}", "trace": format_exc()}), 500

        # 3) Shape response
        sources = []
        for row in rows:
            dist  = row.get("dist")
            score = (1.0 - float(dist)) if dist is not None else None
            sources.append({
                "id": f"{row.get('doc_id','')}-{row.get('chunk_ix','')}",
                "title": row.get("title") or "",
                "score": round(score, 4) if score is not None else None,
                "snippet": (row.get("chunk_text") or "")[:320],
                "url": row.get("source_url")
            })
        return jsonify({"answer":"", "sources": sources})
