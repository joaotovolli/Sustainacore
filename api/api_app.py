import os
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # dev-friendly

@app.route("/ask", methods=["POST"])
def ask():
    payload = request.get_json(force=True) or {}
    q = (payload.get("question") or "").strip()
    top_k = int(payload.get("top_k", 3))
    if not q:
        return jsonify(error="missing question"), 400
    return jsonify(answer=f"Hello from API. You asked: {q}", sources=[])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
