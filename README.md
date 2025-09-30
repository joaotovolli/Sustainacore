# SustainaCore Gateway — `/ask2` (Oracle-First RAG, with Optional LLM/Gemini Layers)

This service powers the SustainaCore assistant endpoint **`/ask2`**, used by **Oracle APEX**.  
**Current default:** **Oracle-first RAG** — retrieval and shaping are driven by Oracle Autonomous Database (23ai vector search) and lightweight Python glue.  
Optional layers (feature-toggled): **LLM Refiner (OpenAI)**, **Ollama micro-orchestrator**, and a **Gemini gateway** you can enable when needed.

---

## TL;DR

- **Contract (stable):** `POST /ask2` → `{ answer, sources, meta }`
- **Default mode:** Oracle-first (no Gemini planning).  
- **Optional:**  
  - **LLM Refiner** (`ask2_llm_refiner.py`) — composes/compacts answer text over retrieved contexts (OpenAI).  
  - **Ollama “micro”** (`ask2_llm_orchestrator.py`) — local LLM small-talk + safe synthesis.  
  - **Gemini gateway** (`app/retrieval/*`) — intent/routing/planning if `GEMINI_FIRST_ENABLED=1`.  
- **APEX integration:** call `/ask2` directly or via APEX proxy (recommended for CSP/rate-limits).

---

## Architecture

APEX (Public UI)
│ REST (JSON)
▼
Flask/FastAPI app
├─ Oracle-first pipeline (default)
│ 1) Normalize/guard question
│ 2) Oracle 23ai: VECTOR(384) + KNN (COSINE/DOT)
│ 3) Deduplicate, cap per source & per URL
│ 4) Shape {answer, sources} (no hallucinations; “no answer” on low similarity)
│
├─ (Optional) LLM Refiner (OpenAI): tighten/compact answer over retrieved snippets
├─ (Optional) Ollama micro-orchestrator: small-talk + JSON-safe synthesis
└─ (Optional) Gemini gateway: intent/routing/planning (feature flag)

yaml
Copy code

**Data store:** Oracle ADB 23ai tables with `VECTOR(384)` embeddings + KNN index; ESG/news/doc metadata; constraints and FT/context indexes for speed.

---

## Endpoints

### `POST /ask2`
**Request**
```json
{
  "q": "Summarize TECH100 policy changes on AI governance since 2025-07",
  "k": 24
}
Response

json
Copy code
{
  "answer": "TECH100 firms disclosed policy refreshes focused on model oversight and incident reporting. Examples include ...",
  "sources": [
    "Responsible AI — Company A (2025-08-12)",
    "AI Governance Update — Company B (2025-07-30)"
  ],
  "meta": {
    "k": 6,
    "took_ms": 1280,
    "model_info": {"embed": "AI$MINILM_L6_V2"}
  }
}
GET /healthz
Liveness/readiness (used by systemd and CI).

Modes & Feature Flags
Layer	File(s)	When to use	How to enable
Oracle-first (default)	app.py, app/retrieval/oracle_retriever.py	Fast, deterministic, lowest cost	Do nothing (default path)
LLM Refiner (OpenAI)	ask2_llm_refiner.py	Improve fluency/compactness of answer	Set OPENAI_API_KEY and run as WSGI middleware
Ollama micro-orchestrator	ask2_llm_orchestrator.py	Local small-talk + safe synthesis	Set OLLAMA_URL and wrap app with middleware
Gemini gateway	app/retrieval/*	Intent/routing/planning at front door	GEMINI_FIRST_ENABLED=1 (off by default)

Additional knobs (see Environment):

Retrieval caps: RETRIEVER_MAX_FACTS, RETRIEVER_FACT_CAP, RETRIEVER_PER_SOURCE_CAP

KNN metric/K: ORACLE_KNN_METRIC (COSINE/DOT), ORACLE_KNN_K

Rate limits: ASK2_RATE_WINDOW, ASK2_RATE_MAX

Request Shaping & Dedup (Oracle-First)
Scope-first SQL + KNN over VECTOR(384) embeddings.

Dedup rules:

Canonical key = lower(NORMALIZED_URL or SOURCE_ID or DOC_ID).

Later duplicates override earlier ones.

Near-dupe collapse on (title, hash(first 200 chars)).

Final cap: ≤ 6 sources, ≤ 2 chunks per source, ≤ 1 per exact URL.

Guardrails: If similarity below threshold → concise “no answer” response.

Environment
Create and source an env file (never commit to Git):

ini
Copy code
# --- Oracle ---
DB_USER=...
DB_PASS=...
DB_DSN=dbri4x6_high
TNS_ADMIN=/path/to/Wallet_dbRI4X6
ORACLE_KNN_METRIC=COSINE
ORACLE_KNN_K=24
ORACLE_EMBED_MODEL=AI$MINILM_L6_V2

# --- Rate limits & caps ---
ASK2_RATE_WINDOW=10
ASK2_RATE_MAX=8
RETRIEVER_MAX_FACTS=8
RETRIEVER_FACT_CAP=6
RETRIEVER_PER_SOURCE_CAP=2

# --- Optional LLM Refiner (OpenAI) ---
OPENAI_API_KEY=...
SCAI_LLM_MODEL=gpt-4o-mini
SCAI_LLM_MAX_TOKENS=700

# --- Optional Ollama micro-orchestrator ---
OLLAMA_URL=http://127.0.0.1:11434
LLM_MODEL=llama3.1:8b

# --- Optional Gemini gateway (OFF by default) ---
GEMINI_FIRST_ENABLED=0
GEMINI_API_KEY=...
GEMINI_MODEL_INTENT=gemini-1.5-flash
GEMINI_MODEL_COMPOSE=gemini-1.5-pro
ASK2_LATENCY_BUDGET_MS=4500
Local Dev
bash
Copy code
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -r requirements.txt

# Export your env (see above), then:
uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
# or Flask entry for legacy routes:
python app.py
Smoke tests:

bash
Copy code
pytest -q
curl -s http://127.0.0.1:8080/healthz
curl -s -X POST http://127.0.0.1:8080/ask2 -H "Content-Type: application/json" -d '{"q":"hello","k":8}'
Deployment (VM + systemd)
Copy code to /opt/sustainacore-ai/ and an env file with secrets (never in Git).

Ensure Oracle Instant Client + Wallet on the VM (TNS_ADMIN).

(Optional) Install Ollama/OpenAI/Gemini depending on your selected mode.

Restart:

bash
Copy code
sudo systemctl daemon-reload
sudo systemctl restart sustainacore-ai.service
sudo systemctl status sustainacore-ai.service --no-pager
Verify:

bash
Copy code
curl http://localhost:8080/healthz
CORS: ask2_cors_mw.py reflects allowed origins; use the APEX proxy in production for CSP, quotas, and origin control.

APEX Integration
Direct call: REST data source calling /ask2.

Proxy (recommended): APEX→Proxy Process→Gateway — centralizes CSP/rate-limits, hides VM endpoint, and simplifies key rotation.

Render answer and the cleaned sources list; optionally toggle a debug region for meta during development.

Agentic DevOps — Codex Cloud → GitHub → Codex CLI/VM
Goal: Short, auditable iterations with automatic build/test/deploy, no secrets in Git.

Operating model

Codex Cloud

You describe the change (scope + acceptance & rollback).

Codex creates a short PR in GitHub (small diff, clear commit message).

GitHub (source of truth)

Repo layout:

bash
Copy code
api/                  # FastAPI/Flask app + middlewares
app/                  # Retrieval & routing packages
db/                   # DDL, views (no data, no wallets)
apex/                 # Dated APEX exports
tools/, ops/scripts/  # VM deploy helpers
tests/                # Contract & integration tests
CI runs: lint, unit tests (pytest -q), basic contract tests for /ask2.

Codex CLI on VM

Applies the approved PR, runs ops/scripts/deploy_vm.sh, restarts systemd, posts logs.

If a regression is detected, it proposes a rollback PR or toggles flags (e.g., GEMINI_FIRST_ENABLED=0) to keep /ask2 contract green.

Ground rules

No secrets/wallets in Git. Use systemd env files & APEX Web Credentials.

Always include Acceptance & Rollback notes in each PR description.

Prefer new PRs over reopening conflicted ones; keep diffs small and reviewable.

Keep APEX exports date-stamped; DB DDL idempotent; tests fast.

Agent crib sheet (same as AGENTS.md)

Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true

Run (dev): uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080

Deploy (VM): ops/scripts/deploy_vm.sh

Security
Public endpoints are read-only; ingestion, if any, is key-gated.

Strict CORS and APEX proxy recommended for production.

On low similarity or empty evidence, the service declines to answer.

Troubleshooting
DB connectivity: check TNS_ADMIN, DB_DSN, wallet ACLs, listener reachability.

Slow answers: verify KNN index, caps (RETRIEVER_*), and dedup rules; avoid over-K.

LLM timeouts: disable refiner, fall back to Oracle-first (toggle off LLM/Gemini).

Conflicts in Git: abandon conflicted PR, open a fresh short PR with Codex Cloud.

License
TBD (MIT/Apache-2.0 recommended).

bash
Copy code
