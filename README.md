# SustainaCore Gateway — `/ask2`

The SustainaCore Gateway is the production entry point for APEX experiences that rely on the `/ask2` retrieval augmented generation (RAG) service. The default pipeline is **Oracle-first**: question normalization, similarity search, and answer shaping are executed inside Oracle Autonomous Database 23ai with a thin Python layer for orchestration. Optional feature flags let you introduce downstream LLM refiners, small‑talk orchestration, or Gemini planning when you need additional polish.

---

## Quick Reference

| Contract | `POST /ask2` → `{ answer, sources, meta }` |
| --- | --- |
| Default mode | Oracle-first pipeline (no Gemini planning) |
| Optional layers | LLM Refiner (OpenAI), Ollama "micro" orchestrator, Gemini intent/routing gateway |
| Primary client | Oracle APEX (direct call or APEX proxy) |

### Why you might enable optional layers

- **LLM Refiner (`ask2_llm_refiner.py`)** – tightens tone and compactness over Oracle-retrieved snippets (OpenAI powered).
- **Ollama micro-orchestrator (`ask2_llm_orchestrator.py`)** – injects small-talk and safe JSON synthesis through a local Ollama runtime.
- **Gemini gateway (`app/retrieval/*`)** – adds intent classification, routing, and multi-tool planning (`GEMINI_FIRST_ENABLED=1`).

---

## Architecture at a Glance

```
APEX (Public UI)
│ REST (JSON)
▼
Flask / FastAPI app
├─ Oracle-first pipeline (default)
│ 1. Normalize + guard the incoming question
│ 2. Oracle 23ai VECTOR(384) + KNN (COSINE or DOT)
│ 3. Deduplicate hits; cap per source + per URL
│ 4. Shape `{answer, sources}` (decline gracefully on low similarity)
│
├─ (Optional) LLM Refiner — OpenAI completion over retrieved context
├─ (Optional) Ollama micro-orchestrator — small-talk + JSON safety layer
└─ (Optional) Gemini gateway — intent/routing/planning when flagged on
```

**Primary datastore**: Oracle Autonomous Database 23ai with `VECTOR(384)` embeddings, KNN index, ESG/news/document metadata, plus FT/context indexes.

---

## API

### `POST /ask2`

**Request**
```json
{
  "q": "Summarize TECH100 policy changes on AI governance since 2025-07",
  "k": 24
}
```

**Response**
```json
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
```

### `GET /healthz`
Simple readiness/liveness endpoint (used by CI and systemd).

---

## Modes, Feature Flags, and Tuning

| Layer | Files | When to use | How to enable |
| --- | --- | --- | --- |
| Oracle-first (default) | `app.py`, `app/retrieval/oracle_retriever.py` | Fastest, deterministic baseline | Enabled automatically |
| LLM Refiner (OpenAI) | `ask2_llm_refiner.py` | Improve fluency/compactness of answers | Export `OPENAI_API_KEY` and wrap as WSGI middleware |
| Ollama micro-orchestrator | `ask2_llm_orchestrator.py` | Local small-talk + guarded synthesis | Provide `OLLAMA_URL`, apply middleware |
| Gemini gateway | `app/retrieval/*` | Intent routing & planning | Set `GEMINI_FIRST_ENABLED=1` |

**Retrieval knobs**
- `RETRIEVER_MAX_FACTS`, `RETRIEVER_FACT_CAP`, `RETRIEVER_PER_SOURCE_CAP`
- `ORACLE_KNN_METRIC` (`COSINE` or `DOT`), `ORACLE_KNN_K`
- Rate limiting: `ASK2_RATE_WINDOW`, `ASK2_RATE_MAX`

**Deduplication policy**
- Canonical key = lower(`NORMALIZED_URL` or `SOURCE_ID` or `DOC_ID`).
- Later duplicates override earlier ones.
- Near-dupe collapse on `(title, hash(first 200 chars))`.
- Final response cap: ≤ 6 sources, ≤ 2 chunks per source, ≤ 1 per exact URL.
- Guardrail: low similarity ⇒ concise "no answer" message.

---

## Environment Configuration

Create a local env file (never commit to Git) and source it before running the service:

```ini
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
```

---

## Local Development

1. **Bootstrap**
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -U pip
   pip install -r requirements.txt
   ```
2. **Run the service**
   ```bash
   uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
   # or legacy Flask entrypoint
   python app.py
   ```
3. **Smoke tests**
   ```bash
   pytest -q
   curl -s http://127.0.0.1:8080/healthz
   curl -s -X POST http://127.0.0.1:8080/ask2 \
     -H "Content-Type: application/json" \
     -d '{"q":"hello","k":8}'
   ```

---

## Deployment (VM + systemd)

1. Copy the repository into `/opt/sustainacore-ai/` and configure a local env file with secrets (do **not** commit secrets).
2. Ensure Oracle Instant Client + wallet assets (via `TNS_ADMIN`) are present.
3. Optionally install dependencies for OpenAI, Ollama, or Gemini modes.
4. Restart the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart sustainacore-ai.service
   sudo systemctl status sustainacore-ai.service --no-pager
   ```
5. Verify:
   ```bash
   curl http://localhost:8080/healthz
   ```

**CORS**: `ask2_cors_mw.py` defines allowed origins. In production prefer the APEX proxy to centralize CSP, quotas, and origin controls.

---

## APEX Integration Tips

- Direct mode: create a REST data source that points at `/ask2`.
- Proxy mode (recommended): APEX → Proxy Process → Gateway. This keeps CSP and rate limits centralized, hides VM endpoints, and simplifies key rotation.
- Render answers plus the sanitized source list. A debug region showing `meta` is handy during development.

---

## Agentic DevOps Flow

Short iterations keep `/ask2` reliable:

1. **Codex Cloud** – describe the change (scope, acceptance, rollback). A focused PR is generated.
2. **GitHub** – single source of truth. CI runs linting, `pytest -q`, and basic `/ask2` contract tests.
3. **Codex CLI on the VM** – applies the PR, executes `ops/scripts/deploy_vm.sh`, restarts systemd, and posts logs.
4. On regression: create a rollback PR or toggle safety flags (e.g., `GEMINI_FIRST_ENABLED=0`).

**Repo layout**
```
api/                  # FastAPI/Flask app + middlewares
app/                  # Retrieval & routing packages
apex/                 # Dated APEX exports
db/                   # DDL, views (no data, no wallets)
ops/, tools/          # VM deployment helpers
tests/                # Contract & integration tests
```

**Ground rules**
- Never commit secrets or wallet bundles.
- Every PR description should carry Acceptance + Rollback notes.
- Prefer new, reviewable PRs over reopening conflicted ones.
- Keep APEX exports date-stamped; ensure DB DDL is idempotent; keep tests quick.

---

## Security & Troubleshooting

**Security posture**
- Public endpoints are read-only; ingestion endpoints remain key-gated.
- Enforce CORS via the middleware and lean on the APEX proxy in production.
- Low similarity signals trigger a concise "no answer" response to avoid hallucinations.

**Troubleshooting checklist**
- **Oracle connectivity**: confirm `TNS_ADMIN`, `DB_DSN`, wallet ACLs, and listener reachability.
- **Slow answers**: validate the KNN index, retrieval caps, and dedup policy; avoid excessive `k` values.
- **LLM timeouts**: disable refiners and fall back to Oracle-first while investigating.
- **Git conflicts**: abandon conflicted PRs and raise a fresh, small diff.

---

## License

License TBD (MIT or Apache-2.0 recommended).
