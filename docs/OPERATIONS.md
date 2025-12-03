# Sustainacore Operations Guide

## Embedding parity
- `EMBED_MODEL_NAME` is the single source of truth for the embedding model used by the service. The previous `OLLAMA_EMBED_MODEL` is read only for backwards compatibility.
- At worker start a parity probe reads the Oracle vector column metadata. Dimension or model mismatches log a warning by default.
- Set `STRICT_EMBED_PARITY=true` in production to fail fast when the database corpus does not match the configured model. This will stop the process during boot instead of serving stale embeddings.
- Parity logs are emitted under the `sustainacore.embed` logger and include the provider, expected and actual dimensions, and the model names.

## Readiness probe
- `/readyz` performs a live embedding call followed by a vector search against Oracle. It fails with HTTP 503 if the database is unreachable, the vector dimensions drift, or the embedding call fails.
- Successful probes log the number of rows returned and whether retrieval scoping is enabled under the `app.readyz` logger.
- `/healthz` remains a lightweight liveness ping and should continue to be used for basic service monitoring.

## Retrieval scoping
- Questions are routed into focused corpora:
  - Membership intent (e.g. “Is Microsoft in TECH100?”) scopes to membership/constituent sources and applies company filters when a ticker/company is detected.
  - Company profile or snapshot questions scope to the company profile docset.
  - Regulatory questions scope to regulatory sources.
  - “What is this website?” routes to site/about documentation.
- Requests can override scope via query or JSON parameters: `docset`, `namespace`, `ticker`, or `company`.
- Set `RETRIEVAL_SCOPING=off` to temporarily disable scoping if wider searches are required.
- `RETRIEVAL_TOP_K` controls the initial candidate pool (defaults to 8). Increase cautiously if additional recall is required.
- `SIMILARITY_FLOOR` (default 0.58) defines the similarity threshold used for retrieval quality checks.
- `SIMILARITY_FLOOR_MODE` controls how the floor is applied:
  - `off` — bypass the check entirely.
  - `monitor` *(default)* — log when the top-1 score falls below the floor but continue returning the retrieved answer and contexts.
  - `enforce` — replace low-similarity answers with the standard insufficient-context message and omit `contexts`/sources to avoid citing weak evidence.

## Small-talk handling
- `/ask2` now short-circuits greetings and simple help messages (`hi`, `hello`, `hey`, `thanks`, `thank you`, `help`, `goodbye`).
- These requests return a short professional acknowledgement and 2–4 suggested follow-up prompts without calling retrieval or emitting `contexts`.

## Ask2 & related VM1 APIs
- `/api/ask2` accepts `POST` JSON with `{"user_message": "..."}` (aliases: `question`/`q`/`text`) plus an optional `k`/`top_k` parameter. Requests must include `Authorization: Bearer $API_AUTH_TOKEN` when the token is configured on VM1.
- `/api/news` and `/api/tech100` reuse the same auth guard and will return 401 when the `Authorization: Bearer $API_AUTH_TOKEN` header is missing or mismatched. Keep the token set in the VM1 environment and provide the matching value to any downstream consumers (e.g., VM2 Django).
- The pipeline is Gemini-first: intent detection → planner → Oracle retrieval → composer. Planner output is kept in `meta.plan`/`meta.debug`; user-facing fields (`answer`, `reply`, `message`, `content`) are natural-language replies only. When no facts are found, the service returns a polite fallback instead of planner JSON.
- `/api/health` runs with the same auth gate and reports `oracle` + `model` status for lightweight smoke checks.

## Oracle connectivity (VM1)
- VM1 uses the Oracle thick client with wallet-based DSN. Connection kwargs include `stmtcachesize=0`, `retry_count=0`, and `tcp_connect_timeout` (default 5s) to fail fast instead of hanging on network issues.
- Expected environment variables (no secrets in Git):
  - `DB_USER` (defaults to `WKSP_ESGAPEX`)
  - `DB_PASSWORD`/`DB_PASS`/`DB_PWD`
  - `DB_DSN` (wallet entry, e.g., `dbri4x6_high`)
  - `TNS_ADMIN` (wallet directory) and `WALLET_PWD`
- The helper script `tools/test_db_connect.py` uses the same env vars. Load the VM1 env (e.g., `/etc/sustainacore/db.env` and service drop-ins) and run `python tools/test_db_connect.py` to verify wallet, password, and network health. Success prints `connect_ok` with latency; failures show the Oracle error and elapsed time.

### Password rotation / ORA-28001
- Rotate the ADB password for `WKSP_ESGAPEX` directly in the database console.
- Update VM1 environment sources (e.g., `/etc/sustainacore/db.env` and systemd override files) with the new `DB_PASSWORD`/`DB_PASS` and any updated `WALLET_PWD`, then restart the API service so Gunicorn picks up the change.
- Validate with `tools/test_db_connect.py` and `/api/health` after the restart. Keep credentials out of Git and CI artifacts.

## Observability
- Embedding parity, readiness results, and multi-hit orchestrator fallbacks emit structured logs (`sustainacore.embed`, `app.readyz`, `app.multihit`).
- Retrieval responses include `meta.scope`, `meta.filters`, and the detected `top_score` to aid debugging.
- The `/ask2_direct` and `/ask2_simple` endpoints echo `meta.insufficient_context` and the configured scope for diagnostics.
