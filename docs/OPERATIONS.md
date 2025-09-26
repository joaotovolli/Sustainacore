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

## Observability
- Embedding parity, readiness results, and multi-hit orchestrator fallbacks emit structured logs (`sustainacore.embed`, `app.readyz`, `app.multihit`).
- Retrieval responses include `meta.scope`, `meta.filters`, and the detected `top_score` to aid debugging.
- The `/ask2_direct` and `/ask2_simple` endpoints echo `meta.insufficient_context` and the configured scope for diagnostics.
