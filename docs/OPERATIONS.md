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
- `SIMILARITY_FLOOR` (default 0.58) prevents low-signal matches. If the best score falls below the floor the service returns the “insufficient context” message instead of forcing an answer.

## Observability
- Embedding parity, readiness results, and multi-hit orchestrator fallbacks emit structured logs (`sustainacore.embed`, `app.readyz`, `app.multihit`).
- Retrieval responses include `meta.scope`, `meta.filters`, and the detected `top_score` to aid debugging.
- The `/ask2_direct` and `/ask2_simple` endpoints echo `meta.insufficient_context` and the configured scope for diagnostics.
