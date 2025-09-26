# Release Notes

## fix(rag): lock embedder + resilient /ask2 + scoped retrieval + readiness probe
- Lock embedding model configuration via `EMBED_MODEL_NAME` with startup parity checks and optional strict enforcement.
- Harden `/ask2` orchestration to ignore brittle headers, provide graceful fallbacks, and ensure normal traffic succeeds without `X-Orch` overrides.
- Add scoped retrieval with intent detection, similarity floor guardrails, and deduplicated context assembly across all vector endpoints.
- Introduce `/readyz` Oracle vector probe alongside documentation and configuration defaults for operations teams.
- Expanded unit and integration tests cover embedding parity, orchestrator behavior, retrieval scoping, and readiness probing.
