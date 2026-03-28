# Sustainacore Operations Guide
<!-- cspell:ignore cutover readlink sustainacoredb sysconfig -->

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
  - `DB_DSN` (wallet entry, e.g., `sustainacoredb_tp`)
  - `TNS_ADMIN` (wallet directory) and `WALLET_PWD`
- The helper script `tools/test_db_connect.py` uses the same env vars. Load the VM1 env (e.g., `/etc/sustainacore/db.env` and service drop-ins) and run `python tools/test_db_connect.py` to verify wallet, password, and network health. Success prints `connect_ok` with latency; failures show the Oracle error and elapsed time.

### Password rotation / ORA-28001
- Rotate the ADB password for `WKSP_ESGAPEX` directly in the database console.
- Update VM1 environment sources (e.g., `/etc/sustainacore/db.env` and systemd override files) with the new `DB_PASSWORD`/`DB_PASS` and any updated `WALLET_PWD`, then restart the API service so Gunicorn picks up the change.
- Validate with `tools/test_db_connect.py` and `/api/health` after the restart. Keep credentials out of Git and CI artifacts.

## Production DB source of truth
- Active production ADB: `SustainacoreDB`.
- Legacy rollback ADB: `dbRI4X6` / `apexRI4X6`. Leave it untouched during stabilization; do not run cleanups or retire it until the cooling-off period ends.
- VM1 runtime source of truth:
  - env: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/app.env`, `/etc/sustainacore-ai/secrets.env`
  - wallet: `/opt/adb_wallet`
  - primary service: `sustainacore-ai.service`
  - dependent timers/services: `sc-telemetry-report.service`, `sc-idx-pipeline.service`
  - SC_IDX units must explicitly set `TNS_ADMIN=/opt/adb_wallet` so the index pipeline does not inherit a stale wallet path from shared env files
- VM2 runtime source of truth:
  - env: `/etc/sustainacore.env`, `/etc/sustainacore/db.env`, `/etc/sysconfig/sustainacore-django.env`
  - wallet: `/opt/adb_wallet_tp`
  - primary services: `gunicorn.service`, `gunicorn-preview.service`
  - dependent timer/service: `sc-web-telemetry-rollup.timer`, `sc-web-telemetry-rollup.service`
- Fast verification:
  - VM1: run a service-like `select global_name from global_name`
  - VM2: run `scripts/vm2_manage.sh shell -c "select global_name from global_name"`
  - both must resolve to `GE3654DEB76FCC9_SUSTAINACOREDB`
- Rollback outline:
  - restore the previous wallet and `/etc/sustainacore/db.env` on each VM
  - restart the affected services
  - re-run homepage, login, telemetry, Ask2, and DB identity checks before declaring rollback complete

## Post-cutover completeness
- The active `SustainacoreDB` schema must include the AI regulation dataset used by the website:
  - `FACT_INSTRUMENT_SNAPSHOT`
  - `DIM_JURISDICTION`
  - `DIM_INSTRUMENT`
  - `FACT_SNAPSHOT_OBLIGATION`
  - `FACT_SNAPSHOT_MILESTONE_DATE`
  - `BRG_SNAPSHOT_SOURCE`
  - `DIM_SOURCE`
  - and the related bridge/dimension tables loaded by `infra/geo_ai_reg`
- Ask2 correctness depends on `ESG_DOCS` retrieval assets being present on the active DB:
  - `ESG_DOCS.EMBEDDING` populated for the live corpus
  - `ESG_DOCS_TEXT_IDX` present for Oracle Text fallback
  - a vector index is optional for correctness on the current corpus size, but embeddings are not
- Tech100 index freshness depends on `SC_IDX_LEVELS` being current on the active DB. The displayed “Latest data through …” date on `/tech100/index/` comes from `SC_IDX_LEVELS`, not from the legacy VM1 `/api/tech100` helper.

## Observability
- Embedding parity, readiness results, and multi-hit orchestrator fallbacks emit structured logs (`sustainacore.embed`, `app.readyz`, `app.multihit`).
- Retrieval responses include `meta.scope`, `meta.filters`, and the detected `top_score` to aid debugging.
- The `/ask2_direct` and `/ask2_simple` endpoints echo `meta.insufficient_context` and the configured scope for diagnostics.

## SC_IDX orchestration (VM1)
- The SC_IDX / TECH100 operational pipeline now uses LangGraph as the primary orchestration layer.
- Operator entrypoint: `python3 tools/index_engine/run_pipeline.py`.
- Primary scheduler: `sc-idx-pipeline.timer` -> `sc-idx-pipeline.service`.
- Daily operator report scheduler: `sc-telemetry-report.timer` -> `sc-telemetry-report.service`.
- The scheduler-facing repo path is `/home/opc/Sustainacore`; on production VM1 it should resolve to a versioned release checkout under `/opt/sustainacore-sc-idx-*`.
- Verify the live scheduler revision with:
  - `readlink -f /home/opc/Sustainacore`
  - `sudo -n -u opc git -C "$(readlink -f /home/opc/Sustainacore)" rev-parse --short HEAD`
- The graph is deliberately thin and low-memory:
  - no daemon
  - no Redis/Celery
  - bounded retries
  - bounded readiness fallbacks
  - repo-native JSON artifacts plus Oracle-backed stage state
- Run artifacts:
  - reports: `tools/audit/output/pipeline_runs/`
  - telemetry: `tools/audit/output/pipeline_telemetry/`
  - daily operator reports: `tools/audit/output/pipeline_daily/`
  - health snapshot: `tools/audit/output/pipeline_health_latest.txt`
  - all of the above expose the active `repo_root` / `repo_head` so operators can verify the
    deployed checkout
- Compact run status codes in `SC_IDX_JOB_RUNS`:
  - `OK`, `DEGRADED`, `SKIP`, `ERROR`, `BLOCKED`
- SC_IDX systemd units treat exit code `2` as a terminal blocked/non-advancing outcome, not a crash to auto-restart.
- Failure email policy:
  - `failed` and `blocked` attempt email by default
  - `stale` attempts email by default, even when the graph technically concluded
  - repeated `success_with_degradation` attempts email by default after
    `SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD` consecutive degraded runs
  - single `success_with_degradation` only emails when `SC_IDX_EMAIL_ON_DEGRADED=1`
  - `daily_budget_stop` only emails when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`
  - `clean_skip` only stays quiet when freshness is not stale
  - smoke runs do not email
- Alert duplicate-suppression state is stored in `SC_IDX_ALERT_STATE`; the gate is only marked after successful SMTP delivery.
- SMTP env names for SC_IDX alerts: `SMTP_USER`, `SMTP_PASS`, `MAIL_FROM`, `MAIL_TO`.
- Freshness guardrails:
  - `SC_IDX_STALE_ALLOWED_LAG_DAYS` bounds tolerated lag before the operator verdict becomes `Stale`
  - `SC_IDX_TRADING_DAY_FALLBACK_MAX_GAP` bounds the synthetic weekday fallback used when calendar
    refresh degrades on a timeout or 403
- Daily report recipients resolve in this order:
  - `SC_IDX_DAILY_REPORT_RECIPIENTS`
  - `TELEMETRY_REPORT_RECIPIENTS`
  - `MAIL_TO`
- See [SC_IDX LangGraph orchestration](index_engine_langgraph_orchestration.md) for node layout, retry rules, and verification.
