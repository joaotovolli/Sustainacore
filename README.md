# Sustainacore

![CI](https://img.shields.io/badge/status-production_ready-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Contributions](https://img.shields.io/badge/contributions-welcome-brightgreen)

## Executive Summary
Sustainacore delivers ESG knowledge retrieval and orchestration services that unify retrieval augmented generation, workflow automation, and governance tooling. This mono-repo tracks the production FastAPI surface, the Django public site served from VM2 (Nginx → Gunicorn), APEX integrations used for secondary/admin flows, database artifacts, and supporting infrastructure that keep the platform reliable for enterprise deployments.

## Quick Start
1. Clone the repository and create a Python virtual environment: `python3 -m venv .venv && source .venv/bin/activate`.
2. Install dependencies: `pip install -U pip && pip install -r requirements.txt`.
3. Export environment variables from `.env` (or copy from `.env.sample`).
4. Launch the retrieval API locally with `uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080`.
5. Run the regression suite with `pytest -q` before committing changes.

### CI sanity check
- The `Sanity` GitHub Action installs the Python dependencies from `requirements.txt` plus Django test extras, sets
  `PYTHONPATH` to include the repo root and `website_django/`, and seeds a placeholder `DJANGO_SECRET_KEY` for settings
  import.
- It runs a fast smoke subset (`tests/test_api_tech100.py` and `website_django/core/tests/test_api_client.py`) to catch
  import and API regression issues without pulling in the full suite.

### ChatGPT / Codex code reviews
- Reviews never run automatically on pull requests; they only trigger when explicitly requested.
- To opt in on a PR, add the `codex-review` label. Remove the label to silence new reviews on subsequent pushes.
- The connector ignores large or generated areas (`archive/**`, `datasets/**`, `website_django/venv/**`,
  `website_django/static/**`, `website_django/staticfiles/**`, `tests/fixtures/**`, `docs/**`, `infra/**`) so the credit
  usage focuses on meaningful code changes.

## VM1/VM2 architecture at a glance
- **VM1 (API/RAG/Oracle):** Hosts the Flask/Gunicorn API with `/api/health` and `/api/ask2`. Ask2 now runs a Gemini-first pipeline (intent → planner → Oracle retriever → composer) that returns natural-language answers plus sources. Planner JSON stays in the `meta` block for debugging; user-facing fields (`answer`, `reply`, `message`, `content`) are text-only fallbacks when no facts are found.
- **VM2 (Django website):** Runs the public site in `website_django/` and proxies chat requests to VM1. `/ask2/` serves a minimal HTML page that POSTs to `/ask2/api/`, which in turn forwards `{ "user_message": "..." }` to VM1 with an optional bearer token. Backend connectivity is controlled by `BACKEND_API_BASE` (default `http://10.0.0.120:8080`) and `BACKEND_API_TOKEN` from the environment.

### Email code auth (VM1)
- `POST /api/auth/request-code` → `{ "email": "user@example.com" }` responds with `{ "ok": true }`.
- `POST /api/auth/verify-code` → `{ "email": "user@example.com", "code": "123456" }` responds with `{ "token": "...", "expires_in_seconds": 2592000 }`.
- `AUTH_TOKEN_SIGNING_KEY` must be set in the environment for token signing.

**Public site routing:**
- `sustainacore.org` DNS is managed in IONOS and points to VM2.
- VM2 terminates HTTPS with Nginx and proxies to Gunicorn → Django for the main site.
- Oracle APEX remains available for administrative/legacy workflows only; it is not the primary front-end.

## VM2 Django Website & Deployment
- **What VM2 is:** VM2 runs the public Django website for Sustainacore, served by Gunicorn + Nginx. The repository is checked out on VM2 at `/opt/code/Sustainacore`, and the Django project lives in `website_django/`.
- **Environment variables and secrets:** Gunicorn loads production settings from `/etc/sustainacore.env` via `EnvironmentFile`. A repo-local `.env.vm2` (present only on VM2 and never committed) is sourced by `deploy_vm2_website.sh` so that values such as `DJANGO_SECRET_KEY=***`, `BACKEND_API_BASE`, and `BACKEND_API_TOKEN` are available while running `manage.py` commands.
- **Deployment flow:** The "Deploy VM2 Django Website" GitHub Action SSHs to VM2 and runs `bash ./deploy_vm2_website.sh`, making that script the single entry point. Manual deploys follow the same path:
  ```bash
  cd /opt/code/Sustainacore
  git fetch origin main
  git reset --hard origin/main
  bash ./deploy_vm2_website.sh
  ```
- **Ask SustainaCore chat:** `/ask2/` renders the chat UI and `/ask2/api/` proxies to VM1 `/api/ask2`. Replies surface `reply`/`content`/`message` text only; the planner JSON stays on VM1 in `meta`.

See [docs/OPERATIONS.md](docs/OPERATIONS.md) for VM1 Ask2/Oracle details and [docs/vm2-website-deploy.md](docs/vm2-website-deploy.md) for a deeper walkthrough of the VM2 setup.

For production deployments, `config/prod.env.example` captures the supported feature flags (persona and normalization). Copy the file to your environment tooling as needed.

## Eval Pack 
To validate persona quality and request normalization end-to-end:

1. Launch the API locally: `uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080`.
2. In a separate shell, enable the feature flags and run the harness:
   ```bash
   export PERSONA_V1=1 REQUEST_NORMALIZE=1
   python scripts/run_eval.py
   ```
3. The script reads `eval/eval.jsonl`, posts each case to `/ask2`, and fails if grounding, latency, or formatting regress.

Set `ASK2_URL` if your server runs on a different host or port.

> **CI note:** The persona eval workflow runs only when the `ASK2_URL` repository secret is configured with an absolute `/ask2` endpoint. Without it, the job is skipped automatically. Pushes and pull requests receive a guard check named "Persona guard" that reports success while reminding maintainers that persona evaluation is manual-only.

### Production feature flags

After merging to main, rollout the repo-managed persona and normalization flags on the VM:

```bash
./scripts/deploy_flags.sh PERSONA_V1=1 REQUEST_NORMALIZE=1
```

The helper writes `/etc/systemd/system/sustainacore-ai.service.d/15-persona.conf`, reloads the unit, and echoes the effective environment overrides.

## Features
- **RAG pipelines** orchestrating curated ESG corpora with LLM-powered reasoning.
- **Adapter-first architecture** across embedding, retrieval, and orchestration modules.
- **Operational middleware** delivering failover routing, observability, and compliance logging.
- **Infrastructure tooling** for VM deployment, CI/CD hygiene, and data refresh workflows.

### Chat routing upgrades
- `/ask2` now calls the smart router first when `ASK2_ENABLE_SMALLTALK=true`, returning a consistent "Hello! I can help with Sustainacore, TECH100, and ESG questions." greeting without invoking Gemini.
- Normal Q&A responses reformat sources via the router formatter, dedupe duplicate citations, and honor `ASK2_MAX_SOURCES` plus `ASK2_SOURCE_LABEL_MODE=concise` for compact labels.
- The Gemini composer validates responses that arrive wrapped in fenced ```json code-blocks and reuses the parsed `answer`/`sources` whenever present.
- A similarity guard enforces `SIMILARITY_FLOOR`: if the top retrieval score falls below the floor, the API responds with a clarifier instead of ungrounded sources and sets `meta.routing="low_conf"`.

#### Environment variables
- `ASK2_ENABLE_SMALLTALK` – Toggle router-first smalltalk detection (default `true`).
- `ASK2_MAX_SOURCES` – Cap the number of formatted sources returned (default `6`).
- `ASK2_SOURCE_LABEL_MODE` – Controls label style (`concise` adds "Title › Section", default `default`).
- `ASK2_SYNTH_FALLBACK` – When set (any truthy value), synthesize a short bullet summary from retrieved facts if Gemini omits an answer entirely.
- `SIMILARITY_FLOOR` – Existing retrieval floor reused by the clarifier guard.

## Stack
- **Runtime:** Python 3.11, FastAPI, Uvicorn, WSGI fallbacks; Django public site on VM2 served via Nginx → Gunicorn at <https://sustainacore.org>.
- **Storage:** Oracle APEX workspace, Oracle DB schemas, vector stores for embeddings.
- **Tooling:** GitHub Actions, pytest, cspell, Terraform/VM scripts under `infra/`.
- **Integrations:** Oracle APEX app export (`app/apex/f101_latest.sql`) aligned with the live workspace and used for secondary/administrative flows (not the primary public front-end).

## Screenshots & Demos
> Coming soon. Drop exports under `docs/images/` and update the placeholders below.
>
> ![APEX dashboard placeholder](docs/images/apex-dashboard-placeholder.png)
> ![Ask2 pipeline placeholder](docs/images/ask2-pipeline-placeholder.png)

## Roadmap
- Harden observability and tracing for production workloads.
- Expand integration tests around failover scenarios.
- Publish managed dataset snapshots and schema migrations.
- Document infrastructure-as-code paths for multi-region rollouts.

## Project Structure
- `app/` – FastAPI retrieval service plus adapters. Latest APEX export lives in `app/apex/`.
- `db/` – `schema/`, `migrations/`, and `seed/` directories for Oracle database assets.
- `datasets/` – Placeholder for lightweight demo CSVs referenced in docs.
- `infra/` – VM, CI, and deployment automation.
- `scripts/` – Utility scripts for maintenance and developer tooling.
- `archive/` – Curated historical assets with a manifest for provenance.

## Releases & Archives
Curated dated artifacts are now standardized under `archive/`:

| Category | Canonical | Archive Path |
| --- | --- | --- |
| Oracle APEX export | `app/apex/f101_latest.sql` | `archive/apex_exports/` |
| Database schema | `db/schema/schema.sql` | `archive/sql_dumps/` |
| VM bundles & backups | — | `archive/vm_backups/` |
| Legacy code & misc assets | — | `archive/misc/` (see nested folders) |

Refer to [`archive/MANIFEST.md`](archive/MANIFEST.md) for an authoritative mapping of originals → archived locations with timestamps.

## Governance
- The `/ask2` contract remains the backbone: `POST /ask2` → `{ "answer", "sources", "meta" }`.
- Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).
- Security disclosures should follow [SECURITY.md](SECURITY.md).

## License
This project is available under the [MIT License](LICENSE).
