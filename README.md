# Sustainacore

![CI](https://img.shields.io/badge/status-production_ready-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Contributions](https://img.shields.io/badge/contributions-welcome-brightgreen)

## Executive Summary
Sustainacore delivers ESG knowledge retrieval and orchestration services that unify retrieval augmented generation, workflow automation, and governance tooling. This mono-repo tracks the production FastAPI surface, APEX integrations, database artifacts, and supporting infrastructure that keep the platform reliable for enterprise deployments. 

## Quick Start
1. Clone the repository and create a Python virtual environment: `python3 -m venv .venv && source .venv/bin/activate`. 
2. Install dependencies: `pip install -U pip && pip install -r requirements.txt`.
3. Export environment variables from `.env` (or copy from `.env.sample`).
4. Launch the retrieval API locally with `uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080`.
5. Run the regression suite with `pytest -q` before committing changes.

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

## Stack
- **Runtime:** Python 3.11, FastAPI, Uvicorn, WSGI fallbacks.
- **Storage:** Oracle APEX workspace, Oracle DB schemas, vector stores for embeddings.
- **Tooling:** GitHub Actions, pytest, cspell, Terraform/VM scripts under `infra/`.
- **Integrations:** Oracle APEX app export (`app/apex/f101_latest.sql`) aligned with the live workspace.

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
