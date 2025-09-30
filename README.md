# Sustainacore

![CI](https://img.shields.io/badge/status-experimental-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Contributions](https://img.shields.io/badge/contributions-welcome-brightgreen)

## Executive Summary
Sustainacore delivers ESG knowledge retrieval and orchestration services that stitch together retrieval augmented generation (RAG), workflow automation, and governance tooling. This repository captures the shared infrastructure, adapters, and middleware used to power the Sustainacore platform.

## Features
- **RAG pipelines** that combine curated ESG corpora with LLM orchestration.
- **Adapter-first architecture** for swapping retrieval, embedding, and orchestration providers.
- **Operational tooling** for failover routing, observability, and compliance logging.
- **Extensible middleware** designed for incremental experiments without impacting core pathways.

## Tech Stack
- **Python** applications orchestrating retrieval, refinement, and middleware layers.
- **FastAPI & WSGI** entrypoints serving API traffic.
- **Vector and relational stores** for embeddings and operational metadata.
- **Infrastructure scripts** targeting cloud VMs and containerized deployments.

## Quick Start
> Detailed quick start documentation is under active development. Use the placeholders below until automated setup scripts are published.

1. Clone the repository and create a Python virtual environment.
2. Install dependencies from `requirements.txt`.
3. Review environment variables in deployment notes before running any services.

## Roadmap
- Harden observability and tracing for production workloads.
- Expand integration tests around failover scenarios.
- Publish managed dataset snapshots and schema migrations.
- Document infrastructure-as-code paths for multi-region rollouts.

## Contributing
We welcome thoughtful contributions! Please review [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and coding standards. For major changes, open an issue to discuss your proposal before submitting a pull request.

## Code of Conduct
Participation in this project is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to fostering an inclusive and respectful community.

## Security
Security disclosures should follow the process defined in [SECURITY.md](SECURITY.md). Please avoid filing public issues for potential vulnerabilities.

## Releases & Archives
Large, dated binary artifacts are curated under the [`archive/`](archive) directory with subfolders for APEX exports, SQL dumps, VM backups, and legacy code backups. Consult `archive/MANIFEST.md` for an up-to-date index of preserved files.

## Legacy Quick Reference
The `/ask2` contract remains the backbone of Sustainacore integrations.

| Item | Details |
| --- | --- |
| Contract | `POST /ask2` → `{ answer, sources, meta }` |
| Default mode | Oracle-first retrieval pipeline (no Gemini planning) |
| Optional layers | LLM refiner (OpenAI), Ollama micro-orchestrator, Gemini intent gateway |
| Primary client | Oracle APEX (direct call or APEX proxy) |

## Operational Notes
### Local development
1. `python3 -m venv .venv && source .venv/bin/activate`
2. `pip install -U pip && pip install -r requirements.txt`
3. Launch `uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080` or run `python app.py` for the legacy entrypoint.
4. Run `pytest -q` and smoke-test `/healthz` plus `/ask2` with sample payloads.

### Deployment tips
- Copy the repository into `/opt/sustainacore-ai/` on target VMs and configure environment files locally.
- Ensure Oracle Instant Client wallets and `TNS_ADMIN` paths are available before starting services.
- Restart via `systemctl restart sustainacore-ai.service` and confirm status with `systemctl status --no-pager`.

### Security & troubleshooting
- Enforce CORS through `ask2_cors_mw.py` and prefer proxying traffic via APEX in production.
- Low similarity signals should respond with "no answer" to avoid hallucinations.
- Disable optional LLM refiners when isolating latency or timeout issues.

## Documentation
Additional reference material lives in [`docs/`](docs). Start with [docs/README.md](docs/README.md) for links to operational runbooks and security resources.

## License
This project is available under the [MIT License](LICENSE).
