SustainaCore — Autopilot rules for Codex.
- Keep /ask2 contract (q,k -> {answer,sources,meta})
- Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true
- Run: uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
- Deploy: ops/scripts/deploy_vm.sh

Agents:
- vm1-esg-ai: scope esg_ai/**, index/**, oracle_scripts/**, target VM1. Must not modify website_django/**.
- vm2-website: scope website_django/**, target VM2. Must not modify ESG/Ask2 folders.
