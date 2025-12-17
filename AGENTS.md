SustainaCore — Autopilot rules for Codex.
- Keep /ask2 contract (q,k -> {answer,sources,meta})
- Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true
- Run: uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
- Deploy: ops/scripts/deploy_vm.sh

Agents:
- vm1-esg-ai: scope esg_ai/**, index/**, oracle_scripts/**, target VM1. Must not modify website_django/**.
- vm2-website: scope website_django/**, target VM2. Must not modify ESG/Ask2 folders.

## Oracle Bootstrap Contract (VM1)
- Codex CLI shells may start with no DB_* envs; this is expected.
- Never source /etc/sustainacore/*.env or /etc/sustainacore-ai/*.env (not bash-safe).
- Always run `python3 tools/oracle/preflight_oracle.py` (or `python3 tools/test_db_connect.py`) before any Oracle task.
- Any new Oracle-facing script must call `load_env_files()` then use `db_helper.get_connection()`.
- If preflight fails: stop and report the error; do not attempt alternative drivers or wallet rewrites.
