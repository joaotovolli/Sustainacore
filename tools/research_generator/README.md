Scheduled Research Generator

Purpose
- Generate research/education drafts focused on AI governance & ethics.
- Produce DOCX + chart + table and submit to PROC_GEMINI_APPROVALS.

CLI
- `python3 -m tools.research_generator.run_generator --once --dry-run`
- `python3 -m tools.research_generator.run_generator --once --force rebalance|weekly|period|anomaly`
- `python3 -m tools.research_generator.run_generator --process-manual --once`
- `python3 -m tools.research_generator.run_generator --process-manual --request-id <id> --once`

Setup
- Initialize PROC_REPORTS:
  - `python3 tools/research_generator/init_proc_reports.py`
- Initialize PROC_RESEARCH_REQUESTS:
  - `python3 tools/research_generator/init_proc_research_requests.py`
- Install systemd timer + venv:
  - `bash tools/research_generator/systemd/install_systemd.sh`

Notes
- Uses Codex CLI only (no Gemini or external LLM clients in research generator).
- No price data or investment advice; derived metrics only.
- Oracle connectivity relies on `TNS_ADMIN` pointing at the VM wallet directory (current default: `/opt/adb_wallet_tp_sso`).
 
Codex Doctor
- `PYTHONPATH=/home/opc/Sustainacore /usr/bin/python3 -m tools.research_generator.doctor_codex`
- The generator services load `/etc/sustainacore/llm.env` for `OPENAI_API_KEY`.

Cleanup (research-only)
- Dry run (counts only):
  - `python3 -m tools.research_generator.cleanup --dry-run`
- Delete research rows + local artifacts:
  - `python3 -m tools.research_generator.cleanup --yes`
  - `bash tools/research_generator/scripts/cleanup_research.sh --yes`
- Deletes only `REQUEST_TYPE='RESEARCH_POST'` from `PROC_GEMINI_APPROVALS`.
- Clears research-only tables (`PROC_RESEARCH_REQUESTS`, `PROC_REPORTS`, `PROC_RESEARCH_REPORTS`, `PROC_RESEARCH_ALERTS`) if present.
- Removes files under `tools/research_generator/output/` and keeps `quota_state.json`.

Systemd Doctor
- `systemctl status research-generator.service --no-pager`
- `systemctl status research-generator-manual.service --no-pager`
- `journalctl -u research-generator-manual.service -n 200 --no-pager`
