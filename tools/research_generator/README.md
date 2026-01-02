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
- Model pinned to `gemini-2.5-flash`.
- No price data or investment advice; derived metrics only.

Systemd Doctor
- `systemctl status research-generator.service --no-pager`
- `systemctl status research-generator-manual.service --no-pager`
- `journalctl -u research-generator-manual.service -n 200 --no-pager`
