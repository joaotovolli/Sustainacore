Scheduled Research Generator

Purpose
- Generate research/education drafts focused on AI governance & ethics.
- Produce DOCX + chart + table and submit to PROC_GEMINI_APPROVALS.

CLI
- `python3 -m tools.research_generator.run_generator --once --dry-run`
- `python3 -m tools.research_generator.run_generator --once --force rebalance|weekly|period|anomaly`

Setup
- Initialize PROC_REPORTS:
  - `python3 tools/research_generator/init_proc_reports.py`
- Install systemd timer + venv:
  - `bash tools/research_generator/systemd/install_systemd.sh`

Notes
- Model pinned to `gemini-2.5-flash`.
- No price data or investment advice; derived metrics only.
