Gemini Jobs Worker

Overview
- Isolated worker that polls `PROC_GEMINI_JOBS` and creates approvals in `PROC_GEMINI_APPROVALS`.
- Applies approved inserts into `ESG_DOCS` with deterministic `SOURCE_ID` de-dupe.
- Uses worker-only instructions in `GEMINI.md` and does not touch Ask2.

Run modes
- One-shot: `python3 -m tools.gemini_jobs_worker.run_worker --once`
- Continuous: `python3 -m tools.gemini_jobs_worker.run_worker`
- Dry-run (approval only): `python3 -m tools.gemini_jobs_worker.run_worker --once --dry-run`
- Supervised first run: `python3 -m tools.gemini_jobs_worker.run_worker --supervise-first-run`

Quota guard
- Enforces 50 calls/60s and 950 calls/day (UTC).
- Persistent counters stored in `/var/lib/sustainacore/gemini_jobs_worker/quota_state.json` with fallback to `tools/gemini_jobs_worker/.state/quota_state.json`.

Systemd
- Install and enable service:
  - `tools/gemini_jobs_worker/systemd/install_systemd.sh`
- Monitor:
  - `journalctl -u gemini-jobs-worker.service -f`
