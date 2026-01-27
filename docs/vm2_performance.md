# VM2 Performance Checklist
<!-- cspell:ignore ppid etime -->

## Quick checks (safe)
- `free -h`
- `ps aux --sort=-%mem | head -n 15`
- `ps -eo pid,ppid,user,etime,rss,cmd | rg "manage\.py runserver"`

## Cleanup guidelines (safe)
- Only stop **orphaned** `python manage.py runserver` processes bound to `127.0.0.1`.
- Never stop `gunicorn`/`nginx` services.
- Use `kill -TERM <PID>` and recheck with `ps -p <PID>`.

## Codex CLI timeout guardrails
- Codex CLI commands time out quickly in this environment; avoid long-running commands.
- Do **not** use `gh pr checks --watch` or long `sleep` loops.
- Use the fast poll script: `tools/ci/poll_pr_checks.sh <pr-number>`.

## Preview public note
- Preview is public (no Basic Auth). Treat CI artifacts as the source of truth for UI diffs.

## Report (2026-01-16)

### Baseline (before cleanup)
- `free -h` (earlier run):
  - Used: 775Mi
  - Available: 181Mi
- Multiple orphan `manage.py runserver` processes on ports 8001/8010/8030/8040/8041/8048.

### Actions applied (safe)
- Terminated only orphaned `manage.py runserver` processes (local dev servers).
- No production services touched.

### After cleanup
- `free -h`:
  - Used: 589Mi
  - Available: 367Mi
- Top memory consumers are now `gunicorn` workers and Codex CLI.

### Rollback
- If a dev server was needed, restart manually with:
  - `DJANGO_SECRET_KEY=test python website_django/manage.py runserver 127.0.0.1:<PORT> --noreload`

## Notes
- No configurable Codex CLI timeout setting was found in `/home/ubuntu/.codex/config.toml` or the installed Codex package.
- Use short, repeatable commands for polling CI.
