# Codex UI Loop Status

## Attempt 1
- Action: verify preview/prod via HTTPS (curl --connect-timeout 2 --max-time 4)
- Result: both preview/prod HTTPS timed out (curl 28)
- Switch: checked local upstreams
- Local: preview http://127.0.0.1:8001 -> 200, prod http://127.0.0.1:8000 -> timeout
- Next: proceed with CI-only UI compare and create UI PR; treat external curl timeouts as VM2 network constraint
