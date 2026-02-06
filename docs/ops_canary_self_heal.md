## Canary + Self-Heal Workflow Notes (Public-Safe)

### Target Host Selection (Avoiding Stale VM1 Public IPs)
The GitHub Actions workflow `Canary + Self-Heal` prefers reaching VM1 through VM2 as a jump host.

Why:
- VM1 may be rebuilt and get a new public IP.
- VM2 is typically the stable, publicly reachable entrypoint for ops automation.
- VM2 already needs to know the correct VM1 private backend address for the website to function.

How it works:
- If `VM2_HOST`, `VM2_USER`, and `VM2_SSH_KEY` are present, the workflow SSHes to VM2 and reads VM1's backend host from:
  - `/etc/sysconfig/sustainacore-django.env` (`SUSTAINACORE_BACKEND_URL` or `BACKEND_API_BASE`)
- The canary then SSHes to VM1 using that private IP via an SSH jump through VM2.
- If VM2 jump secrets are not available, the workflow falls back to `VM_HOST` (direct public SSH).

Secrets referenced by the workflow (no values are stored in git):
- Required: `VM_USER`, `VM_SSH_KEY`
- Preferred (jump host): `VM2_HOST`, `VM2_USER`, `VM2_SSH_KEY`
- Fallback (direct): `VM_HOST`

### Output Safety
The workflow avoids writing brittle heredocs to `$GITHUB_OUTPUT` that can break parsing.
Multi-line outputs use a unique delimiter each time so the runner never errors with:
`Matching delimiter not found`.

