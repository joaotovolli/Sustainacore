# SustainaCore Operations Guide

This guide explains the forward canary → deploy automation, the reverse "VM → GitHub" loop, and the guardrails that keep alerts actionable without flooding inboxes.

## Triggers, filters, and concurrency

The canary workflow (`.github/workflows/canary.yml`) runs automatically on:

- Pushes to `main` that touch `app/**`, `ops/**`, `db/**`, or `.github/**`.
- Pull requests that modify the same paths.
- Manual runs via **Actions → Canary + Self-Heal → Run workflow**.
- A nightly cron at 06:30 UTC.

Concurrency is enforced per ref (`concurrency: canary-${{ github.ref }}`) so only the most recent push for a branch runs at a time. Success on a commit triggers the deploy workflow via `workflow_run`; deploy uses its own `concurrency: deploy-${{ ... }}` group to avoid overlapping SSH sessions.

## Required repository secrets

Configure the following under **Settings → Secrets and variables → Actions**:

| Secret | Description |
| ------ | ----------- |
| `VM_HOST` | Public hostname or IP for `ai.sustainacore.org`. |
| `VM_USER` | SSH user with append access to `~/canary/roundtrip.log` (typically `opc`). |
| `VM_SSH_KEY` | OpenSSH private key that matches `VM_USER`. |
| `OPENAI_API_KEY` *(optional)* | Required by persona/self-eval workflows. |
| `ORG_ID` *(optional)* | Organization identifier consumed by persona tests. |

The canary workflow performs a preflight that checks these secrets. If anything is missing it posts a guidance comment on the pull request (`<!-- canary-preflight -->`) and exits successfully so you do not receive a red ❌.

## Forward loop (GitHub → VM) canary

1. Push a change (or open/update a PR) that touches one of the filtered paths.
2. The `canary` job prepares an ephemeral SSH key file, connects as `${VM_USER}@${VM_HOST}`, and appends `timestamp SHA` to `~/canary/roundtrip.log`. If `~/ops/canary_apply.sh` exists on the VM it is executed.
3. On success the workflow posts a comment containing the tail of `~/canary/roundtrip.log` (`<!-- canary-status -->`) and links back to the run.
4. You can verify from the VM with:
   ```bash
   tail -n 20 ~/canary/roundtrip.log
   ```
5. Need to re-run manually? Open the workflow run in GitHub Actions and choose **Re-run jobs** or trigger `workflow_dispatch` from the Actions tab.

## Self-heal loop and alerts

If any job fails on a pull request:

- A structured comment (`<!-- canary-alert -->`) mentions `@joaotovolli` and `@codex` with the failing job result, a 20-line error snippet, and links to the logs.
- The comment also contains **Fix Plan v1** instructing Codex to attempt up to three targeted fixes (small commits, reruns, and status updates) before switching to a rollback/manual checklist.

Because preflight skips gracefully, only real execution failures trigger these alerts.

## Deploy chain (after canary)

`.github/workflows/deploy.yml` listens for successful completions of the canary workflow on `main`.

- Deploy reuses the secrets preflight; missing credentials result in a skipped run with a step summary.
- The job checks out the exact commit that passed canary (`github.event.workflow_run.head_sha`) and runs `ops/scripts/deploy_vm.sh`, passing the SSH key path via `SSH_KEY`.
- On failure, the workflow updates/creates a `<!-- deploy-alert -->` comment on the originating PR with the latest log snippet and another self-heal plan.
- On success, the `<!-- deploy-status -->` comment confirms deployment and links the run.

Manual deploys are also available through **Actions → Deploy after Canary → Run workflow**, optionally providing a `sha` input.

## Reverse loop (VM → GitHub → Codex Cloud)

Authenticate the VM with GitHub CLI (`gh auth login --with-token`) and run the helper script captured in [`ops/VM_TO_CLOUD.md`](VM_TO_CLOUD.md). The script:

1. Creates a branch from the latest `origin/main`.
2. Writes a status note to `canary/VM_TO_CLOUD.md`.
3. Pushes the branch and opens a PR requesting a Codex review.

Codex should respond by pushing exactly one improvement commit or leaving a precise change request before the PR is merged, completing the reverse validation loop.

## VM helper script (`~/ops/canary_apply.sh`)

Keep an idempotent script on the VM that the canary job can call:

```bash
#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME/canary"
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "canary_apply $timestamp" >> "$HOME/canary/apply.log"
```

Make it executable (`chmod +x ~/ops/canary_apply.sh`). Inspect recent activity with:

```bash
tail -n 40 ~/canary/apply.log
```

## Persona evaluation preflight

The persona workflow still validates that `ASK2_URL`, `OPENAI_API_KEY`, and `ORG_ID` are populated. Missing values add a skip notice to the workflow summary instead of failing so that CI remains clean.

## CI eval (stub mode)

Continuous integration runs enable a deterministic retrieval stub so Persona evaluations pass without the Oracle/vector stack. When the workflow exports `CI_EVAL_FIXTURES=1`, the `/ask2` handler short-circuits to a small fixture (`eval/fixtures/ci_stub.json`) that returns:

- A canned answer that already includes a **Sources:** section.
- At least three string contexts (or more if the request sets a higher `top_k`).
- A non-empty `sources` array with stable `fixture://` URLs.

Deployments and local runs without the flag continue to exercise the real retrieval pipeline.

## Reducing GitHub notification noise

- GitHub only @mentions `@joaotovolli` and `@codex` when a canary/deploy job fails. Success comments omit @mentions.
- Adjust email delivery under <https://github.com/settings/notifications> ("Participating and @mentions" keeps alerts targeted).
- Create inbox filters for `label:canary` or the HTML comment markers if you prefer a custom triage view.

Documenting these knobs keeps the automation reversible and low-risk.
