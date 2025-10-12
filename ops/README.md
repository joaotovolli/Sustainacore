# SustainaCore Operations Guide

This document captures the operational steps and automation hooks required for the forward (GitHub → VM) and reverse (VM → GitHub) canary loops, along with tips to keep notification noise low.

## Repository secrets required for canary automation

Set the following repository secrets under **Settings → Secrets and variables → Actions** before applying the `canary` label to a pull request:

| Secret | Description |
| ------ | ----------- |
| `VM_HOST` | Public hostname or IP address of the SustainaCore OCI virtual machine. |
| `VM_USER` | SSH user on the VM with permission to append to `~/canary/roundtrip.log`. |
| `VM_SSH_KEY` | Private key for `VM_USER` in OpenSSH format (include the `-----BEGIN OPENSSH PRIVATE KEY-----` header). |
| `VM_SSH_PORT` *(optional)* | SSH port if the VM is not exposed on port 22. |
| `OPENAI_API_KEY` | Used by the persona evaluation workflow for LLM-backed checks. |
| `ORG_ID` | Partner organization identifier consumed by persona evaluations. |
| `ASK2_URL` | Absolute URL of the `/ask2` endpoint (e.g., `https://ai.sustainacore.org/ask2`). |

> ℹ️ Secrets missing during a run cause the workflow to post a guidance comment instead of failing. Add the secrets and reapply the `canary` label to retrigger.

## Forward loop (GitHub → VM) canary test

1. Push your branch and open a pull request.
2. Add the `canary` label to the PR.
3. The `.github/workflows/canary.yml` workflow runs and SSHs into the VM.
4. On success, the workflow appends a timestamped entry to `~/canary/roundtrip.log` and posts the tail of the log back to the PR as proof.
5. To manually confirm from the VM:
   ```bash
   tail -n 20 ~/canary/roundtrip.log
   ```
6. If the workflow comments about missing secrets, follow the instructions in the comment, update the repository secrets, and add the `canary` label again.

## Reverse loop (VM → GitHub → Codex Cloud)

Authenticate the VM with GitHub CLI (`gh auth login --with-token` using a PAT that can create branches and PRs). Then execute the following script from the VM to create a round-trip pull request and request a Codex review:

```bash
set -euo pipefail
git config --global user.name "VM Canary Bot"
git config --global user.email "vm+canary@sustainacore.org"
cd ~/Sustainacore
git fetch origin
git checkout -B canary/vm-to-cloud-$(date -u +%Y%m%d_%H%M) origin/main
mkdir -p canary
echo "VM -> GitHub -> Codex Cloud @ $(date -u +'%Y-%m-%dT%H:%M:%SZ')" > canary/VM_TO_CLOUD.md
git add canary/VM_TO_CLOUD.md
git commit -m "canary(vm): reverse loop test"
git push -u origin HEAD
pr_url=$(gh pr create --title "Canary: VM → GitHub → Codex Cloud" --body "Please @codex review and push exactly one improvement.")
echo "PR: $pr_url"
gh pr edit --add-label codex-review
```

After the PR opens, Codex Cloud (or a human standing in for Codex) should add a single follow-up improvement commit or leave a precise change request before merge.

## VM helper script (`~/ops/canary_apply.sh`)

Ensure the VM exposes an idempotent helper script that records each apply attempt:

```bash
#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME/canary"
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "canary_apply $timestamp" >> "$HOME/canary/apply.log"
```

Save this as `~/ops/canary_apply.sh`, make it executable (`chmod +x ~/ops/canary_apply.sh`), and invoke it from deployment tooling to keep a durable audit trail. View the history with:

```bash
tail -n 40 ~/canary/apply.log
```

## Persona evaluation preflight

The persona evaluation workflow validates that `ASK2_URL`, `OPENAI_API_KEY`, and `ORG_ID` secrets are set and that `ASK2_URL` is an absolute URL. When those requirements are unmet, the workflow records a skip reason instead of failing. Update repository secrets accordingly before rerunning the job.

## Reducing GitHub notification noise

To keep alerts actionable without altering organization-wide settings:

1. Visit **https://github.com/settings/notifications** and switch your default participation level to **Participating and @mentions** for email.
2. Add custom notification filters (e.g., "label:canary" or "label:codex-review") so that GitHub only emails you for the automation labels you care about.
3. If you forward notifications to email, create a mail rule that matches `subject:[Sustainacore]` and only flags items containing `canary` or `codex-review` so that routine CI does not clutter your inbox.
4. Leverage the **Saved filters** panel in the GitHub notifications inbox to quickly jump to canary or persona evaluation updates.

Documenting your filters inside your personal notes (instead of editing org-level settings) keeps the setup reversible and low-risk.
