# VM → GitHub → Codex round trip

This helper script lets the deployment VM prove the reverse loop by opening a pull request, pinging Codex, and waiting for a single improvement commit (or a precise change request if pushes are restricted).

## Prerequisites

- `git` configured with a bot identity (`git config --global user.name "VM Canary Bot"` and `git config --global user.email "vm+canary@sustainacore.org"`).
- `gh` authenticated with a token that can create branches and pull requests (`gh auth login --with-token`).
- The repository cloned at `~/Sustainacore` with `origin` pointing at GitHub.

## Ready-to-run snippet

```bash
set -euo pipefail
cd ~/Sustainacore
git fetch origin
branch="canary/vm-to-cloud-$(date -u +%Y%m%d_%H%M)"
git checkout -B "$branch" origin/main
mkdir -p canary
echo "VM → GitHub → Codex Cloud @ $(date -u +'%Y-%m-%dT%H:%M:%SZ')" > canary/VM_TO_CLOUD.md
git add canary/VM_TO_CLOUD.md
git commit -m "canary(vm): reverse loop test"
git push -u origin "$branch"
pr_url=$(gh pr create --title "Canary: VM → GitHub → Codex Cloud" --body "Codex: please leave one improvement commit or a precise change request.")
printf 'PR created: %s\n' "$pr_url"
gh pr edit --add-reviewers codex --add-assignee codex
```

## Expected outcome

1. GitHub notifies Codex via the reviewer assignment.
2. Codex pushes one improvement commit (or leaves a detailed change request) and updates the PR thread.
3. Merge or close the PR once the improvement lands; delete the temporary branch (`git push origin --delete "$branch"`) to keep the repo tidy.

Keep a copy of the resulting PR link in your ops log so future audits can confirm the reverse loop remains healthy.
