#!/usr/bin/env bash
set -euo pipefail

pr_number="${1:-}"
if [[ -z "$pr_number" ]]; then
  echo "usage: $0 <pr-number>" >&2
  echo "example: bash tools/ci/poll_pr_checks.sh 405" >&2
  exit 2
fi

# Fast, single-shot status query. Exit codes:
# 0 = all checks successful
# 1 = at least one check failed
# 2 = pending/in progress

status_lines=$(gh pr view "$pr_number" --json statusCheckRollup -q '.statusCheckRollup[] | "\(.name)\t\(.conclusion // "PENDING")\t\(.status)"')

if [[ -z "$status_lines" ]]; then
  echo "no checks found" >&2
  exit 2
fi

failed=0
pending=0

while IFS=$'\t' read -r name conclusion status; do
  case "$conclusion" in
    FAILURE|ERROR|TIMED_OUT|CANCELLED)
      failed=1
      ;;
    PENDING|NEUTRAL|SKIPPED)
      if [[ "$status" != "COMPLETED" ]]; then
        pending=1
      fi
      ;;
  esac

done <<< "$status_lines"

if [[ "$failed" -eq 1 ]]; then
  echo "checks failed" >&2
  exit 1
fi

if [[ "$pending" -eq 1 ]]; then
  echo "checks pending" >&2
  exit 2
fi

echo "checks passed"
exit 0
