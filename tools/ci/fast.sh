#!/usr/bin/env bash
set -euo pipefail

cmd="${1:-}"
shift || true

http_head() {
  local url="$1"
  curl -sS -I --connect-timeout 10 --max-time 60 --retry 0 "$url" | head -n 20
}

http_code() {
  local url="$1"
  curl -sS -o /dev/null -w "%{http_code}\n" --connect-timeout 10 --max-time 60 --retry 0 "$url"
}

gh_runs() {
  local workflow="$1"
  gh run list -R "${GH_REPO:?Set GH_REPO}" --workflow "$workflow" --limit 8 --json databaseId,status,conclusion,headSha,headBranch,htmlUrl,createdAt
}

gh_run_view() {
  local run_id="$1"
  gh run view -R "${GH_REPO:?Set GH_REPO}" "$run_id" --json status,conclusion,htmlUrl
}

gh_run_download() {
  local run_id="$1"
  local name="$2"
  local dir="$3"
  gh run download -R "${GH_REPO:?Set GH_REPO}" "$run_id" -n "$name" -D "$dir"
}

gh_pr_body() {
  local pr="$1"
  local file="$2"
  gh pr edit -R "${GH_REPO:?Set GH_REPO}" "$pr" --body-file "$file"
}

gh_pr_comment() {
  local pr="$1"
  local file="$2"
  gh pr comment -R "${GH_REPO:?Set GH_REPO}" "$pr" --body-file "$file"
}

case "$cmd" in
  http_head) http_head "$@" ;;
  http_code) http_code "$@" ;;
  gh_runs) gh_runs "$@" ;;
  gh_run_view) gh_run_view "$@" ;;
  gh_run_download) gh_run_download "$@" ;;
  gh_pr_body) gh_pr_body "$@" ;;
  gh_pr_comment) gh_pr_comment "$@" ;;
  *)
    echo "usage: $0 {http_head|http_code|gh_runs|gh_run_view|gh_run_download|gh_pr_body|gh_pr_comment} ..." >&2
    exit 2
    ;;
 esac
