#!/usr/bin/env bash
set -euo pipefail

EXPECTED_GH_USER="joaotovolli"
EXPECTED_GH_ID="225354763"
EXPECTED_GIT_NAME="Joao Tovolli"
EXPECTED_GIT_EMAIL="225354763+joaotovolli@users.noreply.github.com"

if env | cut -d= -f1 | grep -Eq '^(GH_TOKEN|GITHUB_TOKEN)$'; then
  echo "WARNING: GH_TOKEN or GITHUB_TOKEN is present in the environment. If identity validation fails, unset token override variables and retry."
fi

ACTUAL_GH_USER="$(gh api user -q .login 2>/dev/null || true)"
ACTUAL_GH_ID="$(gh api user -q .id 2>/dev/null || true)"

if [ "${ACTUAL_GH_USER}" != "${EXPECTED_GH_USER}" ]; then
  echo "BLOCKED: GitHub CLI is authenticated as '${ACTUAL_GH_USER}', expected '${EXPECTED_GH_USER}'."
  echo "User action required if browser approval is needed: gh auth login -h github.com --git-protocol https --scopes repo,workflow,read:org --web"
  exit 2
fi

if [ "${ACTUAL_GH_ID}" != "${EXPECTED_GH_ID}" ]; then
  echo "BLOCKED: GitHub CLI account id is '${ACTUAL_GH_ID}', expected '${EXPECTED_GH_ID}'."
  exit 2
fi

# This verifier intentionally does not write Git config: Codex workspace-write
# sandboxes may expose .git/config as read-only.
ACTUAL_GIT_NAME="$(git config --get user.name || true)"
ACTUAL_GIT_EMAIL="$(git config --get user.email || true)"

if [ "${ACTUAL_GIT_NAME}" != "${EXPECTED_GIT_NAME}" ] || [ "${ACTUAL_GIT_EMAIL}" != "${EXPECTED_GIT_EMAIL}" ]; then
  echo "Run from normal WSL shell:"
  echo "git config user.name \"Joao Tovolli\""
  echo "git config user.email \"225354763+joaotovolli@users.noreply.github.com\""
  exit 2
fi

echo "GitHub authenticated user: ${ACTUAL_GH_USER}"
echo "GitHub account id: ${ACTUAL_GH_ID}"
echo "Git author name: ${ACTUAL_GIT_NAME}"
echo "Git author email: ${ACTUAL_GIT_EMAIL}"
