#!/usr/bin/env bash
set -euo pipefail

EXPECTED_GH_USER="joaotovolli"
EXPECTED_GIT_NAME="Joao Tovolli"
EXPECTED_GIT_EMAIL="225354763+joaotovolli@users.noreply.github.com"

if env | cut -d= -f1 | grep -Eq '^(GH_TOKEN|GITHUB_TOKEN)$'; then
  echo "WARNING: GH_TOKEN or GITHUB_TOKEN is present in the environment. If identity validation fails, unset token override variables and retry."
fi

ACTUAL_GH_USER="$(gh api user -q .login 2>/dev/null || true)"

if [ "${ACTUAL_GH_USER}" != "${EXPECTED_GH_USER}" ]; then
  echo "BLOCKED: GitHub CLI is authenticated as '${ACTUAL_GH_USER}', expected '${EXPECTED_GH_USER}'."
  echo "User action required if browser approval is needed: gh auth login -h github.com --git-protocol https --scopes repo,workflow,read:org --web"
  exit 2
fi

GITHUB_ID="$(gh api user -q .id)"
GITHUB_LOGIN="$(gh api user -q .login)"
GITHUB_NOREPLY_EMAIL="${GITHUB_ID}+${GITHUB_LOGIN}@users.noreply.github.com"

if [ "${GITHUB_NOREPLY_EMAIL}" != "${EXPECTED_GIT_EMAIL}" ]; then
  echo "BLOCKED: GitHub noreply email is '${GITHUB_NOREPLY_EMAIL}', expected '${EXPECTED_GIT_EMAIL}'."
  exit 2
fi

git config user.name "${EXPECTED_GIT_NAME}"
git config user.email "${EXPECTED_GIT_EMAIL}"

ACTUAL_GIT_NAME="$(git config --get user.name || true)"
ACTUAL_GIT_EMAIL="$(git config --get user.email || true)"

echo "GitHub authenticated user: ${GITHUB_LOGIN}"
echo "Git author name: ${ACTUAL_GIT_NAME}"
echo "Git author email: ${ACTUAL_GIT_EMAIL}"

if [ "${ACTUAL_GIT_NAME}" != "${EXPECTED_GIT_NAME}" ]; then
  echo "BLOCKED: git user.name is not ${EXPECTED_GIT_NAME}."
  exit 2
fi

if [ "${ACTUAL_GIT_EMAIL}" != "${EXPECTED_GIT_EMAIL}" ]; then
  echo "BLOCKED: git user.email is not ${EXPECTED_GIT_EMAIL}."
  exit 2
fi
