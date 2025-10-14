# Contributing to Sustainacore

Thanks for your interest in improving Sustainacore! This document outlines how to get started and what we expect from contributors.

## Ways to contribute
- Report bugs using the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md).
- Suggest new features or enhancements using the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md).
- Improve documentation, automation, or tests to keep the platform healthy.

## Development workflow
1. Fork the repository and create a feature branch.
2. Set up a virtual environment and install dependencies from `requirements.txt`.
3. Make focused changes with descriptive commit messages.
4. Run the relevant tests or linters described in project documentation.
5. Open a pull request that explains the motivation, implementation, and testing.

## Pull request guidelines
- Keep pull requests small and focused for easier reviews.
- Include screenshots or logs for UI or operational changes when possible.
- Describe how you verified your changes and call out any known limitations.
- Ensure CI checks pass before requesting review.
- To request an autonomous Codex review, add the `codex-review` label or comment `@codex review` on the pull request. Reviews stay quiet until one of these triggers is present.

## CI/CD quick reference

- The **Canary + Self-Heal** workflow runs automatically on pushes/PRs touching `app/**`, `ops/**`, `db/**`, or `.github/**` (plus nightly cron and manual dispatch).
- Preflight checks post a neutral comment if required secrets (`VM_HOST`, `VM_USER`, `VM_SSH_KEY`) are missing; the job exits successfully so you do not receive a red ❌.
- Successful canary runs append proof to `~/canary/roundtrip.log` on the VM and comment the log tail back to the PR.
- Failures @mention `@joaotovolli` and `@codex` with a **Fix Plan**. Codex will attempt up to three targeted fixes (small commits) before switching to a rollback checklist.
- The **Deploy after Canary** workflow is gated by a green canary on the same commit. Deploy updates the PR with success/failure status and shares log snippets for fast triage.
- Reverse loop validation (VM → GitHub → Codex) is documented in [`ops/VM_TO_CLOUD.md`](ops/VM_TO_CLOUD.md); aim to keep that exercise passing at least once per cycle.

## Code style
- Follow Python formatting conventions agreed upon in the repository.
- Use descriptive names and add comments where intent might be unclear.
- Prefer configuration-driven changes over ad-hoc scripts when expanding automation.

## Writing & terminology
- Repository spellchecking is configured via [`.cspell.json`](.cspell.json) so our domain terms stay green in CI.
- Use "evaluations" in narrative prose; the term "eval pack" is intentionally allowed for the CI workflow name and related docs.
- When a new project-specific term is necessary, add it to `.cspell.json` rather than suppressing the Hygiene workflow or excluding entire files.

## Community expectations
We expect contributors to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful, collaborative, and open to feedback.

Thank you for helping improve Sustainacore!
