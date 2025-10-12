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

## Code style
- Follow Python formatting conventions agreed upon in the repository.
- Use descriptive names and add comments where intent might be unclear.
- Prefer configuration-driven changes over ad-hoc scripts when expanding automation.

## Community expectations
We expect contributors to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful, collaborative, and open to feedback.

Thank you for helping improve Sustainacore!
