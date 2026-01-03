# Research Generator Playbook

## Goals
- Produce research/education analysis focused on AI governance & ethics.
- Use data-driven insights from Oracle; no stock prices; no investment advice.
- Emphasize Core Index (Top 25, weight > 0) vs Coverage (All 100, includes 75 weight = 0).

## Insight Selection
- Lead with core vs coverage gaps (mean, IQR, concentration) and sector shifts.
- Highlight breadth and turnover to explain rebalance dynamics.
- Use top movers to anchor narrative; include only 1–2 short support lines from public summaries.
- Avoid overly negative framing; if negative, keep neutral and data-backed.

## Core vs Coverage Strategy
- Core is weighted; coverage is unweighted.
- Zero-weight slice (75) provides a benchmark for the broader universe.
- Always state the mean gap and dispersion (IQR) differences.

## Table/Figure Commentary
- Each table/figure needs 3–6 callouts with explicit numbers.
- Use “The chart above shows…” and “The table below highlights…” in the narrative.

## Quality Gate
- Require at least 6 numeric references and 3 non-trivial stats (IQR, HHI, turnover, breadth).
- If sector delta flags are present, show “FLAG” and call out inconsistency.

## Model Selection Guidance
- Use Codex-style model for code navigation and Oracle plumbing.
- Use GPT-style model for statistical analysis, narrative critique, and improved framing.
- Use Gemini for concise narrative when available; fall back to GPT if rate-limited.
