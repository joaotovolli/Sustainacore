Title: Ask2 structured formatting + harness verification

Restart confirmation
- Service: sustainacore-ai.service (systemd)
- Restart: sudo systemctl restart sustainacore-ai.service
- Health: http://127.0.0.1:8080/healthz (200 OK)

Harness
- Command: python3 tools/ask2_eval/run_cases.py --k 6 --timeout 8

Before (fragmented + run-on formatting)
- File: tools/ask2_eval/out/20251230_150114/summary.md
- Excerpt:
  "**Answer** Yes – Microsoft Corporation is included in the July 2025 portfolio... • Rank 1... by its financial results and stock performance[81][41]."

After (sanitized + structured formatting)
- File: tools/ask2_eval/out/20251230_151721/summary.md
- Excerpt:
  "**Answer**\nYes – Microsoft Corporation is included in the July 2025 portfolio of the TECH100 AI Governance & Ethics Index.\n\n**Key facts (from SustainaCore)**\n- SustainaCore Launches the TECH100 AI Governance & Ethics Index.\n- 1 – Microsoft Corporation ranks first in the July 2025 TECH100 AI Governance & Ethics Index.\n- Yes – Microsoft Corporation is included in the July 2025 portfolio of the TECH100 AI Governance & Ethics Index.\n\n**Evidence**\n- Membership › TECH100 AI Governance & Ethics Index › Microsoft Corporation (ID: 3800): \"Yes – Microsoft Corporation is included in the July 2025 portfolio...\""
