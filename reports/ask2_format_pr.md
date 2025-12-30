Title: Ask2 structured formatting + harness verification

Restart confirmation
- Service: sustainacore-ai.service (systemd)
- Restart: sudo systemctl restart sustainacore-ai.service
- Health: http://127.0.0.1:8080/healthz (200 OK)

Harness
- Command: python3 tools/ask2_eval/run_cases.py --k 6 --timeout 8

Before (pre-restart, run-on formatting)
- File: tools/ask2_eval/out/20251230_143240/summary.md
- Excerpt:
  "Here's the best supported summary from SustainaCore: - Membership › TECH100 AI Governance & Ethics Index › Microsoft Corporation: Yes – Microsoft Corporation is included... Key sco"

After (post-restart, structured formatting)
- File: tools/ask2_eval/out/20251230_144645/summary.md
- Excerpt:
  "**Answer**\nYes – Microsoft Corporation is included...\n\n**Key facts (from SustainaCore)**\n- Yes – Microsoft Corporation is included...\n\n**Evidence**\n- Membership › TECH100 AI Governance & Ethics Index › Microsoft Corporation (ID: 3800): \"Yes – Microsoft Corporation is included...\""
