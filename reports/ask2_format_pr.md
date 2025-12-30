Title: Ask2 structured formatting + harness verification

Restart confirmation
- Service: sustainacore-ai.service (systemd)
- Restart: sudo systemctl restart sustainacore-ai.service
- Health: http://127.0.0.1:8080/healthz (200 OK)

Harness
- Command: python3 tools/ask2_eval/run_cases.py --k 6 --timeout 8

Before (missing Sources header + single-line sections)
- File: tools/ask2_eval/out/20251230_153539/summary.md
- Excerpt:
  "**Answer**\nYes – Microsoft Corporation is included...\n\n**Key facts**\n- ...\n\n1. Membership › TECH100 AI Governance & Ethics Index › Microsoft Corporation — /sources/..."

After (sanitized + structured formatting + Sources)
- File: tools/ask2_eval/out/20251230_153841/summary.md
- Excerpt:
  "**Answer**\nYes – Microsoft Corporation is included in the July 2025 portfolio of the TECH100 AI Governance & Ethics Index.\n\n**Key facts**\n- SustainaCore Launches the TECH100 AI Governance & Ethics Index.\n- 1 – Microsoft Corporation ranks first in the July 2025 TECH100 AI Governance & Ethics Index.\n- Yes – Microsoft Corporation is included in the July 2025 portfolio of the TECH100 AI Governance & Ethics Index.\n\n**Sources**\n1. Membership › TECH100 AI Governance & Ethics Index › Microsoft Corporation — /sources/membership-tech100-ai-governance-ethics-index-microsoft-corporation"
