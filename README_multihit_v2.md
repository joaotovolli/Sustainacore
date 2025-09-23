# wsgi_multihit_bundle_v2

Safe, append-only drop-in that adds a multi-hit retrieval + RRF/MMR orchestrator in front of `/ask` without touching systemd or NGINX.

## What it does
- Normalizes noisy UI prompts.
- Classifies intent; extracts entities.
- Generates 3–5 query variants.
- Calls downstream `/ask` multiple times internally and fuses results (RRF + MMR).
- Composes an evidence-first answer (direct answer → bullets referencing [S1..] → source map).
- Adds telemetry headers: `X-Intent`, `X-K`, `X-RRF`, `X-MMR`, `X-Variants`, `X-Question-Normalized`, `X-ElapsedMs`.

## Install
```bash
set -euo pipefail
unzip -l /home/opc/wsgi_multihit_bundle_v2.zip

TMP=$(mktemp -d)
unzip -q /home/opc/wsgi_multihit_bundle_v2.zip -d "$TMP"
sudo rsync -a "$TMP/opt/sustainacore-ai/" /opt/sustainacore-ai/
sudo chown -R opc:opc /opt/sustainacore-ai

bash /opt/sustainacore-ai/tools/POST_DEPLOY.sh
```

## Tuning (env overrides, optional)
- `SMART_RRF_K` (default 60)
- `SMART_MMR_LAMBDA` (default 0.7)
- `SMART_VARIANTS` (default 3)
- `SMART_K_SEQ` (default "8,16,24")
- `SMART_MAX_CONTEXTS` (default 12)
- `SMART_LATENCY_MS` (default 2400)
