# Codex UI Loop Status

## Attempt 1
- Action: verify preview/prod via HTTPS (curl --connect-timeout 2 --max-time 4)
- Result: both preview/prod HTTPS timed out (curl 28)
- Switch: checked local upstreams
- Local: preview http://127.0.0.1:8001 -> 200, prod http://127.0.0.1:8000 -> timeout
- Next: proceed with CI-only UI compare and create UI PR; treat external curl timeouts as VM2 network constraint

## Attempt 2
- Action: create UI PR #409 with base.html comment trigger
- Result: ui_compare_home run started (run id 21109689619) and still in_progress after short polls
- Next: continue polling run id 21109689619 and download artifacts when complete

## Attempt 3
- Action: UI compare run 21109689619 failed due to diff threshold
- Issue: artifacts missing because script wrote to website_django/artifacts/ui but upload path was artifacts/ui
- Fix: set OUTPUT_DIR to ../artifacts/ui in workflow
- Next: push fix and re-run ui_compare_home

## Attempt 4
- Action: reran ui_compare_home (run 21109716658) after artifact path fix
- Result: run completed but failed threshold; artifacts downloaded and committed to docs/screenshots/ui/home/pr-409/run-21109716658
- Diff: mismatchPixels=144531, mismatchPercent=11.1521
- Preview deploy: ops/scripts/deploy_preview_vm2.sh 409 restarted gunicorn-preview; local 127.0.0.1:8001 still timed out
- Next: review diff images, confirm preview is serving PR branch, adjust UI or deployment verification

## Attempt 5
- Hypothesis: diff is dominated by preview banner + consent banner (expected on preview only)
- Fix: hide .preview-banner and .consent-banner in ui_compare_home.mjs for preview capture
- Next: rerun UI compare and re-commit artifacts

## Attempt 6
- Observation: diff still 3.6% after hiding preview banner
- Fix: hide dynamic Tech100 sections and hero list in ui_compare_home.mjs for both prod/preview
- Next: rerun ui_compare_home and re-commit artifacts
