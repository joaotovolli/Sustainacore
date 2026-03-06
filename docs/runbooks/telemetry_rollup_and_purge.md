# Telemetry Rollup And Purge Runbook

Use this runbook after Oracle writes are healthy again. Do not enable the VM2 rollup timer while `ORA-65114` is still happening.

## 1. Verify the write path
Public endpoint check:
```bash
python3 - <<'PY'
import json, requests, time
s = requests.Session()
path = f"/telemetry-runbook-check-{int(time.time())}/"
headers = {
    "User-Agent": "Mozilla/5.0 (TelemetryRunbook)",
    "Origin": "https://sustainacore.org",
    "Referer": "https://sustainacore.org/",
    "Content-Type": "application/json",
}
consent = s.post(
    "https://sustainacore.org/telemetry/consent/",
    headers=headers,
    data=json.dumps({"analytics": True, "functional": False, "source": "runbook"}),
    timeout=10,
)
event = s.post(
    "https://sustainacore.org/telemetry/event/",
    headers=headers,
    data=json.dumps({"event_name": "download_click", "metadata": {"page": f"https://sustainacore.org{path}"}}),
    timeout=10,
)
print("path", path)
print("consent_status", consent.status_code)
print("event_status", event.status_code)
PY
```

DB verification on VM2:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh shell -c \"from telemetry.models import WebEvent; print(WebEvent.objects.order_by('-event_ts').values_list('event_ts','event_type','event_name','path').first())\""
```

## 2. Run rollup manually
Roll yesterday by default:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh aggregate_web_telemetry"
```

Specific UTC day:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh aggregate_web_telemetry --date 2026-03-05"
```

## 3. Verify aggregate rows
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh shell -c \"from telemetry.models import WebEventDaily; print('daily_total', WebEventDaily.objects.count()); print('latest', WebEventDaily.objects.order_by('-bucket_date').values_list('bucket_date','event_type','event_count').first())\""
```

## 4. Purge raw rows safely
Non-destructive safety check first:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh purge_web_telemetry --raw-days 10000 --aggregate-days 10000 --session-days 10000 --consent-days 10000 --aggregates --sessions --consents"
```

Real retention run after aggregate verification:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && scripts/vm2_manage.sh purge_web_telemetry --aggregates --sessions --consents"
```

## 5. Enable the VM2 timer
Only after the manual write-path and rollup checks succeed:
```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 vm2 \
  "cd /opt/code/Sustainacore && sudo cp infra/systemd/sc-web-telemetry-rollup.* /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now sc-web-telemetry-rollup.timer"
```

## 6. Failure signatures
- `ORA-65114: space usage in container is too high`
  - raw inserts and aggregate inserts will both fail
  - do not enable the timer until this is resolved
- `raw_match_count 0` after a `204` telemetry response
  - the endpoint is reachable but persistence is still broken
