# Ask2 VM1 Safety + Recovery Runbook

VM1 is resource constrained (1 CPU core, ~1 GB RAM). Treat it as a fragile target.

## Symptoms

### Symptom: SSH fails with banner exchange timeout / connection closed
Examples:
- `Connection timed out during banner exchange`
- `Connection closed by UNKNOWN port 65535`

### Symptom: VM2 cannot reach VM1 backend
Examples:
- `/ask2/api/` returns `502 backend_failure` from VM2 with a timeout to `http://<VM1_PRIVATE_IP>:8080`.
- On VM2: `curl http://<VM1_PRIVATE_IP>:8080/healthz` times out.

## Root Cause Hypothesis
- VM1 is CPU or memory saturated (often due to embedding/ingest runs), causing sshd and/or the API to stop responding.
- Long-running ingestion without bounds (large shards, no `timeout`, no cooldown) can starve the instance.

## Golden Rules (VM1)
- Run ONE command at a time.
- Wait 10-20 seconds between VM1 commands (more after restarts/ingest).
- No parallelism, no watchers, no `--workers > 1`.
- Prefer small, bounded commands. If a command is expected to take time: wrap with `timeout`.

## Preflight Checklist

### From VM2 (preferred path)
1. Confirm backend is reachable:
   - `curl -fsS --max-time 5 http://<VM1_PRIVATE_IP>:8080/healthz`
2. If this fails, do not proceed with ingestion.

### From WSL2 (jump through VM2)
Use the VM2 private IP that VM2 is configured to call (usually from `BACKEND_API_BASE`).

## Safe Ingestion Recipe (Oracle ESG_DOCS)

### Design constraints
- Shard size: 10-25 chunks per file max.
- Time bound: `timeout 120s` per shard.
- CPU pressure: `nice -n 10`.
- Cooldown: sleep 30-60s between shards.
- Stop early if:
  - 2 shard timeouts occur, or
  - VM2 -> VM1 healthz starts timing out.

### Command template (VM1)
Use the repo ingest tool and keep batch sizes small:

```bash
timeout 120s nice -n 10 \
  sudo -n env PYTHONPATH=/opt/sustainacore-ai \
  /opt/sustainacore-ai/.venv/bin/python \
  /opt/sustainacore-ai/tools/rag/ingest_public_corpus.py \
  --in-jsonl /tmp/ask2_corpus_shards_v2/<shard>.jsonl \
  --batch-size 10
```

Then cool down:
```bash
sleep 45
```

### Post-shard verification (VM1)
Verify counts by `SOURCE_TYPE` (and optionally restrict to public URLs):
- Total by type:
  - `SELECT source_type, COUNT(*) FROM ESG_DOCS GROUP BY source_type ORDER BY COUNT(*) DESC`
- Public URLs only:
  - `WHERE source_url LIKE 'https://sustainacore.org/%'`

Run verification as a short Python snippet using the repo bootstrap helpers (do not print secrets).

## Recovery Steps When SSH Breaks

1. Stop attempting new ingest shards.
2. Check reachability from VM2:
   - `curl -fsS --max-time 5 http://<VM1_PRIVATE_IP>:8080/healthz`
3. Wait 2 minutes and retry SSH once (no tight loops).
4. If SSH still fails:
   - Reboot VM1 via OCI console (Instance Actions -> Reboot).
5. After reboot:
   - Wait until VM2 -> VM1 `/healthz` is reachable again.
   - Verify `sustainacore-ai.service` is active.
   - Resume ingestion with smaller shards (halve shard size if the last shard timed out).

## Notes
- Avoid depending on a local Ollama daemon on VM1. In-process embeddings are safer on small instances.
- If VM2 is returning `502 backend_failure`, treat it as a production outage for Ask2 and stop ingestion immediately.

