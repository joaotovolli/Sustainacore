# WSL2 Oracle Access (Optional)

Local development defaults to fixture mode and does not require Oracle.
Only enable Oracle locally if you have a wallet and credentials stored **outside** the repo.

## Local dev modes
- Default: fixture mode (no Oracle required)
- Optional: Oracle mode (local env only)

## Local-only Oracle env vars
Set these in your shell (do not commit):
```bash
export ORACLE_WALLET_DIR=/path/to/wallet
export ORACLE_DSN=...
export ORACLE_USER=...
export ORACLE_PASSWORD=...
```

Notes:
- Never commit these values.
- Do not print wallet contents or `.env` values.
- If your wallet uses `TNS_ADMIN`, set it to the wallet directory.

## Smoke check (safe)
Runs only if env vars are present and prints success/failure only:
```bash
source .venv/bin/activate
python scripts/dev/oracle_smoke.py
```

## Fixture mode envs (local preview)
```bash
export NEWS_UI_DATA_MODE=fixture
export TECH100_UI_DATA_MODE=fixture
export AI_REG_UI_DATA_MODE=fixture
```
