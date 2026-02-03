# WSL2 Oracle Access (Optional)

Local development defaults to fixture mode and does not require Oracle.
Only enable Oracle locally if you have a wallet and credentials stored **outside** the repo.

## Local dev modes
- Default: fixture mode (no Oracle required)
- Optional: Oracle mode (local env only)

## Thick mode (Instant Client)
If thin mode is unstable, install Oracle Instant Client Basic (Linux x86-64) locally and enable thick mode:
```bash
mkdir -p ~/.oracle/instantclient
unzip instantclient-basic-linux.x64-*.zip -d ~/.oracle/instantclient
ln -sfn ~/.oracle/instantclient/instantclient_* ~/.oracle/instantclient/current
```
Then ensure the shared library path is available (prefer system linker, fallback to env):
```bash
export LD_LIBRARY_PATH="$HOME/.oracle/instantclient/current:${LD_LIBRARY_PATH:-}"
```

## Copy wallet locally (outside repo)
Use the VM alias to copy the wallet into WSL2 local storage (outside the repo):
```bash
mkdir -p ~/.oracle/<profile>
chmod 700 ~/.oracle ~/.oracle/<profile>
scp vm1:/path/to/wallet/tnsnames.ora ~/.oracle/<profile>/
scp vm1:/path/to/wallet/sqlnet.ora ~/.oracle/<profile>/
scp vm1:/path/to/wallet/cwallet.sso ~/.oracle/<profile>/
scp vm1:/path/to/wallet/ewallet.p12 ~/.oracle/<profile>/
chmod 600 ~/.oracle/<profile>/*
```

## Local-only Oracle env vars
Set these in your shell (do not commit):
```bash
export ORACLE_WALLET_DIR="$HOME/.oracle/<profile>"
export ORACLE_TNS_ADMIN="$HOME/.oracle/<profile>"
export ORACLE_DSN="<service_alias>"
export ORACLE_USER="<user>"
export ORACLE_PASSWORD="<password>"
export LD_LIBRARY_PATH="$HOME/.oracle/instantclient/current:${LD_LIBRARY_PATH:-}"
```

Notes:
- Never commit these values.
- Do not print wallet contents or `.env` values.
- If your wallet uses `TNS_ADMIN`, set it to the wallet directory.
- Keep any local-only env file (e.g., `~/.sustainacore_oracle_env`) outside the repo.
- If TLS DN matching causes local connection failures, you may set `SSL_DN_MATCH=no` (or `ORACLE_SSL_DN_MATCH=no`)
  in your local shell for the smoke test only. Do not commit this setting.

## Smoke check (safe)
Runs only if env vars are present and prints success/failure only:
```bash
source .venv/bin/activate
timeout 20s python scripts/dev/oracle_smoke.py
```

## Fixture mode envs (local preview)
```bash
export NEWS_UI_DATA_MODE=fixture
export TECH100_UI_DATA_MODE=fixture
export AI_REG_UI_DATA_MODE=fixture
```
