# Local Development (WSL2)

This repo is designed for Linux tooling. The recommended workflow is to work from WSL2 so the bash scripts and Python tooling behave the same as CI and the VMs.
WSL2 is the primary environment for Django-heavy development; VM1/VM2 are deployment targets only.

## Prereqs (Windows)
- WSL2 installed with an Ubuntu distro.
- Git and Python 3 in the WSL distro.

If WSL is not installed, run these **PowerShell (Admin)** commands (do not run from here):
```powershell
wsl.exe --install
wsl.exe --set-default-version 2
wsl.exe --install -d Ubuntu
```

## WSL repo workspace
From WSL, use the Windows path via `/mnt`:
```bash
cd "/mnt/c/Users/joaot/OneDrive/Documentos/My Project/codex/Sustainacore"
```

## Bootstrap (WSL2)
This sets up a local venv and installs dependencies for both the API and Django app, then runs a Django config check if present.
```bash
bash scripts/dev/setup_wsl2.sh
```

If you only need the Django website locally and want a faster setup:
```bash
SKIP_ROOT_DEPS=1 bash scripts/dev/setup_wsl2.sh
```

## Run Django locally (WSL2)
```bash
bash scripts/dev/run_django.sh
```

Notes:
- The local Django script defaults to fixture mode (`NEWS_UI_DATA_MODE=fixture`, `TECH100_UI_DATA_MODE=fixture`, `AI_REG_UI_DATA_MODE=fixture`).
- To wait until the server is ready:
  ```bash
  scripts/dev/wait_for_http.sh http://127.0.0.1:8000/ 60 2
  ```

## Run CI-like sanity checks (WSL2)
```bash
bash scripts/dev/run_tests.sh
```

## Snapshot pages (local vs prod)
```bash
python scripts/dev/preview_verify.py --timeout 15
```

Snapshots and reports are stored under `local_artifacts/` and are intentionally not committed.

## Screenshots (local vs prod)
```bash
cd website_django
../scripts/dev/capture_screenshots.sh ../local_artifacts/url_list_<timestamp>.json \
  ../local_artifacts/screenshots_<timestamp>
```

## SSH connectivity check (VM1/VM2)
```bash
VM1_HOST=... VM1_USER=... VM2_HOST=... VM2_USER=... \
  [SSH_KEY_PATH=...] bash scripts/dev/ssh_smoke.sh
```

## Oracle connectivity check (optional)
Oracle is optional for local dev and only runs if env vars are present.
```bash
python scripts/dev/oracle_smoke.py
```

## Deploy to VM2 / VM1 (WSL2)
VM2:
```bash
VM2_HOST=... VM2_USER=... APP_DIR_VM2=... SSH_KEY_PATH=... \
  bash scripts/deploy/deploy_vm2.sh
```

VM1:
```bash
VM1_HOST=... VM1_USER=... APP_DIR_VM1=... SSH_KEY_PATH=... \
  bash scripts/deploy/deploy_vm1.sh
```

## Notes
- The Django project lives under `website_django/`.
- The FastAPI app entrypoint is `app.retrieval.app:app` (see `README.md`).
- The VM deploy scripts live under `scripts/deploy/` and are designed to run from WSL.
- `local_artifacts/` is used for local-only snapshots and is ignored by git.
