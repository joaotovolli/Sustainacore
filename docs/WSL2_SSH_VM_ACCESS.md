# WSL2 SSH Access to VM1 / VM2

This doc explains how to configure SSH access from WSL2 without storing keys in the repo.

## Key storage (outside repo)
- Store SSH keys under `~/.ssh/` in WSL2.
- Do **not** copy keys into the repo or `local_artifacts/`.
- If you keep keys in Windows, copy them into WSL2 manually (outside the repo), then:
  ```bash
  chmod 600 ~/.ssh/<key_name>
  ```

## Optional: SSH config aliases
Create or update `~/.ssh/config`:
```ssh-config
Host vm1
  HostName <VM1_HOST>
  User <VM1_USER>
  IdentityFile ~/.ssh/<key_name>
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new

Host vm2
  HostName <VM2_HOST>
  User <VM2_USER>
  IdentityFile ~/.ssh/<key_name>
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
```

## Smoke check (safe)
Use the bounded script (no secrets printed):
```bash
VM1_HOST=... VM1_USER=... VM2_HOST=... VM2_USER=... \
  [SSH_KEY_PATH=~/.ssh/<key_name>] bash scripts/dev/ssh_smoke.sh
```

Expected output:
- `uname -a` and `echo OK` for each VM

## Notes
- `StrictHostKeyChecking=accept-new` is safe for first-time host key acceptance.
- Use `BatchMode=yes` to avoid interactive prompts in automation.
- If a smoke check fails, re-run with `SSH_SMOKE_DEBUG=1` to show SSH error details.
