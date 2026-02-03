# Local Secrets (Do Not Commit)

This repository is public. Secrets, keys, and wallets must **never** enter git history.

## Where secrets live
- Store all sensitive material outside the repo, under a local-only folder such as:
  `C:\Users\<your_user>\...\\Keys`
- Do not copy anything from `Keys/` into the repo.

## How to reference secrets safely
- Use environment variables in your shell.
- Use `.env.example` for variable names only (blank values).
- Use `SSH_KEY_PATH` to point to a private key outside the repo.
- Use WSL paths when needed, e.g. `/mnt/c/Users/<your_user>/.../Keys/...`.

## Git safety
- `.gitignore` includes patterns for keys, wallets, `.env*`, and local artifacts.
- If you accidentally create a secret file in the repo, delete it immediately and verify `git status` is clean.
