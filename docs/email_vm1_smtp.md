## VM1 SMTP (IONOS) setup

This enables sending mail from `info@sustainacore.org` on VM1 via IONOS (STARTTLS on 587).

### Setup and test

Run interactively (prompts for the mailbox password; writes to `/etc/sustainacore-ai/secrets.env`):

```bash
sudo bash tools/email/setup_ionos_smtp_vm1.sh
```

Defaults: SMTP host `smtp.ionos.co.uk`, port `587`, user/mail-from `info@sustainacore.org`, mail-to `joaotovolli@outlook.com`. Secrets are stored only in `/etc/sustainacore-ai/secrets.env` (root:root, 600). A real email is sent using stdlib `smtplib`.

### Troubleshooting

- Connection refused / timed out: verify network egress and host/port; the setup script checks TCP connectivity before sending.
- Auth failed: re-run the setup script and re-enter the correct IONOS mailbox password.
- STARTTLS issues: ensure port 587 and STARTTLS are allowed; no extra dependencies are used.

### Login code fast-fail tuning
Login code delivery uses short SMTP timeouts by default to avoid hanging the auth API.
You can override (VM1 env only; do not commit):
- `LOGIN_CODE_SMTP_TIMEOUT_SEC` (default `3`)
- `LOGIN_CODE_SMTP_RETRY_ATTEMPTS` (default `0`)
- `LOGIN_CODE_SMTP_RETRY_BASE_SEC` (default `0.5`)

These settings only affect one-time login codes and keep `POST /api/auth/request-code` responsive.
