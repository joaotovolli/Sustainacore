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
