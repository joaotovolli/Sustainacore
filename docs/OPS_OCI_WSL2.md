# OCI CLI + SSH Setup (WSL2) and Ask2 Wiring (VM2 -> VM1)

This document is a public, placeholder-only checklist for configuring:

- OCI CLI authentication on WSL2 (local files only)
- SSH host aliases for VM access (local files only)
- Ask2 request path (VM2 Django + Nginx -> VM1 backend over VCN private IP)

It intentionally does **not** include secrets, OCIDs, fingerprints, or key material.

## OCI CLI (WSL2 Local Only)

### Files and permissions

Create:

- `~/.oci/` with mode `700`
- `~/.oci/oci_api_key.pem` with mode `600`
- `~/.oci/config` with mode `600`

Example `~/.oci/config` (placeholders):

```ini
[DEFAULT]
user=<USER_OCID>
tenancy=<TENANCY_OCID>
region=<REGION>
fingerprint=<FINGERPRINT>
key_file=<KEY_FILE>  # e.g. /home/<user>/.oci/oci_api_key.pem
```

See `docs/templates/oci_config.example`.

### Verification commands

Run without printing config contents:

```bash
oci -v
oci os ns get
oci iam user get --user-id <USER_OCID>
```

## SSH Host Aliases (WSL2 Local Only)

Add a VM1 alias to `~/.ssh/config` (placeholders):

```sshconfig
Host vm1-bot
    HostName <VM1_PUBLIC_IP>
    User ubuntu
    IdentityFile ~/.ssh/<VM1_KEY>.key
    IdentitiesOnly yes
    StrictHostKeyChecking accept-new
```

Verify:

```bash
ssh vm1-bot "whoami; hostname; uptime"
```

## Ask2 Wiring (VM2 -> VM1)

### Data flow

1. Browser calls VM2 at `/ask2/api/` (Django).
2. VM2 forwards to VM1 backend at `http://<VM1_PRIVATE_IP>:8080`.
3. VM1 runs the Ask2 backend service and replies to VM2, which returns to the browser.

### VM1 requirements

- VM1 backend listens on port `8080` on a non-loopback interface.
- Port `8080` is **not** open publicly.
- Only allow inbound from the VCN private range (example: `10.0.0.0/24`) or from VM2 private IP.

Health check:

```bash
curl -fsS --max-time 3 http://127.0.0.1:8080/healthz
```

### VM2 configuration

VM2 must point at the *new* VM1 private IP. Common locations:

- Nginx proxy config for backend API routes (example `proxy_pass http://<VM1_PRIVATE_IP>:8080;`)
- `/etc/sysconfig/sustainacore-django.env` (or similar) containing:
  - `BACKEND_API_BASE=http://<VM1_PRIVATE_IP>:8080`

After updating, restart VM2 services (example):

```bash
sudo nginx -t
sudo systemctl reload nginx
sudo systemctl restart gunicorn.service gunicorn-preview.service
```

### Compatibility note (routes)

VM2 historically forwarded Ask2 requests to VM1 at `/api/ask2`. VM1 should keep a
backwards-compatible alias route for `/api/ask2` that maps to the current Ask2 handler.

