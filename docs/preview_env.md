# Preview Environment (preview.sustainacore.org)

## Purpose
The preview environment is a protected clone of the production Django site for testing changes with real Oracle data.
It is **Basic Auth protected** and **noindex** by both Nginx and Django.

## DNS
Create an A record in IONOS:
- `preview` → `141.147.76.55`

## VM2 setup steps
1) Create/update Basic Auth file:
```bash
BASIC_AUTH_USER=joaogt BASIC_AUTH_PASS=joaogt \
  bash /opt/code/Sustainacore/tools/vm2/setup_preview_basic_auth.sh
```

2) Install the preview Nginx config and reload:
```bash
sudo bash /opt/code/Sustainacore/tools/vm2/deploy_preview_nginx.sh
```

3) Validate locally before DNS:
```bash
curl -i -u 'joaogt:joaogt' -H 'Host: preview.sustainacore.org' http://127.0.0.1/
curl -i -u 'joaogt:joaogt' -H 'Host: preview.sustainacore.org' http://127.0.0.1/robots.txt
```

Expect:
- `200` for `/`
- `X-Robots-Tag` header
- `robots.txt` with `Disallow: /`

## TLS (Certbot)
After DNS is live, expand the existing cert:
```bash
sudo certbot --nginx --expand -d sustainacore.org -d www.sustainacore.org -d preview.sustainacore.org
```

## Preview link (after DNS + TLS)
Open:
- `https://preview.sustainacore.org`

## Screenshots (preview)
```bash
cd /opt/code/Sustainacore/website_django
TECH100_SCREENSHOT_DIR=preview \
TECH100_BASE_URL=https://preview.sustainacore.org \
TECH100_BASIC_AUTH_USER=joaogt \
TECH100_BASIC_AUTH_PASS=joaogt \
TECH100_SCREENSHOT_MODE=after \
node scripts/run_tech100_screenshots_ci.mjs
```

Artifacts are stored in `docs/screenshots/preview/{before,after,diff}`.

## Rotate Basic Auth password
Re-run the setup script with new credentials:
```bash
BASIC_AUTH_USER=joaogt BASIC_AUTH_PASS=NEWPASS \
  bash /opt/code/Sustainacore/tools/vm2/setup_preview_basic_auth.sh
```

## Disable preview quickly
```bash
sudo rm -f /etc/nginx/conf.d/preview.sustainacore.conf
sudo nginx -t && sudo systemctl reload nginx
```
