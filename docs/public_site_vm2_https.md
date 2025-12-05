# Sustainacore Public Site (VM2 HTTPS)

## Overview
- Main public site: Django app on VM2 behind Nginx → Gunicorn at https://sustainacore.org.
- APEX is kept for secondary/admin use only; it is not the primary public front-end.

## DNS
- IONOS A records for `@` and `www` both point to `141.147.76.55` (VM2).

## Web Server
- Nginx on VM2 listens on ports 80/443 and reverse-proxies to Gunicorn on `http://127.0.0.1:8000`.
- Static files are served from `/opt/code/Sustainacore/website_django/static/` via Nginx.
- HTTP (80) redirects to HTTPS (443).

## HTTPS / Certbot
- Let’s Encrypt certificates managed by Certbot:
  - `ssl_certificate`: `/etc/letsencrypt/live/sustainacore.org/fullchain.pem`
  - `ssl_certificate_key`: `/etc/letsencrypt/live/sustainacore.org/privkey.pem`
- To (re)issue the cert:
  ```bash
  sudo certbot --nginx -d sustainacore.org -d www.sustainacore.org
  ```
- Renewal is handled by Certbot’s systemd timer (`snap.certbot.renew.timer`).
