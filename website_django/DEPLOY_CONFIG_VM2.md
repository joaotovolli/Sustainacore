# VM2 Django Deploy Configuration

Set these environment variables for the website service (e.g., in the systemd unit or an `/etc/default/` env file):

- `DJANGO_SECRET_KEY` – required. New secret key for Django sessions.
- `DJANGO_DEBUG` – optional. `true/1` to enable debug; defaults to `false`.
- `DJANGO_ALLOWED_HOSTS` – optional comma-separated hostnames/IPs. Defaults to `sustainacore.org,www.sustainacore.org,141.147.76.55,localhost`.

If using SQLite (current setup):
- Ensure the working directory is writable so `db.sqlite3` can be created at runtime.
- Keep `db.sqlite3` out of Git (already ignored).

Example snippet for systemd (`/etc/systemd/system/sustainacore.service`):

```
[Service]
Environment="DJANGO_SECRET_KEY=<new-generated-key>"
Environment="DJANGO_DEBUG=false"
Environment="DJANGO_ALLOWED_HOSTS=sustainacore.org,www.sustainacore.org,141.147.76.55"
WorkingDirectory=/opt/sustainacore/website_django
ExecStart=/opt/sustainacore/website_django/venv/bin/gunicorn core.wsgi:application --bind 0.0.0.0:8000
```

Reload systemd after edits and restart the service to apply new values.
