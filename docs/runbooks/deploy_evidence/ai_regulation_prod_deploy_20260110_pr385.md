# AI regulation discoverability deploy evidence (PR 385)

- Date: 2026-01-10
- Commit: 436da423db794f54cf73f17f82a827811b8555e3

## Deploy commands
- `git fetch origin`
- `git checkout main`
- `git pull --ff-only`
- `bash ./deploy_vm2_website.sh`

## Service status (truncated)

### gunicorn.service
```
● gunicorn.service - SustainaCore Django website (VM2)
     Loaded: loaded (/etc/systemd/system/gunicorn.service; enabled; preset: enabled)
    Drop-In: /etc/systemd/system/gunicorn.service.d
             └─20-oracle-db.conf, 50-backend-api.conf, 60-admin-email.conf, sustainacore-env.conf
     Active: active (running) since Sat 2026-01-10 18:09:58 UTC; 32s ago
   Main PID: 471907 (gunicorn)
      Tasks: 4 (limit: 1039)
     Memory: 76.9M (peak: 80.1M)
        CPU: 1.644s
     CGroup: /system.slice/gunicorn.service
             ├─471907 /home/ubuntu/.venvs/sustainacore_vm2/bin/python /home/ubuntu/.venvs/sustainacore_vm2/bin/gunicorn core.wsgi:application --bind 0.0.0.0:8000
             └─471919 /home/ubuntu/.venvs/sustainacore_vm2/bin/python /home/ubuntu/.venvs/sustainacore_vm2/bin/gunicorn core.wsgi:application --bind 0.0.0.0:8000

Jan 10 18:09:58 vm2---django systemd[1]: Started gunicorn.service - SustainaCore Django website (VM2).
Jan 10 18:09:58 vm2---django gunicorn[471907]: [2026-01-10 18:09:58 +0000] [471907] [INFO] Starting gunicorn 21.2.0
Jan 10 18:09:58 vm2---django gunicorn[471907]: [2026-01-10 18:09:58 +0000] [471907] [INFO] Listening at: http://0.0.0.0:8000 (471907)
Jan 10 18:09:58 vm2---django gunicorn[471907]: [2026-01-10 18:09:58 +0000] [471907] [INFO] Using worker: sync
Jan 10 18:09:58 vm2---django gunicorn[471919]: [2026-01-10 18:09:58 +0000] [471919] [INFO] Booting worker with pid: 471919
```

### nginx.service
```
● nginx.service - A high performance web server and a reverse proxy server
     Loaded: loaded (/usr/lib/systemd/system/nginx.service; enabled; preset: enabled)
     Active: active (running) since Mon 2025-12-29 09:32:35 UTC; 1 week 5 days ago
       Docs: man:nginx(8)
    Process: 471914 ExecReload=/usr/sbin/nginx -g daemon on; master_process on; -s reload (code=exited, status=0/SUCCESS)
   Main PID: 1001 (nginx)
      Tasks: 3 (limit: 1039)
     Memory: 6.0M (peak: 10.8M)
        CPU: 21.752s
     CGroup: /system.slice/nginx.service
             ├─  1001 "nginx: master process /usr/sbin/nginx -g daemon on; master_process on;"
             ├─471917 "nginx: worker process"
             └─471918 "nginx: worker process"

Jan 10 17:46:45 vm2---django systemd[1]: Reloaded nginx.service - A high performance web server and a reverse proxy server.
Jan 10 17:50:10 vm2---django systemd[1]: Reloading nginx.service - A high performance web server and a reverse proxy server...
Jan 10 17:50:10 vm2---django nginx[470060]: 2026/01/10 17:50:10 [notice] 470060#470060: signal process started
Jan 10 17:50:10 vm2---django systemd[1]: Reloaded nginx.service - A high performance web server and a reverse proxy server.
Jan 10 18:06:45 vm2---django systemd[1]: Reloading nginx.service - A high performance web server and a reverse proxy server...
Jan 10 18:06:45 vm2---django nginx[471228]: 2026/01/10 18:06:45 [notice] 471228#471228: signal process started
Jan 10 18:06:45 vm2---django systemd[1]: Reloaded nginx.service - A high performance web server and a reverse proxy server.
Jan 10 18:09:58 vm2---django systemd[1]: Reloading nginx.service - A high performance web server and a reverse proxy server...
Jan 10 18:09:58 vm2---django nginx[471914]: 2026/01/10 18:09:58 [notice] 471914#471914: signal process started
Jan 10 18:09:58 vm2---django systemd[1]: Reloaded nginx.service - A high performance web server and a reverse proxy server.
```

## Production validation (truncated)

### Home page nav + promo
```
Nav link:
44:                    <a href="/ai-regulation/" class="nav__link">AI Regulation</a>
Promo link:
250:            <a class="text-link" href="/ai-regulation/">Open AI Regulation →</a>
Promo button:
256:                <a class="btn btn--primary" href="/ai-regulation/">View the heat map</a>
```

### TECH100 cross-link
```
TECH100 link matches:
42:                    <a href="/ai-regulation/" class="nav__link">AI Regulation</a>
141:          <a class="btn btn--secondary" href="/ai-regulation/">Open AI Regulation</a>
```

### GET /ai-regulation/
```
HTTP/2 200 
server: nginx/1.24.0 (Ubuntu)
date: Sat, 10 Jan 2026 18:10:26 GMT
content-type: text/html; charset=utf-8
content-length: 15752
vary: Accept-Encoding
x-frame-options: DENY
vary: Cookie
x-content-type-options: nosniff
referrer-policy: same-origin
cross-origin-opener-policy: same-origin
set-cookie: csrftoken=5KpRUmjUqU04daX35iTFhOgR4XxCvAfz; expires=Sat, 09 Jan 2027 18:10:26 GMT; Max-Age=31449600; Path=/; SameSite=Lax
```

### GET /static/css/ai_reg.css
```
HTTP/2 200 
server: nginx/1.24.0 (Ubuntu)
date: Sat, 10 Jan 2026 18:10:26 GMT
content-type: text/css
content-length: 2839
last-modified: Sat, 10 Jan 2026 17:50:09 GMT
vary: Accept-Encoding
etag: "69629151-b17"
cache-control: public, max-age=86400
accept-ranges: bytes

```

### GET /static/js/ai_regulation.js
```
HTTP/2 200 
server: nginx/1.24.0 (Ubuntu)
date: Sat, 10 Jan 2026 18:10:26 GMT
content-type: application/javascript
content-length: 15457
last-modified: Sat, 10 Jan 2026 17:50:09 GMT
vary: Accept-Encoding
etag: "69629151-3c61"
cache-control: public, max-age=86400
accept-ranges: bytes

```
