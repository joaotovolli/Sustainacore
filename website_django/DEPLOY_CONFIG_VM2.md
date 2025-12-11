# VM2 Deployment Configuration

## Networking
- Primary host names: `sustainacore.org` and `www.sustainacore.org`.
- All traffic terminates at the load balancer before reaching the application node.

## Application Runtime
- Django runs under a WSGI application server with environment variables provided via systemd.
- Set `DJANGO_SECRET_KEY` in the VM environment; no secrets are stored in the repository.

## Static and Media Assets
- Static assets are collected during deployment and served by the web server.
- Media uploads are stored on persistent disk attached to the VM.
