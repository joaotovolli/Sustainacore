# SEO Foundations (SustainaCore.org)

## What was implemented
- `robots.txt` served at `/robots.txt` with crawl rules and sitemap reference.
- `sitemap.xml` served via Django sitemap framework with core public pages and lastmod timestamps, with the sitemap view removing Django's default `X-Robots-Tag` header.
- `/sitemap.xml` is cached in-memory for 1 hour to avoid transient upstream hiccups (`SITEMAP_CACHE_SECONDS`).
- Canonical host redirects enforced in Nginx (HTTP → HTTPS, www → non-www).
- Canonical URLs in the base template (absolute URL, no query string) with per-page overrides available.
- Unique page titles and meta descriptions for key public pages, including `/press/` resources.
- `sitemap.xml` served via Django sitemap framework with core public pages and lastmod timestamps.
- Canonical URLs in the base template (absolute URL, no query string) with per-page overrides available.
- Unique page titles and meta descriptions for key public pages.
- Site-wide Organization and WebSite JSON-LD; NewsArticle JSON-LD for the news listing.
- Django tests covering robots, sitemap, canonical tags, and JSON-LD presence.

## Google Search Console (manual steps for Joao)
1. Verify the domain property for `sustainacore.org` in Google Search Console.
2. Submit the sitemap: `https://sustainacore.org/sitemap.xml`.
3. Use URL Inspection on key pages (`/`, `/tech100/`, `/news/`) and request indexing.

## Local validation
1. Run checks/tests:
   - `scripts/vm2_manage.sh check`
   - `scripts/vm2_manage.sh test core.tests.test_seo`
2. Start the server and verify endpoints:
   - Use the VM2 service stack (Nginx/Gunicorn) for validation instead of ad-hoc `runserver`.
   - `curl -k -I https://127.0.0.1/sitemap.xml -H "Host: sustainacore.org"` (TLS cert is for sustainacore.org, so `-k` skips the host mismatch)
   - `curl -I http://127.0.0.1/sitemap.xml` (if HTTP is enabled)
   - `curl -k -I https://127.0.0.1/robots.txt -H "Host: sustainacore.org"`
3. Verify redirects in production:
   - `curl -I http://sustainacore.org/`
   - `curl -I https://www.sustainacore.org/`
   - `curl -I http://www.sustainacore.org/`
