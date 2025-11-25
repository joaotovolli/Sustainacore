# SustainaCore Django (VM2) – Security Review

## Findings
- **SECRET_KEY hard-coded in `core/settings.py`** – High risk. The key was committed to the repository, enabling session forgery if reused in production.
- **DEBUG defaulted to `True`** – Medium risk. Production deployments could expose detailed error pages and sensitive context.
- **ALLOWED_HOSTS fixed list in code** – Low risk. Hostnames were hard-coded rather than configurable, reducing deploy flexibility.
- **SQLite database tracked in Git (`website_django/db.sqlite3`)** – High risk. Repository contained application data and schema; could include credentials or PII from admin usage.
- **Virtual environment tracked (`website_django/venv/` in repo history)** – Low risk. Increases risk of committing machine-specific artifacts and potential secrets from local tooling.

No additional API keys or external service credentials were found within `website_django/` during this review.

## Open questions / assumptions
- Production currently relies on SQLite; no managed database credentials were found.
- No analytics or email provider keys are present; assuming these are not configured on VM2.

## Recommended manual actions
- Rotate the Django `SECRET_KEY` in production and set `DJANGO_SECRET_KEY` on VM2.
- Optionally, use `git filter-repo` or similar to scrub historic commits containing the old `SECRET_KEY`, then force-push.

## Summary of changes in this revision
- Moved Django `SECRET_KEY`, `DEBUG`, and `ALLOWED_HOSTS` to environment variables with safe defaults and explicit failure when missing.
- Added `.gitignore` to keep SQLite databases, virtual environments, and static build output out of Git; removed the tracked `db.sqlite3` file.
- Rebuilt the homepage with new templates, navigation, and supporting pages (Lab, Methodology, Privacy) using lightweight static assets.
