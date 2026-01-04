# Admin portal (VM2)

The admin portal is intentionally hidden and responds with HTTP 404 unless the
request comes from a logged-in session whose email matches `SC_ADMIN_EMAIL`.

## Portal URL

- `/_sc/admin/`

## Environment variables

- `SC_ADMIN_EMAIL` (defaults to `joaotovolli@hotmail.com`)
- Oracle DB credentials are sourced from the same env vars used by VM2 Django:
  `DB_USER/DB_PASSWORD/DB_DSN` (or `ORACLE_USER/ORACLE_PASSWORD/ORACLE_DSN`).

## Oracle tables

The portal relies on two Oracle tables:

- `PROC_GEMINI_JOBS`
- `PROC_GEMINI_APPROVALS`
 - `NEWS_ITEMS` (publish news posts)
 - `NEWS_TAGS` / `NEWS_ITEM_TAGS` (news tags)
 - `NEWS_ASSETS` (image uploads)

### Initialize tables

- SQL script: `website_django/sc_admin_portal/sql/init_proc_tables.sql`
- Management command:
  - `python website_django/manage.py init_proc_tables`

## Workflow

### Joao -> Gemini CLI

Submit jobs via the portal form. Each job inserts a row into `PROC_GEMINI_JOBS`
with status `PENDING` and any optional attachment stored as a BLOB.

### Manual research requests

Manual research requests are inserted into `PROC_RESEARCH_REQUESTS` with status
`PENDING` and an optional `SOURCE_APPROVAL_ID` when resubmitting approvals.

If VM1 applied the retry schema changes, the portal shows `retry_count` and
`next_retry_at` cooldowns. The “Retry now” button clears `next_retry_at` and
appends `admin_retry_now <timestamp>` to `RESULT_TEXT` so the manual timer can
pick it up.

### Gemini CLI -> Joao

Gemini/VM1 inserts rows into `PROC_GEMINI_APPROVALS` with status `PENDING`.
Joao reviews and approves or rejects them in the portal, which updates:

- `STATUS` to `APPROVED` or `REJECTED`
- `DECIDED_AT`, `DECIDED_BY`, and optional `DECISION_NOTES`

To attach files or add Gemini comments for approvals, populate:

- `FILE_NAME`, `FILE_MIME`, `FILE_BLOB` for attachments
- `GEMINI_COMMENTS` for additional notes shown in the approval detail view

## Publish news (rich body)

The “Publish news” tab creates curated stories with exactly three inputs:
tags, headline, and rich body HTML. The body is sanitized on save and stored in
`NEWS_ITEMS.BODY_HTML`. Uploaded images are stored in `NEWS_ASSETS` and referenced
as `/news/assets/<asset_id>/` URLs inside the HTML.

## Publish news smoke check

1) Start the local server:
   - `DJANGO_SECRET_KEY=test python website_django/manage.py runserver 127.0.0.1:8065 --noreload`
2) Visit `/_sc/admin/#publish-news` and publish a story containing:
   - Headline + tags
   - A paragraph, a small table, and one uploaded image
3) Verify listings and detail render:
   - `curl -s http://127.0.0.1:8065/news/ | rg "News & Insights"`
   - `curl -s http://127.0.0.1:8065/news/NEWS_ITEMS:<id>/ | rg "news-detail__rich"`

## Draft cleanup

The legacy SocialDraftPost rows can be removed with:

- `python website_django/manage.py purge_portal_drafts`
