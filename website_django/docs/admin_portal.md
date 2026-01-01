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

### Initialize tables

- SQL script: `website_django/sc_admin_portal/sql/init_proc_tables.sql`
- Management command:
  - `python website_django/manage.py init_proc_tables`

## Workflow

### Joao -> Gemini CLI

Submit jobs via the portal form. Each job inserts a row into `PROC_GEMINI_JOBS`
with status `PENDING` and any optional attachment stored as a BLOB.

### Gemini CLI -> Joao

Gemini/VM1 inserts rows into `PROC_GEMINI_APPROVALS` with status `PENDING`.
Joao reviews and approves or rejects them in the portal, which updates:

- `STATUS` to `APPROVED` or `REJECTED`
- `DECIDED_AT`, `DECIDED_BY`, and optional `DECISION_NOTES`

To attach files or add Gemini comments for approvals, populate:

- `FILE_NAME`, `FILE_MIME`, `FILE_BLOB` for attachments
- `GEMINI_COMMENTS` for additional notes shown in the approval detail view

## Draft cleanup

The legacy SocialDraftPost rows can be removed with:

- `python website_django/manage.py purge_portal_drafts`
