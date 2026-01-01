# Admin portal (VM2)

The admin portal is intentionally hidden and responds with HTTP 404 unless the
request comes from a logged-in Django user whose email matches
`SC_ADMIN_EMAIL`.

## How to use

1. Set `SC_ADMIN_EMAIL` (defaults to `joaotovolli@hotmail.com`).
2. Create or log in with a Django user that has the matching email.
3. Visit `/_sc/admin/` to manage draft posts.

## Notes

- Draft posts are stored in the VM2 Django database via `SocialDraftPost`.
- Non-authorized access always returns HTTP 404 and is not linked anywhere.
