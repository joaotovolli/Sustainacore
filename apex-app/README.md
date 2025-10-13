# Sustainacore APEX App

- App ID (DEV): 101
- Workspace (DEV): <WORKSPACE_NAME>
- Parsing schema: <SCHEMA_NAME>
- Policy: Git is authoritative; all changes via PR; no direct Builder edits. Nightly drift detection reconciles any emergency Builder edits back to Git.
- Note: Full APEX export will be provided as a GitHub Release asset, downloaded by CI at import time.
