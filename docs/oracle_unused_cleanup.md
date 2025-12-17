# Oracle unused object cleanup

## Workflow
1. VM1 audit produces the unused table signals and `rename_plan_full.csv` inputs.
2. Cloud runtime scan (full repo, including `website_django/**`) generates `repo_hits_runtime.csv` and the final rename artifacts under `tools/oracle_audit/final/<timestamp>/`.
3. VM1 executes the rename plan (tables and Oracle Text indexes) using the final lists provided by the runtime scan.

## Safety rules
- Do not rename `DR$...` Oracle Text tables directly; rename the Oracle Text indexes instead.
- Do not rename `VECTOR$...` internals directly; rename the owning vector index instead.
