# GitHub setup

State of drashtishah/r2 on GitHub. Update when settings change.

## Repo

- Public.
- Merge methods: merge commit, rebase. Squash disabled; git log is the project memory.
- `delete_branch_on_merge`: true.
- `allow_update_branch`: true.
- `allow_auto_merge`: true.

## Branch protection on `main`

- Required check: `test` (job in `.github/workflows/check-paths.yml`).
- Strict: PR must be up to date with `main`.
- `enforce_admins`: false.
- Force pushes and branch deletion: blocked.

## Workflows

- `.github/workflows/check-paths.yml`: runs `tests/test_paths.py` on every push and PR.
- `.github/workflows/auto-merge.yml`: on PR open, runs `gh pr merge --auto --merge` so the PR merges with a merge commit once `test` passes.

## Reproduce

```
gh repo edit drashtishah/r2 --visibility public --accept-visibility-change-consequences

gh api -X PATCH repos/drashtishah/r2 \
  -F allow_auto_merge=true \
  -F allow_squash_merge=false \
  -F allow_update_branch=true \
  -F delete_branch_on_merge=true

gh api -X PUT repos/drashtishah/r2/branches/main/protection --input - <<'JSON'
{
  "required_status_checks": {"strict": true, "contexts": ["test"]},
  "enforce_admins": false,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
JSON
```
