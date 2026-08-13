These are the CI/CD deploy workflows (`deploy-dev.yml` / `deploy-test.yml` /
`deploy-prod.yml`), parked here on purpose - GitHub Actions only scans
`.github/workflows/`, so anything outside it never triggers.

Disabled 2026-08-13 because the project is still local-dev only: no
`DATABRICKS_HOST_*`/`DATABRICKS_TOKEN_*` secrets are configured in the repo,
and `staging`/`prod` targets in `databricks.yml` are still `REPLACE_WITH_*`
placeholders, so these were failing on every trigger.

To re-enable once the project is further along:
1. Fill in the `staging`/`prod` targets in `databricks.yml` (real workspace
   hosts, catalogs, warehouse ids).
2. Add the matching secrets in repo Settings > Secrets and variables >
   Actions (`DATABRICKS_HOST_DEV`/`DATABRICKS_TOKEN_DEV`, `..._STAGING`,
   `..._PROD`).
3. `git mv .github/workflows-disabled/deploy-*.yml .github/workflows/`
