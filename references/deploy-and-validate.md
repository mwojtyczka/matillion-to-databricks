# Deploy and validate

## Deploy — depends on whether *you* (the agent) can run the CLI

Deploying a bundle runs the **Databricks CLI** (`databricks bundle deploy`) — there is no
SDK/REST equivalent. Who runs it depends on where the agent is running:

**If you can run a shell / the CLI (e.g. Claude Code):** deploy directly. Delegate to the
deployment skill rather than hand-rolling commands:

> "use the databricks-resource-deployment skill to deploy this bundle"

That skill handles Lakeflow pipelines + Jobs, prefers serverless compute, uses `databricks sync`, and enforces UC 3-layer namespaces.

**If you are running inside the workspace and CANNOT run the CLI (e.g. Databricks
Genie / Assistant):** you cannot deploy. **Generate the bundle, then explicitly ask the
user to run the deploy themselves** — never claim you deployed it. Give them the exact
commands and where the bundle is:

> I've written the bundle to `<workspace path>`. I can't run the Databricks CLI from
> here, so please deploy it yourself from a machine that has the CLI:
> ```bash
> databricks workspace export-dir "<workspace path>" ./migrated-bundle
> cd ./migrated-bundle
> # set workspace host + warehouse_id in databricks.yml first
> databricks bundle deploy -t dev
> databricks bundle run <job_name> -t dev
> ```
> Tell me once it's deployed and I'll run the validation checks.

> **No local CLI at all?** Bundles are CLI-only, so the fallback is to create the
> resources directly with the Databricks SDK from a **notebook** (`import databricks.sdk`
> → `w.jobs.create(...)` / `w.pipelines.create(...)`). Offer to also emit a `deploy.py`
> the user can run as a notebook. This gives up the bundle's state/diffing and target
> model, so prefer the CLI path when it's available.

## Validate — run the checklist (works in Genie too)

After deploy, run this checklist. In Claude Code, use the
`fe-databricks-tools:databricks-query` skill; in Genie, run the SQL in-chat (Genie can
execute SQL even though it can't run the CLI). If you couldn't deploy, run this only
after the user confirms the deploy succeeded.

- [ ] Every target table from each `rewrite-table-dl` and every `sql-executor`/`python-script` output exists.
  ```sql
  SHOW TABLES IN my_catalog.my_schema;
  ```
- [ ] Target tables have a sane row count (not zero, not wildly off from source).
  ```sql
  SELECT COUNT(*) FROM my_catalog.my_schema.maia_sample_sales_summary;
  ```
- [ ] Spot-check one aggregate against the source. For the sample, total revenue must match between source and summary:
  ```sql
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales;          -- source
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales_summary;  -- must equal
  ```
- [ ] The Job ran green end-to-end (all tasks succeeded in the run history).

## Gotcha

Grant the pipeline/job's principal UC access (`USE CATALOG`, `USE SCHEMA`, `SELECT`/`MODIFY`) before the first run, or tasks fail with permission errors. The databricks-resource-deployment skill covers the grant pattern.

If the migration uses secrets (`references/secrets.md`), also grant the run-as principal `READ` on the secret scope(s), or any task that calls `dbutils.secrets.get` / `{{secrets/...}}` fails at runtime:

```bash
databricks secrets put-acl matillion_migration <run-as-principal> READ
```
