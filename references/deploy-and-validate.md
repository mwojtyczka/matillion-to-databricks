# Deploy and validate

## Deploy — depends on whether *you* (the agent) can run the CLI

Deploying a bundle runs the **Databricks CLI** (`databricks bundle deploy`). Who runs it depends on where the agent is running:

**If you can run a shell / the CLI (e.g. Claude Code):** deploy directly. Delegate to the
deployment skill rather than hand-rolling commands:

> "use the databricks-resource-deployment skill to deploy this bundle"

That skill handles Lakeflow pipelines + Jobs, prefers serverless compute, uses `databricks sync`, and enforces UC 3-layer namespaces.

**If you are running inside the workspace and CANNOT run the CLI (e.g. Databricks
Genie / Assistant):** you cannot deploy. **Generate the bundle, then explicitly ask the
user to run the deploy themselves** — never claim you deployed it. Give them the exact
commands and where the bundle is.

**The committed `databricks.yml` keeps placeholders** (empty `warehouse_id`, placeholder
host) so no real environment values are baked in — which means a bare `databricks bundle
deploy` **fails** (an empty `warehouse_id` on a SQL task errors with a cryptic
`is not a valid endpoint id`). So the deploy command you hand the user **must pass the
real values via `--var`** (using the values they gave you in Step 5), and set the host.
Emit a ready-to-run command, filled in with their answers — don't leave `<...>` for the
things you already know:

> I've written the bundle to `<workspace path>`. I can't run the Databricks CLI from
> here, so please deploy it yourself from a machine that has the CLI. Pass the config
> values with `--var` (the committed bundle intentionally ships with placeholders):
> ```bash
> databricks workspace export-dir "<workspace path>" ./migrated-bundle
> cd ./migrated-bundle
> databricks bundle deploy -t dev --profile <profile> \
>   --var="catalog=<catalog>" \
>   --var="schema=<schema>" \
>   --var="warehouse_id=<warehouse_id>"
> databricks bundle run <job_name> -t dev --profile <profile> \
>   --var="catalog=<catalog>" --var="schema=<schema>" --var="warehouse_id=<warehouse_id>"
> ```
> (The `dev` target's `workspace.host` is a placeholder — set it to your workspace URL in
> `databricks.yml`, or ensure your `--profile` points there.) Tell me once it's deployed
> and I'll run the validation checks.

Fill every `--var` with the value the user confirmed in Step 5. `warehouse_id` has no
default and **must** be supplied — omitting it is the most common deploy failure. If they
didn't give one, ask (`databricks warehouses list` shows the IDs) before emitting the
command.

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
