# Deploy and validate

## Deploy — depends on whether *you* (the agent) can run the CLI

Deploying a bundle runs the **Databricks CLI** (`databricks bundle deploy`). Who runs it depends on where the agent is running:

**If you can run a shell / the CLI (e.g. Claude Code):** deploy directly with the
Databricks CLI — `databricks bundle deploy -t dev` (passing the `--var`s below), then
`databricks bundle run <job> -t dev`. The bundle already targets serverless compute and
UC 3-layer namespaces (that's how the skill emits it — see `references/dab-gotchas.md`);
no wrapper is needed.

**If you are Genie in-workspace:** you have a **job-scoped** CLI (`runDatabricksCli` —
trigger/list runs, inspect run output), but **`bundle deploy`/`validate`/`run`/`destroy`
are outside its allow-list, so you cannot deploy.** Generate the bundle, run the setup
notebook, and **ask the user to run the *initial* deploy themselves** (exact command +
bundle location below) — never claim you deployed it. **After** that first deploy exists,
you *can* iterate on the SQL/notebook layer: trigger the Job via the job-scoped CLI, read
each failed task's output, fix transform-content bugs in the deployed source files in
place, re-run the setup notebook if a source table changed, and re-trigger — until the
transforms are green. Only a change to `resources/*.yml` (task graph, `run_if`, paths,
serverless, params) needs another user-run `bundle deploy`, since bundle commands stay
outside your allow-list.

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
> # 1. Deploy
> databricks bundle deploy -t dev --profile <profile> \
>   --var="catalog=<catalog>" \
>   --var="schema=<schema>" \
>   --var="warehouse_id=<warehouse_id>"
> # 2. ONLY IF a synthetic setup notebook was emitted (the "fabricate" branch of Step 1) —
> #    create the synthetic source tables by running it once (it is NOT a Job task); skip
> #    if the real source tables already exist. Omit this line entirely when the migration
> #    targets real, existing tables (no notebook was emitted).
> #    Open src/setup/00_generate_source_data.py, set the catalog/schema widgets, Run All.
> # 3. Run the Job
> databricks bundle run <job_name> -t dev --profile <profile> \
>   --var="catalog=<catalog>" --var="schema=<schema>" --var="warehouse_id=<warehouse_id>"
> ```
> (The `dev` target's `workspace.host` is a placeholder — set it to your workspace URL in
> `databricks.yml`, or ensure your `--profile` points there.) Tell me once it's deployed
> and I'll run the validation checks.

**If a synthetic setup notebook was emitted, it must be run once between deploy and run**
(step 2 above) unless the real source tables already exist — it populates the source/input
tables the Job reads (`src/setup/00_generate_source_data.py`, a manual step, not a Job task).
Skipping it makes the first `bundle run` fail at the first read with `TABLE_OR_VIEW_NOT_FOUND`.
**When the migration targets real, existing tables no notebook is emitted** — drop step 2 and
just confirm the tables are present. See `SKILL.md` → Step 5c/Step 6.

Fill every `--var` with the value the user confirmed in Step 5. `warehouse_id` has no
default and **must** be supplied — omitting it is the most common deploy failure. If they
didn't give one, ask (`databricks warehouses list` shows the IDs) before emitting the
command.

## Validate — run the checklist (works in Genie too)

After deploy, run this checklist. From a CLI, run the SQL with `databricks sql` or the
SQL statements API; in Genie, run it in-chat (Genie can execute SQL). If you couldn't
deploy, run this only after the user confirms the deploy succeeded.

- [ ] Every target table from each `rewrite-table-dl` and every `sql-executor`/`python-script` output exists.
  ```sql
  SHOW TABLES IN my_catalog.my_schema;
  ```
- [ ] Target tables have a sane row count (not zero, not wildly off from source).
  ```sql
  SELECT COUNT(*) FROM my_catalog.my_schema.sample_sales_summary;
  ```
- [ ] Spot-check one aggregate against the source. For the sample, total revenue must match between source and summary:
  ```sql
  SELECT SUM(revenue) FROM my_catalog.my_schema.sample_sales;          -- source
  SELECT SUM(revenue) FROM my_catalog.my_schema.sample_sales_summary;  -- must equal
  ```
- [ ] The Job ran green end-to-end (all tasks succeeded in the run history).
- [ ] **If the user provided expected output, reconcile against it** (`SKILL.md` → Step 6b).
  A green run proves execution, not correctness. Only compare *values* when the Job ran
  against **real** source data (or a user-supplied input+expected pair) — synthetic setup
  data (Step 5c) is random, so with it you can only check schema/shape.
  - **Golden table/file** — same schema, same row count, key-based value diff:
    ```sql
    -- rows in expected but not produced (and swap args for the reverse)
    SELECT * FROM my_catalog.my_schema.expected_summary
    EXCEPT
    SELECT * FROM my_catalog.my_schema.sample_sales_summary;   -- must return 0 rows
    ```
  - **Row-count + aggregate spec** — each `COUNT(*)` and named aggregate equals expected:
    ```sql
    SELECT COUNT(*) AS n, SUM(revenue) AS total_revenue
    FROM my_catalog.my_schema.sample_sales_summary;            -- compare to the spec
    ```
  A mismatch is a migration bug (usually a dialect-semantics difference — rounding, nulls,
  date boundaries, join multiplicity, dedupe); trace to the transform, fix, re-run, re-check.

## Gotcha

Grant the pipeline/job's principal UC access (`USE CATALOG`, `USE SCHEMA`, `SELECT`/`MODIFY`) before the first run, or tasks fail with permission errors (`GRANT USAGE ON CATALOG … ; GRANT SELECT/MODIFY ON SCHEMA … TO \`<principal>\``).

If the migration uses secrets (`references/secrets.md`), the scope and its keys must exist
**before** the first run (a task calling `dbutils.secrets.get` / `{{secrets/...}}` fails at
runtime otherwise), and the run-as principal needs `READ`. Emit these in the README's
deployment steps, one `put-secret` per credential surfaced in the hardcoded-value sweep:

```bash
# create the scope + populate each secret (e.g. a webhook URL, a source-DB password)
databricks secrets create-scope matillion_migration
databricks secrets put-secret matillion_migration webhook_url        # prompts for the value
databricks secrets put-secret matillion_migration snowflake_password

# grant the job's run-as principal READ on the scope
databricks secrets put-acl matillion_migration <run-as-principal> READ
```

Never bake the secret values into these commands or the README — `put-secret` prompts for
the value (or reads a file); the committed docs show only key names.
