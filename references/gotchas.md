# Matillion → Databricks migration gotchas

Read before translating any component. Grows as new issues surface.

## `[Environment Default]` catalog/schema placeholders

Matillion resolves `catalog: "[Environment Default]"` / `schema: "[Environment Default]"` from its environment config at runtime. Databricks has no equivalent — you must substitute a real UC 3-layer namespace (`catalog.schema.table`).

Watch for inconsistency: in the samples, `sales-by-category-region.tran.yaml` uses `[Environment Default]` while the `python-script` in `create-maia-demo-data.orch.yaml` hardcodes `marcin_demo.default`. Pick one target catalog/schema and apply it everywhere (ideally as a bundle variable).

More broadly, `[Environment Default]` is just one hardcoded value among many — surface **every** literal (catalog/schema, warehouse/host, paths, connection details, credentials, tuning constants) and classify each as a bundle variable, job parameter, secret, or leave-inline. See `references/hardcoded-values.md`.

## Seed data in `sql-executor` is not a transformation

`CREATE OR REPLACE TABLE ... INSERT INTO ... VALUES (...)` blocks are demo/fixture data. Keep them as a Job setup SQL task. Do **not** model them as Lakeflow pipeline tables — the pipeline should read them as sources, not own them.

## `python-script` uses Matillion-runtime APIs

`context`, `context.cursor()`, `subprocess`, `interpreter: "Python 3"`, `user: "Privileged"` exist only in Matillion. Extract the real payload (usually embedded SQL) and run it via `spark.sql(...)` or a SQL task. Discard the plumbing.

## Backticked identifiers & aliases carry over

Matillion `joinExpressions` predicates (e.g. `` `s`.`product_id` = `p`.`product_id` ``) are already valid Spark SQL. Copy verbatim. Preserve the `mainTableAlias` and per-join aliases — `columnMappings` depend on them.

## Preserve explicit column projections

`table-input.columnNames` and `join.columnMappings` are explicit whitelists. Do not replace with `SELECT *` — downstream steps and the final target schema depend on the exact columns and order.

## Rewrite ≠ append

`rewrite-table-dl` means full overwrite each run. Map to a materialized view (full refresh) or `CREATE OR REPLACE`, never `INSERT INTO` (which appends).

## Variables must migrate with the pipelines

A pipeline that reads a variable breaks if the variable has nowhere to resolve. Migrate variable declarations **before** the steps that read them. Map by scope/behavior, not name: project/env variables → bundle variables; scalar job variables → Job parameters; grid variables → `for_each` input or a UC lookup table; and **write-back** variables (`updateScalarVariables` in a step's `postProcessing`) → **task values**, not parameters (Databricks parameters are immutable within a run). Full detail: `references/variables.md`.

## Don't over-materialize the transformation chain

Mapping every Matillion transformation component to its own `CREATE OR REFRESH MATERIALIZED VIEW` is faithful but wasteful: each intermediate becomes a storage-backed table Lakeflow recomputes every refresh. For a linear chain producing a single output, **consolidate into one MV with CTEs** — same result, one object instead of N. Keep a separate dataset only when it's reused, branches, or needs its own data-quality expectations. See `references/transformation/rewrite-table.md`.

## Orphaned datasets after consolidation / renaming

Lakeflow does **not** drop a table when you remove or rename its defining query in the pipeline — the old dataset just stops updating and lingers in the schema. After consolidating a 1:1 mapping (or renaming any MV/streaming table), manually `DROP` the now-orphaned tables, or `SHOW TABLES` will keep showing stale intermediates that look like real outputs.

## Secrets are not variables

Credentials in the Matillion project (connection passwords, API tokens, storage keys, or values sourced from a cloud secret manager) migrate to **Databricks secret scopes**, referenced at runtime via `{{secrets/scope/key}}` / `dbutils.secrets.get` / a UC connection. **Never** map a secret to a bundle variable (`${var.x}`) or job parameter — those are plaintext and show up in the UI, `bundle summary`, and run logs. Never write a secret into a source file or the migration notes; if an export contains a plaintext credential, rotate it. Grant the run-as principal `READ` on the scope before the first run. See `references/secrets.md`.

## Nested orchestrations (`run-orchestration`)

An orchestration pipeline can call another orchestration pipeline (`run-orchestration`, the shared-job pattern) — distinct from `run-transformation`. It maps to a `run_job_task` (nested Databricks Job), not a pipeline task. Deeply nested chains may hit Databricks' nested-job depth limits; inline (flatten) when the child isn't genuinely reused across parents. See `references/orchestration/run-orchestration.md`.
