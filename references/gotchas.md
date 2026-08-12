# Matillion → Databricks migration gotchas

Read before translating any component. Grows as new issues surface.

## `[Environment Default]` catalog/schema placeholders

Matillion resolves `catalog: "[Environment Default]"` / `schema: "[Environment Default]"` from its environment config at runtime. Databricks has no equivalent — you must substitute a real UC namespace.

**Do not hardcode the catalog/schema — always parameterize it as a bundle variable.** The catalog and schema change between environments (dev/staging/prod), so a baked-in `main.matillion_demo` is an environment leak that forces code edits per deployment. Declare `catalog` / `schema` bundle variables once and reference them everywhere: SQL tasks via `sql_task.parameters` + `USE CATALOG IDENTIFIER(:catalog)`, notebooks via `dbutils.widgets`, Lakeflow via the pipeline's `catalog`/`schema` fields. Full pattern: `references/variables.md`.

Watch for inconsistency: in the samples, `sales-by-category-region.tran.yaml` uses `[Environment Default]` while the `python-script` in `matillion-migration-demo.orch.yaml` hardcodes `marcin_demo.default`. Map **both** to the one `catalog`/`schema` variable pair so they stay consistent.

More broadly, `[Environment Default]` is just one hardcoded value among many — surface **every** literal (catalog/schema, warehouse/host, paths, connection details, credentials, tuning constants) and classify each as a bundle variable, job parameter, secret, or leave-inline. See `references/hardcoded-values.md`.

## Bundle variables are NOT substituted inside SQL files

`${var.catalog}` is **bundle-config** syntax — the CLI resolves it in `databricks.yml`, *not* inside a `.sql` file a SQL task runs. Writing `${var.catalog}` (or `${catalog}`) in the SQL does **not** interpolate; it runs verbatim and fails or hits the wrong object. Don't "fix" this by hardcoding the namespace back into the SQL either.

Correct pattern — pass the values as **SQL task parameters** and read them with `:name` markers, setting the namespace once so tables stay unqualified:

```sql
-- top of the .sql file
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:schema);
CREATE OR REPLACE TABLE my_table AS SELECT ...;   -- unqualified
```
```yaml
# the SQL task in databricks.yml — bundle var -> task parameter -> :marker
sql_task:
  warehouse_id: ${var.warehouse_id}
  parameters:
    catalog: ${var.catalog}
    schema: ${var.schema}
  file: { path: ../src/setup/my_table.sql }
```

`IDENTIFIER()` is required — a bare `:catalog` is treated as a string/column value, not an object name. Notebook tasks do the equivalent with `dbutils.widgets` + `base_parameters`. Full detail: `references/variables.md` → "Parameterizing catalog/schema in a SQL task".

## `is not a valid endpoint id` on deploy = empty/invalid `warehouse_id`

`databricks bundle deploy` failing with `Error: cannot create job:  is not a valid endpoint id` (note the blank before "is") means a SQL task was created with an **empty or invalid `warehouse_id`**. The committed `databricks.yml` ships with `warehouse_id` as a placeholder (`""`) on purpose, so a bare `databricks bundle deploy` hits this. Supply a real SQL warehouse ID:

```bash
databricks bundle deploy -t dev --profile <profile> --var="warehouse_id=<id>"
# find IDs with:  databricks warehouses list --profile <profile>
```

(Or set the `warehouse_id` default in `databricks.yml`.) When an agent hands a user the deploy command, it must fill in `--var="warehouse_id=..."` (and the other config vars) from the user's answers — see `references/deploy-and-validate.md`.

## `SCHEMA_NOT_FOUND` at run time = target schema was never created

The bundle deploys fine, but the **first task fails at run time** with `[SCHEMA_NOT_FOUND] The schema \`<catalog>\`.\`<schema>\` cannot be found`. Matillion resolved `[Environment Default]` (or a Snowflake `database.schema`) against an environment that already had the schema; the migrated Job doesn't recreate it, so `USE SCHEMA IDENTIFIER(:schema)` fails on a fresh catalog. Have the **first setup task create the schema** before using it:

```sql
USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE SCHEMA IDENTIFIER(:schema);
```

The **catalog** must already exist — creating catalogs needs metastore-admin rights and is out of scope for an ETL Job, so surface a missing catalog to the user rather than trying to create it. (This is easy to miss because a *re-run* against a schema a previous run created looks clean — it only bites on a genuinely fresh namespace.)

## Reads assume upstream/source tables already exist

A transform that reads `FROM raw_orders` (or a Snowflake `RAW.*` table) will fail with `TABLE_OR_VIEW_NOT_FOUND` if nothing produces that table. Matillion pipelines often **read source tables populated by ingestion outside the pipeline** — the migrated Job inherits that assumption. Before claiming a Job runs end-to-end, check every `FROM`/`JOIN` resolves to either a table an upstream task creates or a real pre-existing source. If the source data genuinely lives elsewhere, point the reads at it (via `catalog`/`schema` params); only seed fixture tables for a self-contained demo, and label them as such. (Both worked examples add a first `seed`/dimension task for exactly this reason.)

## A pre-existing schema silently defeats the setup notebook's skip-guard

The setup notebook guards each fabricated source with `if not spark.catalog.tableExists(...)`
so it no-ops against real data. But on a **shared workspace** (e.g. a demo metastore) the
target `catalog.schema` may already exist with **differently-shaped** tables of the same name
(a colleague's demo, or your own earlier run of a *different* version). The guard then skips
creation, and the transforms run against the wrong tables — surfacing as a confusing
`UNRESOLVED_COLUMN`/`AMBIGUOUS_REFERENCE` far from the real cause (the "suggested columns" in
the error won't match the source you expected). **For a verification run, deploy to a
dedicated, uniquely-named schema** (`--var schema=<something_unique>`) rather than a generic
`main.<common_name>`; check `databricks schemas list <catalog>` first. Only trust the
skip-guard when you know the pre-existing tables are the *real* sources with the expected
schema.

## A view whose backing table is dropped fails at query time

If a transform's output is a **non-materialized `VIEW`** built over a scratch/interval table
(common when porting a Snowpark procedure that ends in `CREATE VIEW … AS <join over a
generated table>`), do **not** drop that backing table in cleanup — the view keeps a hard
dependency and querying it fails with `[UC_DEPENDENCY_DOES_NOT_EXIST]`. Either keep the
backing table (refresh it with `CREATE OR REPLACE` each run) or materialize the output as a
`CREATE OR REPLACE TABLE … AS` so it has no live dependency. Materialized outputs may drop
their staging freely; views may not.

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

Mapping every Matillion transformation component to its own `CREATE OR REFRESH MATERIALIZED VIEW` is faithful but wasteful: each intermediate becomes a storage-backed table Lakeflow recomputes every refresh. For a linear chain producing a single output, **consolidate into one MV with CTEs** — same result, one object instead of N. Keep a separate dataset only when it's reused, branches. See `references/transformation/rewrite-table.md`.

## Orphaned datasets after consolidation / renaming

Lakeflow does **not** drop a table when you remove or rename its defining query in the pipeline — the old dataset just stops updating and lingers in the schema. After consolidating a 1:1 mapping (or renaming any MV/streaming table), manually `DROP` the now-orphaned tables, or `SHOW TABLES` will keep showing stale intermediates that look like real outputs.

## Secrets are not variables

Credentials in the Matillion project (connection passwords, API tokens, storage keys, or values sourced from a cloud secret manager) migrate to **Databricks secret scopes**, referenced at runtime via `{{secrets/scope/key}}` / `dbutils.secrets.get` / a UC connection. **Never** map a secret to a bundle variable (`${var.x}`) or job parameter — those are plaintext and show up in the UI, `bundle summary`, and run logs. Never write a secret into a source file or the migration notes; if an export contains a plaintext credential, rotate it. Grant the run-as principal `READ` on the scope before the first run. See `references/secrets.md`.

## Data quality goes to DQX, not Lakeflow or a `WHERE`

`Assert` components and reject/filter logic are data-quality gates. They migrate to **DQX** (the Databricks data quality framework). DQX is a PySpark library, so it runs as a **notebook** task (Python) — or inside a Lakeflow pipeline — never a plain SQL task; but it checks a table produced by *any* task type, so it stays decoupled from the transform's own task type. Two mistakes to avoid: (1) don't reach for a **Lakeflow pipeline** "because there are expectations" — DQX doesn't need one, so pick the transform's task type on its own merits; (2) don't translate a reject-filter into a silent `WHERE` that drops rows — use an `error`-criticality DQX check that **quarantines** them, so failures are auditable. See `references/data-quality.md`. Ensure `databricks-labs-dqx` is installed for the DQX task (a `%pip install` at the top of the notebook, or a cluster/environment library).

## Classic JSON export: type is a number, graph is in `connectors`

The older single-file JSON export has **no `type:` string** and **no inline `transitions`**. A component's type is a numeric `implementationID` (identify it by its parameter-name signature — see `references/classic-json-format.md`), and the step graph lives in separate `successConnectors` / `unconditionalConnectors` / `failureConnectors` (and, for transformations, `connectors`) lists of `sourceID`→`targetID`. Don't look for `transitions`/`sources`/`type` in a classic export — you won't find them, and the graph is in the connector lists.

## Format and backend are independent — don't infer one from the other

Two separate facts about a source: its **export format** (YAML vs classic JSON — governs *parsing*) and its **warehouse backend** (Databricks / Snowflake / Redshift / … — governs *SQL dialect*). They don't imply each other: a classic JSON export can be Databricks-backed, and a Snowflake project can be exported as YAML. The classic-format JSON exports happen to be Snowflake, but that's a coincidence of those files, not a rule. Detect each axis on its own (format by file shape; backend by `dbEnvironment` in JSON, or connection/SQL idioms in YAML).

## Non-Databricks source SQL is not Databricks SQL

If the source backend isn't Databricks (e.g. `dbEnvironment: "snowflake"`), the SQL is that warehouse's dialect — do **not** carry it across verbatim. For Snowflake: three-part `db.SCHEMA.table` → UC `catalog.schema` (parameterized), `::` casts → `CAST`, `IFF` → `IF`, double-quoted UPPERCASE identifiers → backticks, and `GRANT … TO ROLE` is a UC-governance concern (drop from the ETL step), not inline SQL. `QUALIFY` does carry over (Databricks supports it). Full list, and the approach for other backends: `references/snowflake-sql.md`.

## "Snowflake" code can be Python, not just SQL

A `sql-executor` step whose script is `CREATE … LANGUAGE PYTHON` (a Snowpark stored procedure / Python UDF, with `import snowflake.snowpark`) is **Python running inside Snowflake**, not SQL. It doesn't become a SQL task — it becomes a **notebook task** (translate `session.sql(...)` → `spark.sql(...)`, drop the `snowflake-snowpark-python` package and the procedure DDL wrapper). Don't confuse it with Matillion's `python-script` component (Python on the Matillion *agent* — `references/orchestration/python-script.md`). Three kinds of code, three homes — see the table in `references/snowflake-sql.md`.

## Nested orchestrations (`run-orchestration`)

An orchestration pipeline can call another orchestration pipeline (`run-orchestration`, the shared-job pattern) — distinct from `run-transformation`. It maps to a `run_job_task` (nested Databricks Job), not a pipeline task. Deeply nested chains may hit Databricks' nested-job depth limits; inline (flatten) when the child isn't genuinely reused across parents. See `references/orchestration/run-orchestration.md`.
