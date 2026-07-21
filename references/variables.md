# Matillion variables → Databricks parameters & values

Matillion pipelines are parameterized with **variables**. They must be migrated alongside the pipelines — an orchestration or transformation that reads a variable breaks if the variable has nowhere to resolve. Map each variable by its **scope and behavior**, not just its name.

## Where variables appear in the pipeline YAML

Watch for these keys (all present, though empty, in the sample orchestration):

| Key | Meaning |
|---|---|
| `declareSqlVariables` / `variablesToInclude` | which variables a `sql-executor` exposes to its SQL as bind values |
| `setScalarVariables` / `setGridVariables` | values passed **into** a called pipeline (`run-transformation` / `run-orchestration`) |
| `updateScalarVariables` / `updateGridVariables` | values written **back** by a step's `postProcessing` (mid-flow mutation) |
| `${my_var}` | a variable substitution inside SQL / parameters (none appear in the sample, but common in real projects) |

## Mapping by variable type

| Matillion variable | Databricks equivalent | Notes |
|---|---|---|
| **Project / Environment variable** (static config: catalog, warehouse, paths) | **DAB bundle variable** — `${var.x}` in `databricks.yml` | Set once per environment/target. Also resolves `[Environment Default]` (see `gotchas.md`). |
| **Job variable, scalar** (per-run input) | **Job parameter** — referenced as `{{job.parameters.x}}` in tasks | The default value becomes the parameter default. |
| **Grid variable** (a small table of rows/columns) | task-values array passed between tasks, a `for_each` task input, or a small lookup table in UC | No 1:1 primitive — pick by use: iteration → `for_each`; reference data → UC table. |
| **Automatic / system variable** (run id, timestamp, job name) | Databricks task-context — `{{job.id}}`, `{{run_id}}`, `{{job.start_time.iso_date}}` | Map to the nearest built-in; don't recreate manually. |

## Write-back: `updateScalarVariables` (the tricky one)

Matillion lets a step **mutate a variable mid-run** (e.g. capture a row count, then branch on it). Databricks parameters are **immutable within a run**, so this does not map to a parameter. Use **task values** to pass a computed value from one task to a downstream task:

```python
# producing task (e.g. the step that had updateScalarVariables)
row_count = spark.table("my_catalog.my_schema.staging").count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
```
```python
# consuming downstream task
rc = dbutils.jobs.taskValues.get(taskKey="producing_task", key="row_count")
```

For SQL-only flows, write the value to a small state table instead.

## Substitution syntax cheatsheet

| Context | Matillion | Databricks |
|---|---|---|
| Static/env config | `${env_var}` | `${var.env_var}` (bundle) |
| Per-run input in a task | `${job_var}` | `{{job.parameters.job_var}}` |
| **In a SQL-task `.sql` file** | `${var}` | **`:name` parameter marker**, bound by the task's `parameters:` map |
| In pipeline (Lakeflow) SQL | `${var}` | pipeline configuration → `${var}` in SQL, or a spark conf |
| Computed mid-run | `updateScalarVariables` | `dbutils.jobs.taskValues` |
| In a notebook task | `${var}` | `dbutils.widgets.get("name")` (task `base_parameters`) |

## Parameterizing catalog/schema in a SQL task (don't hardcode the namespace)

A SQL-task `.sql` file can't use `${var.x}` (that's bundle-config syntax, resolved before
the file runs). Instead, pass values as **SQL task parameters** and reference them with
`:name` markers. For the target namespace, set it once at the top with
`USE ... IDENTIFIER()` and then reference every table **unqualified** — so the
catalog/schema live in the bundle variables, never baked into the SQL:

```sql
-- top of the .sql file
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:schema);

CREATE OR REPLACE TABLE my_table AS SELECT ...;   -- unqualified; lands in :catalog.:schema
```

```yaml
# the SQL task in databricks.yml — bind the markers to bundle variables
- task_key: build_table
  sql_task:
    warehouse_id: ${var.warehouse_id}
    parameters:
      catalog: ${var.catalog}
      schema: ${var.schema}
    file:
      path: ../src/setup/build_table.sql
```

`IDENTIFIER()` is required — a bare `:catalog` is treated as a string/column value, not an
object name. The reference bundle (`examples/demo/databricks/`) uses exactly this for all
three SQL tasks. (Notebook tasks do the equivalent with `dbutils.widgets` + `base_parameters`.)

## Gotchas

- **Migrate variable declarations first**, before the steps that read them — a dangling `${x}` fails at parse/run time.
- **Grid variables have no direct primitive.** Decide per use (iteration vs. lookup data) rather than forcing a scalar mapping.
- **Write-back variables are not parameters.** If you see `updateScalarVariables`, reach for task values — do not model it as a Job parameter.
- **Scope collisions:** a variable defined at both project and job scope in Matillion resolves to the narrower scope. Preserve that precedence when splitting into bundle vars vs. job parameters.
