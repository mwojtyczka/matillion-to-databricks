# Matillion → Databricks mapping cheatsheet

## Pipeline types

Source artifacts are **Matillion pipelines**; targets are **Databricks Jobs** / **Lakeflow pipelines**.

| Matillion pipeline | Databricks | Detail |
|---|---|---|
| `*.orch.yaml` (orchestration pipeline) | Databricks **Job** | `transitions` → task deps |
| `*.tran.yaml` (transformation pipeline) | Lakeflow **Declarative Pipeline** | `sources` → dataset chain |

## Transformation components (dataflow)

| Matillion type | Databricks | Reference |
|---|---|---|
| `table-input` | source read (explicit projection) | `transformation/table-input.md` |
| `join` | SQL `JOIN` | `transformation/join.md` |
| `aggregate` | `GROUP BY` | `transformation/aggregate.md` |
| `rewrite-table-dl` | `CREATE OR REFRESH MATERIALIZED VIEW` | `transformation/rewrite-table.md` |

## Orchestration components (control flow)

| Matillion type | Databricks | Reference |
|---|---|---|
| `start` / `end-success` | Job graph boundaries (no task) | `orchestration/start-end.md` |
| `sql-executor` | Job SQL task | `orchestration/sql-executor.md` |
| `run-transformation` | Job pipeline task | `orchestration/run-transformation.md` |
| `run-orchestration` | Job `run_job_task` (nested Job) | `orchestration/run-orchestration.md` |
| `python-script` | Job notebook/SQL task | `orchestration/python-script.md` |

## Variables (all scopes)

| Matillion variable | Databricks | Reference |
|---|---|---|
| Project / Environment variable | DAB bundle variable `${var.x}` | `variables.md` |
| Job variable (scalar) | Job parameter `{{job.parameters.x}}` | `variables.md` |
| Grid variable | `for_each` input / UC lookup table | `variables.md` |
| `updateScalarVariables` (write-back) | task values (`dbutils.jobs.taskValues`) | `variables.md` |

## Default choices

- Transformation code: **SQL** (`CREATE OR REFRESH MATERIALIZED VIEW`); Python only when SQL can't express it.
- Full-overwrite (`rewrite-table-dl`) → materialized view. Append-only incremental → streaming table.
- Nested orchestration: `run_job_task` when the child is reused across parents; inline the child's tasks when it's called from only one place.
