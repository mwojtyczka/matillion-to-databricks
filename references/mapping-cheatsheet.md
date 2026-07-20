# Matillion → Databricks mapping cheatsheet

## Job vs. Lakeflow pipeline — the core decision

**Ask: is this deciding what-runs-when (→ Job) or declaring how-data-flows (→ Lakeflow pipeline)?**

- **Databricks Job** = control flow: ordering, conditions, branching, loops, retries, schedules, side effects. The only place control flow can live.
- **Lakeflow pipeline** = declarative dataflow: table→table transforms with auto dependency/incremental/quality/lineage. **No** conditionals, loops, failure-branching, or imperative sequencing.
- **They compose**: the Job is the outer shell; each transformation pipeline runs inside it as a pipeline task.

Anything that branches (`success`/`failure`, `If`), loops (iterators), nests (`run-orchestration`), or has side effects (DDL, API, `python-script`) **must** be a Job task — Lakeflow can't express it. Full rationale in `SKILL.md` → "When to use a Databricks Job vs. a Lakeflow pipeline".

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
