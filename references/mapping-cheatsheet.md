# Matillion → Databricks mapping cheatsheet

## The two decisions

1. **Shell — always a Job.** The orchestration pipeline's control flow becomes the Job's task graph. Not a judgment call.
2. **Executor per task — the ladder** (default is *not* Lakeflow):
   1. Pure SQL, full-refresh → **SQL task**
   2. Imperative / Python / mixed, or a debuggable migration landing → **notebook task**
   3. Incremental/streaming or managed data-quality + lineage → **Lakeflow pipeline** (escape hatch)

Anything that branches (`success`/`failure`, `If`), loops (iterators), nests (`run-orchestration`), or has side effects (DDL, API, `python-script`) **must** be a Job task — Lakeflow can't express it. **Keep one task per Matillion step** — choose the executor, don't collapse the graph. Full rationale in `SKILL.md` → "The two decisions of every migration".

## Pipeline types

Source artifacts are **Matillion pipelines**; the target is always a **Databricks Job**, whose tasks run via SQL / notebook / (rarely) Lakeflow.

| Matillion pipeline | Databricks | Detail |
|---|---|---|
| `*.orch.yaml` (orchestration pipeline) | Databricks **Job** (the shell) | `transitions` → task deps |
| `*.tran.yaml` (transformation pipeline) | a **Job task** — SQL task (default), notebook, or Lakeflow pipeline (escape hatch) | `sources` → one consolidated query |

## Transformation components (dataflow)

These are the *pieces* of one consolidated query (CTEs / SELECT clauses), not separate datasets. The final target is `CREATE OR REPLACE TABLE ... AS` (SQL task) or `CREATE OR REFRESH MATERIALIZED VIEW` (only if Lakeflow).

| Matillion type | Databricks | Reference |
|---|---|---|
| `table-input` | source read (explicit projection), inlined | `transformation/table-input.md` |
| `join` | SQL `JOIN` (a CTE) | `transformation/join.md` |
| `aggregate` | `GROUP BY` (the final SELECT) | `transformation/aggregate.md` |
| `rewrite-table-dl` | `CREATE OR REPLACE TABLE` (or MV if Lakeflow) | `transformation/rewrite-table.md` |

## Orchestration components (control flow)

| Matillion type | Databricks | Reference |
|---|---|---|
| `start` / `end-success` | Job graph boundaries (no task) | `orchestration/start-end.md` |
| `sql-executor` | Job SQL task | `orchestration/sql-executor.md` |
| `run-transformation` | Job SQL task (default) / notebook / pipeline task | `orchestration/run-transformation.md` |
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

- Executor per transformation: **SQL task** (default) → **notebook** (imperative) → **Lakeflow** (incremental/streaming or managed DQ+lineage only). Python only when SQL can't express it.
- **Consolidate the transformation chain**: a linear chain producing one output → **one query with CTEs**, not one dataset per component. Target is `CREATE OR REPLACE TABLE ... AS` (SQL task) or `CREATE OR REFRESH MATERIALIZED VIEW` (if Lakeflow). Full-overwrite (`rewrite-table-dl`) = full refresh; append-only incremental → streaming table (Lakeflow). Give a component its own dataset only if it's reused, branches, or needs expectations. See `transformation/rewrite-table.md`.
- **Keep one task per Matillion step** — choose the executor, don't collapse the Job graph into a single task.
- Nested orchestration: `run_job_task` when the child is reused across parents; inline the child's tasks when it's called from only one place.
