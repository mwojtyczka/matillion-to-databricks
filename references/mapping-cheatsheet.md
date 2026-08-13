# Matillion → Databricks mapping cheatsheet

## Characterize the source (two independent axes)

The two decisions and the target bundle are identical regardless; these only change how you *read* and *translate* the source. They're **independent** — don't infer one from the other.

**Axis 1 — export format (how you *parse*):**

| Format | Looks like | How to read it |
|---|---|---|
| **DPC / YAML** (newer) | one file per pipeline: `*.orch.yaml` / `*.tran.yaml`, `type:` strings, `transitions` / `sources` | the default assumed throughout these references |
| **Classic / JSON** (older) | one `.json` for the whole project: `orchestrationJobs` / `transformationJobs`, numeric `implementationID`, `connectors` lists | decode via `classic-json-format.md` |

**Axis 2 — source warehouse backend (which SQL dialect you *translate*):**

| Backend | Detect | SQL work |
|---|---|---|
| **Databricks** | `catalog`/`schema`, Spark SQL idioms | already Databricks dialect — little to do |
| **Snowflake / Redshift / BigQuery / …** | classic JSON `dbEnvironment`; else connection config + SQL idioms | translate to Databricks SQL — `snowflake-sql.md` (Snowflake worked; same approach for others) |

## The two decisions

1. **Shell — always a Job.** The orchestration pipeline's control flow becomes the Job's task graph. Not a judgment call.
2. **Task type per step — the ladder** (default is *not* Lakeflow):
   1. Pure SQL, full-refresh → **SQL task**
   2. Imperative / Python / mixed, or a debuggable migration landing → **notebook task**
   3. Incremental/streaming + lineage → **Lakeflow pipeline** (escape hatch)

Data quality is **off the ladder** — `Assert`/reject logic → a **DQX notebook** quality-gate task (DQX is a PySpark library; needs Python, not a SQL task) placed after the checked table. It checks a table from any task type, so it never dictates the transform's task type or forces Lakeflow. See `data-quality.md`.

Anything that branches (`success`/`failure`, `If`), loops (iterators), nests (`run-orchestration`), or has side effects (DDL, API, `python-script`) **must** be a Job task — Lakeflow can't express it. **Keep one task per Matillion step** — choose the task type, don't collapse the graph. Full rationale in `SKILL.md` → "The two decisions of every migration".

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
| webhook / API step (Teams/Slack notify) | notebook task (`requests.post`, URL from a secret) | `orchestration/webhook.md` |
| `end-failure`, `and`/`or` gate | *(usually no task)* — Job failure state / default gating; see failure-counting pattern below | `orchestration/start-end.md` |

## Common orchestration patterns

### Failure-counting → notification (`v_failures`)

A near-ubiquitous Matillion idiom: every step routes its **failure** connector to a
"query-to-scalar" step that increments a `v_failures` variable, all paths converge (via
an `and`/`or` gate), then an `If v_failures = 0` branches to a success vs. failure
notification. **Don't port the counter machinery** — Databricks task state already tracks
success/failure. Translate the whole pattern to:

| Matillion | Databricks |
|---|---|
| per-step `failure` connector → increment `v_failures` | **eliminated** — no failure handler tasks, no counter (Job task state already tracks success/failure) |
| `unconditional` connectors chaining steps regardless of failure | downstream task uses **`run_if: ALL_DONE`**, so it runs even if an upstream dependency failed |
| the `and`/`or` gate that waits for all paths | **collapses** — the notification task just `depends_on` all the steps it should wait for |
| `If v_failures = 0` → success notification | success task with **`run_if: ALL_SUCCESS`** |
| `If v_failures > 0` → failure notification | failure task with **`run_if: AT_LEAST_ONE_FAILED`** |

The notifications themselves are usually webhook steps — see `orchestration/webhook.md`.

> **Use `run_if` on the task, not `depends_on … outcome:`.** `outcome:` is legal only when
> depending on an if/else *condition* task; on a normal dependency it fails at deploy
> (`Outcomes can only be specified for if/else condition dependencies`). See
> `references/dab-gotchas.md`.

## Data quality (cross-cutting)

| Matillion type | Databricks | Reference |
|---|---|---|
| `Assert*` / reject-filter | **DQX** notebook task (split valid/quarantine) | `data-quality.md` |

DQX is a PySpark library, so the quality gate is always a **notebook** task (never a SQL task), placed after the checked table. It checks a table produced by any task type, so it doesn't change the transform's own task type or force Lakeflow.

## Variables (all scopes)

| Matillion variable | Databricks | Reference |
|---|---|---|
| Project / Environment variable | DAB bundle variable `${var.x}` | `variables.md` |
| Job variable (scalar) | Job parameter `{{job.parameters.x}}` | `variables.md` |
| Grid variable | `for_each` input / UC lookup table | `variables.md` |
| `updateScalarVariables` (write-back) | task values (`dbutils.jobs.taskValues`) | `variables.md` |

## Secrets (never variables)

| Matillion secret source | Databricks | Reference |
|---|---|---|
| Connection/profile password, API token, storage key | Databricks **secret scope** | `secrets.md` |
| Cloud secret manager (AWS SM / Azure Key Vault / GCP SM) | secret scope (Key Vault-backed on Azure) | `secrets.md` |
| Referenced from a task/notebook | `{{secrets/scope/key}}` / `dbutils.secrets.get` / UC connection | `secrets.md` |

**Never** map a secret to a bundle variable or job parameter — those are plaintext.

## Hardcoded values — surface and classify

Don't carry any literal across blindly. Sweep every component param + inline SQL/Python and pick a target (confirm with the user). See `hardcoded-values.md`.

| Value | Target |
|---|---|
| Credential / token / key | **Databricks secret** |
| Per-environment config (catalog, schema, warehouse, host, path) | **bundle variable** `${var.x}` |
| Per-run input (date, mode, filter) | **job parameter** `{{job.parameters.x}}` |
| True constant (fixed rule, stable enum) | **leave inline** |

## Default choices

- Task type per transformation: **SQL task** (default) → **notebook** (imperative) → **Lakeflow** (incremental/streaming or multi-table lineage only). Python only when SQL can't express it. Data quality is separate — a **DQX notebook** task (Python) checking the output table (`data-quality.md`).
- **Consolidate the transformation chain**: a linear chain producing one output → **one query with CTEs**, not one dataset per component. Target is `CREATE OR REPLACE TABLE ... AS` (SQL task) or `CREATE OR REFRESH MATERIALIZED VIEW` (if Lakeflow). Full-overwrite (`rewrite-table-dl`) = full refresh; append-only incremental → streaming table (Lakeflow). Give a component its own dataset only if it's reused, branches, or needs its own quality gate. See `transformation/rewrite-table.md`.
- **Keep one task per Matillion step** — choose the task type, don't collapse the Job graph into a single task.
- Nested orchestration: `run_job_task` when the child is reused across parents; inline the child's tasks when it's called from only one place.
- **Emit a bundle `README.md`** documenting the migration — source summary, bundle layout, a **before/after** Mermaid DAG (so consolidation is visible), translations table, variables, secrets, synthetic-data summary, deploy commands, post-migration checklist, source list. See `SKILL.md` → Step 5b.
- **Emit a setup notebook** (`src/setup/00_generate_source_data.py`) **only when the source data must be fabricated** — the agent asks up front (Step 1): if the real source tables already exist in the target, skip it entirely and read them directly. When emitted it's a **manual pre-step**, kept out of the Job graph, that the user runs once to fabricate any missing source/input tables with synthetic data (`dbldatagen`) for a test run; `IF NOT EXISTS` guards mean it no-ops against real sources. See `SKILL.md` → Step 5c.
