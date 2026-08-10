---
name: matillion-to-databricks
description: Guide for migrating Matillion ETL pipelines to Databricks. Trigger when the user wants to migrate Matillion pipelines to Databricks, from either export format (newer per-pipeline YAML *.orch.yaml / *.tran.yaml, or the older single-file JSON with all jobs in one .json) and from any source warehouse backend (Databricks, Snowflake, Redshift, BigQuery, …). Matillion orchestration jobs become Databricks Jobs; transformation jobs become Job tasks (SQL task for pure SQL, notebook otherwise), with Lakeflow Declarative Pipelines reserved for incremental/streaming needs. Non-Databricks source SQL (e.g. Snowflake) is translated to Databricks SQL. Consult the relevant component reference before translating each component.
---

# Matillion → Databricks Migration Guide

An end-to-end workflow for migrating Matillion ETL pipelines to Databricks.

**Terminology (keep the sides unambiguous):** the source artifacts are **Matillion pipelines** — matching the Data Productivity Cloud format (note the top-level `pipeline:` key in each file). The Databricks targets are a **Databricks Job** (always) and its **tasks** (SQL task, notebook task, and — only when justified — a Lakeflow pipeline task). Always qualify which side you mean: "Matillion orchestration pipeline" → "Databricks Job"; "Matillion transformation pipeline" → "a Job task" (usually SQL or notebook).

Matillion projects are made of two kinds of job:

- **Orchestration** — a control-flow DAG of steps. Becomes a **Databricks Job** — the outer shell that holds the whole migration.
- **Transformation** — a dataflow DAG of components. Becomes a **task inside that Job** — a SQL task for pure SQL, a notebook task otherwise, or a Lakeflow pipeline only when incremental/streaming features are actually needed.

**First, characterize the source along two *independent* axes.** The two decisions and the target bundle are the same regardless; these only change how you *read and translate* the source. Don't conflate them — a Snowflake project can be exported as YAML, and a JSON export can be Databricks-backed.

1. **Export format — how you *parse* the source:**
   - **DPC / YAML (newer):** one file per pipeline — `*.orch.yaml` (steps connected by `transitions`) and `*.tran.yaml` (components connected by `sources`), each with a top-level `pipeline:` key and string `type:` fields. This is the default the per-component references assume.
   - **Classic / JSON (older):** a **single `.json` file for the whole project**, bundling every job (`orchestrationJobs` / `transformationJobs`), components keyed by numeric `implementationID`, and the step graph in separate `connectors` lists. Decode via `references/classic-json-format.md`; the same per-component references then apply.
2. **Source warehouse backend — which SQL dialect you *translate*:** Matillion runs on top of a warehouse, and the SQL in the project is that warehouse's dialect. If the backend is **Databricks**, the SQL is already Spark SQL — little to do. If it's **Snowflake, Redshift, BigQuery, …**, translate the SQL to Databricks dialect — see `references/snowflake-sql.md` (Snowflake worked in full; same approach for others). In a classic JSON export the backend is the `dbEnvironment` field; for YAML, infer it from the connection config and SQL idioms.

**Terminology (keep the sides unambiguous):** the source artifacts are **Matillion jobs** (orchestration / transformation). The Databricks targets are a **Databricks Job** (always) and its **tasks** (SQL task, notebook task, and — only when justified — a Lakeflow pipeline task). Always qualify which side you mean: "Matillion orchestration" → "Databricks Job"; "Matillion transformation" → "a Job task" (usually SQL or notebook).

Consult the component reference (below) **before** translating each component, not after something breaks.

---

## The two decisions of every migration

Migrating a Matillion project is two nested decisions, in order:

1. **The shell — always a Databricks Job.** The orchestration pipeline's control flow (ordering, branching, retries, schedules, parameters) becomes the Job's task graph. This is not a judgment call: control flow can only live in a Job.
2. **The task type — how each step/transformation runs *inside* the Job** (SQL task, notebook task, or Lakeflow pipeline task). Pick per task using the ladder below. This is where the real judgment is, and the default is **not** Lakeflow.

### Decision 1: orchestration → Job (the outer shell)

A **Databricks Job (Workflow)** is an *imperative task orchestrator*: it decides **what runs, in what order, and under what conditions**. Every Matillion `transitions` edge becomes a task dependency; branches/loops/nesting become `run_if` / `for_each` / `run_job_task`. The Job is the outer shell that holds the entire migration — nothing below replaces it.

### Decision 2: pick the task type for each step (the ladder)

For each step (and each transformation pipeline), walk this ladder **top-down and stop at the first match**. The bias is toward the simplest, most debuggable, warehouse-native option — reserve Lakeflow for when its managed features actually earn their cost.

1. **Pure SQL, batch / full-refresh** → **SQL task.** The default for `sql-executor` and for any transformation that consolidates to one full-refresh query (`table-input` → `join` → `aggregate` → `rewrite-table-dl` with a single output). Cheapest, runs on the SQL warehouse, no cluster.
2. **Imperative logic, mixed SQL + Python, or you just want a debuggable migration landing** → **notebook task** (running `spark.sql(...)`). The default for `python-script` and for transformations too tangled for one clean query. Notebooks are the pragmatic migration workhorse: faithful to imperative sources, steppable cell-by-cell, and free of declarative constraints.
3. **Incremental / streaming / CDC, or multi-output auto-lineage** → consider a **Lakeflow Declarative Pipeline** (pipeline task). This is the *escape hatch*, not the default. Even here, a notebook running Structured Streaming is often simpler for a first migration — reach for Lakeflow specifically when you want it to *manage* checkpoints/state and lineage for you rather than hand-rolling them. See `references/orchestration/run-transformation.md` for the full Lakeflow-vs-task trade-off.

**Why not Lakeflow by default?** A pipeline is a separate resource with its own compute lifecycle and deploy surface. It only pays off when you use what it provides — incremental maintenance, streaming, multi-output lineage. A single full-refresh transform uses none of that, so it's just a SQL task wearing extra machinery. Match the tool to the features you actually need.

**Data quality is *not* on this ladder.** Quality enforcement (Matillion `Assert` components, reject/filter logic) migrates to **DQX** — the Databricks data quality framework. DQX is a PySpark library, so it needs **Python execution: a notebook task** (or inside a Lakeflow pipeline) — it can't run in a plain SQL task. But it can check a table produced by *any* task type, so it stays **decoupled from the transform's own task type**: pick that on its own merits (even a SQL task), then add a separate DQX **notebook** quality-gate task after it. So "this transform needs data-quality checks" is never a reason to promote the transform itself to Lakeflow. See `references/data-quality.md`.

### The capability boundary (what still *forces* a Job task)

Independently of the ladder, if a Matillion step needs any of the following it **must** be a Job task and can never be folded into a Lakeflow pipeline — pipelines can't express control flow:

| Matillion construct | Why it can't be a pipeline | Databricks home |
|---|---|---|
| Conditional transitions / `If` components | Pipelines have no branching | **Job** — task `depends_on` + `run_if` conditions |
| `success` / `failure` transitions | Pipelines don't do per-step failure routing | **Job** — `run_if: all_done` / failure-condition tasks |
| Iterators / loops (grid/loop iterators) | Pipelines don't loop | **Job** — `for_each` task |
| `run-orchestration` (nested pipelines) | Composition of control flow | **Job** — `run_job_task` |
| DDL, API calls, file ops, `python-script` side effects | Not dataflow | **Job** — SQL / notebook task |
| Scheduling, retries, alerts, parameters | Runtime orchestration concerns | **Job** — triggers, task retries, `job.parameters` |

### How it composes

The Job is the outer shell; each step is a task, task type chosen by the ladder:

```
Databricks Job (from the orchestration pipeline)
├─ task: seed/DDL            (sql-executor        → SQL task)
├─ task: run transformation  (run-transformation  → SQL task if pure SQL; else notebook; Lakeflow only if incremental/streaming)
├─ task: data-quality gate   (Assert / reject     → DQX notebook task, after the table it checks)
├─ task: nested orchestration(run-orchestration   → run_job_task)
└─ task: post-process        (python-script       → notebook task, run_if success)
```

**Preserve the task graph — don't collapse control flow.** Choosing task types is orthogonal to the graph's shape. It's tempting to fold everything into one big notebook, but that discards the per-task observability, granular retry/repair-run, and parallelism that *are* the orchestration. Keep one task per Matillion step; only choose *how* each runs.

When unsure about the shell-vs-task-type split, ask: *"Is this deciding what-runs-when (→ the Job's graph), or is it the work a single task does (→ pick a task type)?"*

---

## Step 1 — Inventory the Matillion project

First, **detect the export format** (see the intro) — it changes how you find and read the source, not the target:

```bash
find . -name '*.orch.yaml' -o -name '*.tran.yaml'   # DPC / YAML: one file per pipeline
find . -name '*.json'                               # classic: usually one file for the whole project
```

- **YAML format:** orchestration pipelines (`*.orch.yaml`) are the entry points.
- **Classic JSON format:** open the single `.json` and inventory `jobsTree` + `orchestrationJobs` / `transformationJobs`. Full decoding: `references/classic-json-format.md`.
- **Note the source backend** (independent of format): if the SQL isn't already Databricks dialect (in classic JSON, `dbEnvironment` ≠ `databricks`; in YAML, judged from the connection config / SQL idioms), plan for dialect translation — `references/snowflake-sql.md`.
- For each orchestration, note every `run-transformation` step (which transformation it names) and every `run-orchestration` step (which orchestration it names). This tells you which pipeline feeds which Job task and which orchestrations are nested inside others.
- Note every variable the pipelines declare, pass (`setScalarVariables`/`setGridVariables`), or read (`${...}`) — variables migrate alongside the pipelines. See `references/variables.md`.
- **Flag every secret/credential** — connection passwords, API tokens, storage keys, OAuth entries, or values sourced from a cloud secret manager (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager). These migrate to **Databricks secrets**, not to variables or code. See `references/secrets.md`.
- **Sweep for every other hardcoded value** — **catalog/schema names (always parameterize these as bundle variables — never hardcode the namespace)**, warehouse/host IDs, storage paths, connection details, tuning/business literals. Don't carry any literal across blindly: each one is a candidate for a bundle variable, a job parameter, a secret, or staying inline. See `references/hardcoded-values.md`.

Write down: the list of orchestration pipelines, what each one calls (transformations and nested orchestrations), the variables in play, the secrets in play (and their current source), every hardcoded value worth surfacing, and any transformation not called by anything (a standalone pipeline).

## Step 2 — Parse the orchestration graph

Walk the control-flow DAG from the `start` component to `end-success`. This DAG becomes the **Job's task graph** — each edge is a task dependency; failure edges become failure-condition task dependencies.

- **YAML format:** walk `transitions` (`unconditional` / `success` / `failure`) inside each `.orch.yaml` step.
- **Classic JSON format:** the graph lives in the job's separate `connectors` lists (`successConnectors` / `unconditionalConnectors` / `failureConnectors`, each a `sourceID`→`targetID`), and component *type* is a numeric `implementationID`, not a string. Decode both via `references/classic-json-format.md`.

See:

| Matillion orchestration type | Reference |
|---|---|
| `start` / `end-success` | `references/orchestration/start-end.md` |
| `sql-executor` | `references/orchestration/sql-executor.md` |
| `run-transformation` | `references/orchestration/run-transformation.md` |
| `run-orchestration` | `references/orchestration/run-orchestration.md` |
| `python-script` | `references/orchestration/python-script.md` |
| `Assert` / reject-filter (data quality) | `references/data-quality.md` |
| classic JSON export format (parsing) | `references/classic-json-format.md` |
| non-Databricks source SQL (Snowflake, etc.) | `references/snowflake-sql.md` |
| variables (all scopes) | `references/variables.md` |
| secrets / credentials | `references/secrets.md` |

## Step 3 — Parse each transformation graph

For each transformation, walk the dataflow DAG from `table-input` leaves to the final table-output. This describes one (or a few) output tables and how to compute them. In the **YAML format** the edges are `sources` refs inside each `.tran.yaml`; in the **classic JSON format** they are the job's single `connectors` list (`sourceID`→`targetID`) — see `references/classic-json-format.md`. If the source backend isn't Databricks (e.g. Snowflake), translate each component's SQL/expressions via `references/snowflake-sql.md` as you go.

**Consolidate first, then pick the task type.** A linear chain that yields a single output collapses into **one query** (CTEs for the intermediate `join`/`aggregate` components) — don't emit one dataset per component. Then apply the task-type ladder from "The two decisions" above:
- pure full-refresh SQL → **SQL task** (`CREATE OR REPLACE TABLE ... AS <one SELECT>`) — the common case;
- needs Python/imperative glue → **notebook task**;
- genuinely needs incremental/streaming → **Lakeflow pipeline** (then the consolidation rule about materialized views vs. CTEs applies — `references/transformation/rewrite-table.md`).

Keep a component as its own dataset only when it earns it: it's **reused**, needs independent **monitoring**, or is a genuine **branch point**.

**Watch for data-quality logic while parsing.** `Assert` components, and `filter`/`WHERE` steps that exist to reject bad rows, are quality gates — they migrate to **DQX** (a separate quality-gate task), not into the transform query. Note them here and translate them per `references/data-quality.md`; don't silently fold a reject-filter into the `SELECT`.

See:

| Matillion transformation type | Reference |
|---|---|
| `table-input` | `references/transformation/table-input.md` |
| `join` | `references/transformation/join.md` |
| `aggregate` | `references/transformation/aggregate.md` |
| `rewrite-table-dl` | `references/transformation/rewrite-table.md` |

Quick lookup for every type: `references/mapping-cheatsheet.md`.

## Step 4 — Map each component

For every component in every file, open its reference and translate it. Default to **SQL** (`CREATE OR REPLACE TABLE ... AS SELECT` for a SQL task, or `CREATE OR REFRESH MATERIALIZED VIEW` inside a Lakeflow pipeline); use **PySpark in a notebook** where SQL can't express it or the source is imperative. Choose the task type per the ladder in "The two decisions".

Before writing any code, read `references/gotchas.md` — it lists the mistakes that waste the most time (unresolved `[Environment Default]` placeholders, seed data mistaken for transforms, Matillion-runtime Python APIs). If the project uses any credentials, also read `references/secrets.md` — secrets go in Databricks secret scopes and are referenced at runtime, never inlined or turned into bundle variables. If it has `Assert` components or reject/filter logic (or the user wants quality gates), read `references/data-quality.md` — these become DQX quality-gate tasks; use the DQX skills (`dqx-define-checks`, `dqx-apply-checks`, `dqx-end-to-end`) for the check syntax rather than hand-writing SQL assertions.

**Surface every hardcoded value and let the user choose its target.** Don't silently carry a literal across. For each one, classify it and propose a target — **secret** (credentials), **bundle variable** (per-environment config), **job parameter** (per-run input), or **leave inline** (true constants) — explain why, and confirm before wiring. Present the findings as a table (redact secret values). Full triage: `references/hardcoded-values.md`.

## Step 5 — Assemble the Databricks Asset Bundle

**Ask the user how to name the Job** before emitting the bundle (and each additional Job, if there are nested orchestrations). Don't silently reuse the Matillion pipeline's internal name — propose a clean default derived from the `.orch.yaml` (e.g. `matillion-migration-demo.orch.yaml` → `matillion-migration-demo-job`) and let them confirm or override. This sets the job resource key, the `name:`, and how they'll find it in the Workflows UI, so it's worth a quick check rather than a guess.

**Ask the user for the deployment/config values, don't invent them.** Every bundle variable you introduced during the hardcoded-value sweep needs a real value per target. Present the list and ask — don't guess a default and leave it (a bad `warehouse_id`/host silently breaks the first run). At minimum, confirm:
- **`catalog` / `schema`** — the target UC namespace (resolves `[Environment Default]`).
- **`warehouse_id`** — the SQL warehouse for SQL tasks (there is no sensible default; an empty or wrong ID fails at deploy/run). Ask for the ID, or how to obtain it.
- **`workspace.host`** (per target: dev/prod) — leave as a placeholder in the committed file, but confirm the real host for the user's own deploy.
- **any other variable** from the sweep — paths, connection hosts/ports, per-run inputs. Secrets are handled separately (`references/secrets.md`), not as variable values.

Confirm these interactively (propose defaults where one is genuinely sensible, e.g. `catalog`); leave the committed `databricks.yml` with placeholders so no real environment values are baked in.

**Put resource definitions in a `resources/` folder and wire it in via `include:` — don't inline them in `databricks.yml`.** The top-level `databricks.yml` holds the bundle name, variables, and targets; each Job (and any pipeline) goes in its own file under `resources/`, pulled in with `include: [resources/*.yml]`. Source files (`.sql`, notebooks) live under `src/`. This is the idiomatic DAB layout and keeps `databricks.yml` readable as the project grows. Standard structure:

```
<bundle>/
├─ databricks.yml          # bundle name, variables, include:, targets
├─ resources/
│  ├─ <job>.job.yml        # one file per Job (the orchestration)
│  └─ <pipeline>.pipeline.yml   # only if a transformation needs Lakeflow
└─ src/
   ├─ setup/*.sql          # sql-executor + SQL-task transformations
   ├─ notebooks/*.py       # python-script / notebook tasks
   └─ dq/                  # only if the project has data-quality gates
      ├─ *.checks.yml      # DQX check definitions (metadata form)
      └─ dq_*.py           # DQX notebook tasks (apply checks, split valid/quarantine)
```
```yaml
# databricks.yml
include:
  - resources/*.yml
```

Emit a DAB with:
- one **job** resource per orchestration pipeline (`.orch.yaml`) **in its own `resources/*.yml` file**, named as agreed above, whose tasks mirror the orchestration graph: SQL tasks for `sql-executor`, a task per `run-transformation` (SQL task if the transformation is pure SQL — the common case; notebook if imperative; pipeline task only if it needs Lakeflow), a `run_job_task` for each `run-orchestration` (nested orchestration), and a notebook task for `python-script`,
- a **pipeline** resource (its own `resources/*.yml` file) **only** for transformations that actually need Lakeflow (incremental/streaming) — most migrations emit none,
- a **DQX notebook task** (with its `*.checks.yml`) for each data-quality gate — placed right after the task that produces the checked table, splitting valid rows from a quarantine table (see `references/data-quality.md`); emit none if the project has no `Assert`/reject logic and the user asks for no quality gates,
- **bundle variables / job parameters** for the Matillion variables (see `references/variables.md`), so per-environment config and per-run inputs are parameterized rather than hardcoded,
- **Databricks secret scopes** for every credential (see `references/secrets.md`) — referenced via `{{secrets/scope/key}}` / `dbutils.secrets.get` / a UC connection, never as a bundle variable or plaintext.

See the worked reference bundle in the repo at `examples/databricks-source/databricks/` — a `databricks.yml` + `resources/job.yml` + `src/` layout: an all-SQL-tasks-plus-one-notebook Job with no pipeline resource.

## Step 6 — Deploy and validate

**Deploying runs the Databricks CLI (`databricks bundle deploy`) — there is no SDK/REST equivalent. Who runs it depends on where you (the agent) are running:**

- **If you can run a shell/CLI (e.g. Claude Code):** deploy via the `fe-databricks-tools:databricks-resource-deployment` skill (it handles Jobs + Lakeflow pipelines, prefers serverless, uses `databricks sync`, and UC 3-layer namespaces). Trigger it with: "use the databricks-resource-deployment skill to deploy this bundle".
- **If you're inside the workspace and CANNOT run the CLI (e.g. Databricks Genie):** you cannot deploy. **Generate the bundle, then explicitly ask the user to run `databricks bundle deploy` themselves** — hand them the exact commands and the bundle's location, and never claim you deployed it. Because the committed `databricks.yml` ships with placeholders, the deploy command you emit **must pass the config values with `--var`** (`warehouse_id`, `catalog`, `schema`, …) filled in from the user's Step 5 answers — a bare `bundle deploy` fails on the empty `warehouse_id`. See `references/deploy-and-validate.md` for the ready-to-run template.

Then validate: in Claude Code use `fe-databricks-tools:databricks-query`; in Genie run the checklist SQL in-chat (Genie can run SQL). Follow the checklist in `references/deploy-and-validate.md`.
