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

## How to work — reference-driven, component-by-component, verified

**Do not translate from memory or in one big pass.** Every migration bug this skill guards
against has shipped *because* an agent skipped the reference and batch-translated. Work this
way, without shortcuts:

1. **One component at a time.** For each Matillion component: identify its type → open its
   reference (`references/…`) → translate it → immediately check that output against that
   reference's gotchas. Don't move on until it's right.
2. **Translate SQL against the dialect reference, not memory.** If the backend isn't
   Databricks, keep `references/snowflake-sql.md` open and check *each* function/idiom
   against it as you go — the dialect traps (`"quoted"` identifiers, `SELECT *,x AS existing`,
   `UPDATE…FROM`, `datediff(unit,…)`, sibling-alias refs, `UNION` alignment) are silent and
   pervasive.
3. **Externally-owned APIs: use the owning skill/docs, never memory.** DQX checks →
   `dqx-*` skills; dbldatagen (setup notebook) → `databricks-data-generation` skill +
   <https://databrickslabs.github.io/dbldatagen/>. Guessing option names is a top failure.
4. **Verify before handoff — this is a required gate, not optional.** Run the full
   **`references/verification-checklist.md`** against the generated bundle (and
   `databricks bundle validate` if you have the CLI). Fix every finding. A bundle that
   hasn't been checked against that list is not done.

The rest of this doc is the workflow (Steps 1–6) and the per-component references those
steps point into. Follow them in order; don't jump to emitting a bundle.

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
| webhook / API step (Teams/Slack notify) | `references/orchestration/webhook.md` |
| `Assert` / reject-filter (data quality) | `references/data-quality.md` |
| classic JSON export format (parsing) | `references/classic-json-format.md` |
| non-Databricks source SQL (Snowflake, etc.) | `references/snowflake-sql.md` |
| variables (all scopes) | `references/variables.md` |
| secrets / credentials | `references/secrets.md` |

## Step 3 — Parse each transformation graph

For each transformation, walk the dataflow DAG from the source (read) leaves to the sink (write) nodes. This describes one (or a few) output tables and how to compute them. In the **YAML format** the edges are `sources` refs inside each `.tran.yaml`; in the **classic JSON format** they are the job's single `connectors` list (`sourceID`→`targetID`) — and a component's **read-vs-write role is set by its DAG position, not its type** (a node with no incoming edges is a source; no outgoing edges, a sink), so derive roles from the graph — see `references/classic-json-format.md`. If the source backend isn't Databricks (e.g. Snowflake), translate each component's SQL/expressions via `references/snowflake-sql.md` as you go.

**Consolidate first, then pick the task type.** A **linear** chain collapses into **one query** (a CTE per intermediate `join`/`aggregate` component) — **regardless of length**; a 30-component linear chain is still one `CREATE OR REPLACE TABLE ... AS WITH … SELECT`, not one dataset per component. What breaks the single-query default is **DAG shape, not size** (details in `references/transformation/rewrite-table.md`): a **diamond/fan-out** (a component feeds two+ downstream paths that reconverge) or **multiple sinks** → a **notebook** with one `CREATE OR REPLACE TEMP VIEW` for each shared/branch node (computed once, reused), sinks writing tables. Then apply the task-type ladder from "The two decisions" above:
- pure full-refresh SQL → **SQL task** (`CREATE OR REPLACE TABLE ... AS <one SELECT>`) — the common case;
- needs Python/imperative glue → **notebook task**;
- genuinely needs incremental/streaming → **Lakeflow pipeline** (then the consolidation rule about materialized views vs. CTEs applies — `references/transformation/rewrite-table.md`).

Keep a component as its own dataset only when it earns it: it's **reused**, needs independent **monitoring**, or is a genuine **branch point**. Also watch for an **identical sub-graph repeated across several transformations** (e.g. a shared staff/org-structure lookup) — extract it once as a shared view/table or SQL file rather than duplicating the CTE logic in every output (`references/transformation/rewrite-table.md` → "Repeated sub-graphs").

**Watch for data-quality logic while parsing.** `Assert` components, and `filter`/`WHERE` steps that exist to reject bad rows, are quality gates — they migrate to **DQX** (a separate quality-gate task), not into the transform query. Note them here and translate them per `references/data-quality.md`; don't silently fold a reject-filter into the `SELECT`.

See:

| Matillion transformation type | Reference |
|---|---|
| `table-input` | `references/transformation/table-input.md` |
| `join` | `references/transformation/join.md` |
| `aggregate` | `references/transformation/aggregate.md` |
| `rewrite-table-dl` | `references/transformation/rewrite-table.md` |

Quick lookup for every type: `references/mapping-cheatsheet.md`.

## Step 3b — State the migration plan (checkpoint for non-trivial projects)

Once the graphs are parsed you've made the **coarse, hard-to-reverse decisions** — how
many Jobs, each transformation's task type, anything going to Lakeflow, how Jobs
decompose. These are cheap to change now and expensive to unwind *after* a full
conversion (especially under the autonomous run-to-green loop, where a wrong structural
call otherwise isn't seen until much churn has happened). So **state the plan before
emitting code** — but keep it proportionate:

- **Skip it for a trivial project** (a single orchestration with a handful of
  components) — the "two decisions" are obvious; just proceed.
- **State it for a non-trivial project** (**more than one orchestration, or roughly 8+
  components**). A few lines is enough — the *structural* calls only, not per-component
  detail (that's Step 4):
  - **Jobs** you'll create (one per orchestration; note any nested `run-orchestration`).
  - **Task type per transformation** (SQL task / notebook / Lakeflow) and — importantly —
    **which, if any, go to Lakeflow and why** (Lakeflow is the escape hatch, not the
    default; call out each one so a wrong reach for it is caught here).
  - **Consolidations** — linear chains collapsed to one query, and any shared sub-graph
    extracted once (Step 3).
  - **Namespace + secrets** — the target `catalog.schema`, and that secrets go to scopes.
- **How to present it depends on where you're running** (mirrors the execution layers in
  Step 5d): in an **interactive** session (Claude Code with a human, or Genie chat),
  surface the plan and pause briefly for correction. In **autonomous** mode, state it as a
  visible decision record and proceed without blocking — don't stall an unattended run.

This is a decision record, not a heavyweight gate: it exists to catch a wrong *structural*
choice early, not to re-confirm every component.

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
├─ README.md               # migration summary: translations + before/after DAG (see Step 5b)
├─ databricks.yml          # bundle name, variables, include:, targets
├─ resources/
│  ├─ <job>.job.yml        # one file per Job (the orchestration)
│  └─ <pipeline>.pipeline.yml   # only if a transformation needs Lakeflow
└─ src/
   ├─ setup/
   │  ├─ 00_generate_source_data.py  # setup notebook: synthetic source data, run MANUALLY before first run — not a Job task (Step 5c)
   │  └─ *.sql             # sql-executor + SQL-task transformations
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
- **Databricks secret scopes** for every credential (see `references/secrets.md`) — referenced via `{{secrets/scope/key}}` / `dbutils.secrets.get` / a UC connection, never as a bundle variable or plaintext,
- a **`README.md` at the bundle root** documenting the migration (see Step 5b),
- a **setup notebook** (`src/setup/00_generate_source_data.py`) the user runs **manually once** before the first run to fabricate any missing source/input tables with synthetic data for a test — kept out of the Job graph, guarded so it no-ops against real sources (see Step 5c).

**Emit the bundle correctly by construction — these are the mistakes that make a generated bundle fail to deploy (full list + why: `references/dab-gotchas.md`):**
- **Resource-file paths start with `../`.** In a `resources/*.yml`, `notebook_path` / `file.path` resolve relative to `resources/`, so they must climb out: `../src/notebooks/x.py`, not `src/notebooks/x.py` (the latter fails with `notebook … not found`).
- **Route success/failure with task-level `run_if`, never `depends_on … outcome:`.** `outcome:` is legal only when depending on an if/else *condition* task; on a normal task it fails at deploy. Use `run_if: ALL_SUCCESS` (success-only), `AT_LEAST_ONE_FAILED` (failure handler), `ALL_DONE` (run regardless) — this is how the failure-counting pattern maps (`references/mapping-cheatsheet.md`).
- **First task creates the schema** (`CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)`), and notebook source files use notebook-source format (`# Databricks notebook source` + `# MAGIC`).
- **Use serverless compute — don't emit classic clusters.** A notebook task with **no** `job_cluster_key` / `new_cluster` / `existing_cluster_id` runs on **serverless**; that's the default. Do **not** generate a `job_clusters:` block with a `new_cluster` (`spark_version`/`node_type_id`/`num_workers`) — serverless needs no cluster config, spins up faster, and is the recommended default. SQL tasks already run on the SQL warehouse (`warehouse_id`), not a cluster. Only reach for a configured cluster if a task has a specific need serverless can't meet, and flag it if so.

**Validate before handoff — if you can.** If you (the agent) can run a shell/CLI (e.g. Claude Code), run **`databricks bundle validate -t dev`** on the generated bundle and fix every error before handing it off or deploying — this catches the path and `run_if`/`outcome` errors above automatically. **Genie cannot validate** — `bundle validate` is a bundle command, outside its allow-list (its CLI is job-scoped only) — so on the Genie path the by-construction rules above are mandatory, and the `resources/*.yml` structure is only as correct as you emit it. See `references/dab-gotchas.md`.

See the worked reference bundle in the repo at `examples/databricks-source/databricks/` — a `databricks.yml` + `resources/job.yml` + `src/` layout: an all-SQL-tasks-plus-one-notebook Job with no pipeline resource. (The demo READMEs under `examples/*/README.md` model the generated-README shape below.)

## Step 5b — Write the bundle's `README.md`

Always emit a `README.md` at the bundle root so the migration is self-documenting — the reviewer/operator shouldn't have to reverse-engineer the source to understand what was produced. Keep it to the artifacts of *this* migration (don't restate the skill's general rules). Include these sections, in order:

1. **Summary** — a short description of the **source Matillion project** (its name, what it does in business terms, the export format and source backend, and the jobs/transformations it contains) and a description of the **output** (this bundle: one Databricks Job with N tasks, whether any Lakeflow pipeline or DQX gate was needed).
2. **Bundle layout** — a short tree of what was emitted (`databricks.yml`, `resources/`, `src/setup`, `src/notebooks`, `src/dq`, the setup notebook from Step 5c) with a one-line purpose per entry.
3. **Orchestration overview — a *before/after* pair of Mermaid `flowchart`s.** Emit **both**:
   - **Before** — the Matillion source graph: orchestration steps, and (as a second `subgraph`) the internal component chain of any transformation — every `table-input`/`join`/`aggregate`/`rewrite` node.
   - **After** — the Databricks Job task graph: one node per task, labelled with its task type (SQL task / notebook / pipeline / DQX gate), edges mirroring `depends_on`.

   The pair is required, not optional, because **consolidation is only visible in the contrast**: an after-DAG alone hides that (say) seven Matillion components collapsed into one SQL task. Put the diagrams side by side and make what merged obvious — draw the transformation's whole component subgraph in the *before*, a single `run_transformation` node in the *after*, and note "N components → 1 consolidated CTE query". Use the demo READMEs' before/after diagrams as the exact template (`flowchart TD` with a `subgraph` per Job/transformation; GitHub and the Databricks workspace both render Mermaid).
4. **Key translations** — a table with one row per non-trivial mapping the reader would otherwise have to infer: each Matillion component/step → its Databricks target (task key + file), plus any dialect/shape change worth flagging (e.g. Snowflake `::`→`CAST`, `run-transformation`→consolidated SQL task, `Assert`→DQX gate, a dropped `GRANT`). Link the deeper rule to the relevant `references/*.md` rather than re-explaining it.
5. **Variables** — every bundle variable / job parameter introduced, what it maps from (the Matillion source value), and its default — from the hardcoded-value sweep (`references/variables.md`, `references/hardcoded-values.md`).
6. **Secrets** — every credential surfaced and the secret scope/key it must be read from at runtime; note that none are stored in the bundle (`references/secrets.md`). Include the ready-to-run `databricks secrets create-scope` + one `put-secret <scope> <key>` per credential (values prompted, never inline) that the user must run before the first run. Omit the section only if the project has no credentials.
7. **Synthetic source data (test setup)** — describe the setup notebook (Step 5c): which source/input tables it fabricates (that the Matillion transforms read but no task produces), roughly what each contains (table → columns/row count/notable value ranges), that it's **synthetic stand-in data** generated with `dbldatagen`, and that it **only fills tables that don't already exist** — so where the real sources exist it no-ops and they're used instead. **State explicitly that the user must run this notebook by hand once before the first `bundle run`** — it is deliberately not a Job task, so nothing runs it automatically; skipping it means the Job fails at the first read (`TABLE_OR_VIEW_NOT_FOUND`) on a fresh workspace.
8. **Deployment** — the config values to set (`catalog`, `schema`, `warehouse_id`, host) and the exact commands **in order**, with the manual step called out as required, not optional:
   1. **Download the bundle to a local machine** — the generated bundle usually lives in the workspace, but `databricks bundle deploy` runs from a **local directory**, so export it first: `databricks workspace export-dir "<workspace path to this bundle>" ./<bundle-dir>` then `cd ./<bundle-dir>`. (Skip only if you already have the bundle locally.)
   2. `databricks bundle deploy …`
   3. **Manually run** the setup notebook `src/setup/00_generate_source_data.py` once (open it in the workspace, set the `catalog`/`schema` widgets, Run All) — **required** for a test run on a fresh workspace; skip *only* if the real source tables already exist.
   4. `databricks bundle run …`

   Match what Step 6 hands the user.
9. **Post-migration checklist** — literal checkboxes for the manual steps a human must do. Include both the **test-run** step and the **production** steps:
   - [ ] **Run `src/setup/00_generate_source_data.py` by hand** before the first `bundle run` (for a test on a fresh workspace) — it's not a Job task, so nothing runs it for you.
   - [ ] fill in real `workspace.host` per target, supply the real `warehouse_id`
   - [ ] create the secret scopes and grant READ
   - [ ] for production: ensure the **real** source tables exist (then the synthetic setup notebook is skipped)
   - [ ] review anything translated by intent (Matillion-runtime `python-script`, Snowpark procedures — see Step 5c and `references/snowflake-sql.md`)
   - [ ] run the validation checklist
10. **Sources** — the list of Matillion source files this bundle was generated from (e.g. `matillion/acme_sales.json`, or each `*.orch.yaml`/`*.tran.yaml`), so the migration is traceable back to its input.

Emit the Mermaid node labels and every table row from the *actual* graph and values you produced, not a template — a wrong DAG or a wrong variable list is worse than none.

## Step 5c — Emit a setup notebook (synthetic source data, manual pre-step)

The migrated Job reads whatever tables the Matillion transforms read — often **source/input tables the pipeline didn't create itself** (a Snowflake `RAW.*` schema, an ingestion landing table). On a fresh workspace those don't exist, so the Job fails at the first read (`TABLE_OR_VIEW_NOT_FOUND`) until the sources are in place. To let a reviewer run the converted project for a quick test, emit a **setup notebook** that creates and populates those input tables with **synthetic data**.

**Start from this exact skeleton — the first two cells (`%pip install` + `restartPython()`) are REQUIRED and must come before any import, or the notebook fails with `ModuleNotFoundError: dbldatagen` on serverless/vanilla clusters. Do not omit them.**

```python
# Databricks notebook source
# MAGIC %md # Setup: synthetic source data (manual pre-step — NOT a Job task)
# MAGIC Run this by hand once before the first `bundle run`. Guarded (skip-if-exists), so
# MAGIC it no-ops where the real source tables already exist.

# COMMAND ----------
# MAGIC %pip install dbldatagen

# COMMAND ----------
dbutils.library.restartPython()   # REQUIRED: makes the freshly-installed package importable

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "…")   # one widget per target schema the transforms read
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
import dbldatagen as dg
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType

# COMMAND ----------
# one guarded block PER source table the transforms read (see rules below)
_t = f"{catalog}.{schema}.<SOURCE_TABLE>"
if not spark.catalog.tableExists(_t):
    (dg.DataGenerator(spark, name="<source_table>", rows=200, partitions=1, randomSeed=42)
        # .withColumn(...) for EVERY column any transform reads from this table
    ).build().write.mode("overwrite").saveAsTable(_t)
```

Then fill it in per these rules:

- **It's a manual pre-setup step the user runs once — NOT a task in the Job.** Emit `src/setup/00_generate_source_data.py` as a standalone Databricks **notebook** (`# Databricks notebook source` first line) and keep it **out of the Job's task graph** — don't add it as a task or a `depends_on`. The user runs it themselves before the first `bundle run` (document this in the README's deployment steps + post-migration checklist). Keeping it out of the pipeline means the production Job never fabricates data as a side effect.
- **Guard every write with existence checks.** Use `CREATE TABLE IF NOT EXISTS` (or check the catalog and skip when the table is present) so running the notebook against a workspace that already has the real source tables no-ops instead of clobbering them, and re-running is idempotent.
- **Identify the inputs to create:** every table a transform reads (a source/read node — no incoming connectors) that **no task in the Job produces** is a source input to fabricate.
- **Get each table's columns from the source, don't infer them — and cover EVERY read.** In the classic JSON format a read node lists exactly which columns it selects — parse the **`Column Names`** parameter of each source/read node (and the `join`'s `Output Columns` / read-node projections) to get the precise column list per source table. Take the **union across every transform that reads the table** (different transforms read different subsets — miss one and the setup table lacks a column, failing at run time with `UNRESOLVED_COLUMN`). Column names may contain **hyphens or other non-alphanumeric characters** (e.g. `` `445_YYYY-MM` ``) — match backticked identifiers fully, don't restrict to `[A-Za-z0-9_]`.
- **Verify coverage mechanically — this is the #1 setup failure and it is NOT eyeball-able.** After emitting the setup notebook, run **`python3 scripts/check_setup_coverage.py <bundle-dir>`** (ships in this skill). It parses every transform read-node, diffs against what the setup generates per table, and lists any missing columns; it must report **zero missing** before the notebook is run. Add each missing column with the right type/values (below). Do not rely on reading the notebook by eye — a real project reads 100+ columns across many tables, and a single miss costs a full job-run round-trip to discover. (Static coverage only proves the column *exists*; the type/value rules below and the date-dimension rule make it *correct*.)
- **Date-dimension / calendar tables: generate with date math, not random values.** If a source is a date dimension (a `FULL_DATE` plus derived columns like `MONTH_NAME`, `WEEK_COMMENCING`, `QUARTER`, `PREVIOUS_WORKING_DATE`, `445_QUARTER`, `*_FLAG`), build it from `spark.sql("SELECT explode(sequence(DATE'start', DATE'end')) …")` and derive every column with Spark date functions (`date_format`, `weekofyear`, `date_trunc`, `last_day`, `quarter`, `add_months`, `extract(YEAROFWEEK FROM d)`), **not** `dbldatagen` random values. Transforms join and filter on the real calendar semantics (`WHERE FULL_DATE >= …`, `PARTITION BY YEAR_MONTH`), so random values pass column-coverage but yield garbage and can still fail type-sensitive predicates.
- **Type/value the synthetic data by how the transform *uses* it, not by the column name.** A column named `OVERDUE` sounds numeric but if a transform does `WHEN OVERDUE = 'Overdue'` it's a **string** with specific values — generate it as `values=["Overdue","Not Overdue"]`, not an integer, or the transform fails with `CAST_INVALID_INPUT`. Scan how each column is compared/cast/joined and match the generated type and value domain to that (string codes → the actual code strings; keys → matching FK ranges; dates → dates). Name-based type guessing is a common source of run-time cast failures. This is far more reliable than reverse-engineering columns from downstream usage across many transformations; fall back to downstream inference (which columns are cast/joined/selected) only for a column the read node doesn't enumerate. Types can stay loose (the demo sources use `STRING` and the transforms `CAST`), but the **column set** should come from the source's own `Column Names`.
- **Generate with `dbldatagen`, and get the API from its owner — don't write it from memory.** The dbldatagen API surface is **not** owned by this skill (it changes upstream), so don't hardcode or memorize option names here. To author the `withColumn` spec, **invoke the `databricks-data-generation` skill** (validated, local patterns) and consult the **official docs** — <https://databrickslabs.github.io/dbldatagen/> — as the source of truth. dbldatagen rejects an unknown option with a hard `DataGenError(msg='invalid column option …')` and mis-parses a bad date literal with a `ValueError: time data … does not match format`; when you hit either, look up the correct option/format in those sources rather than guessing another name.
- **Migration-specific wiring the dbldatagen docs won't tell you** (get these right regardless of the API details):
  - **Install + restart:** dbldatagen is **not** on serverless / vanilla clusters, so the notebook's first two cells are `%pip install dbldatagen` then `dbutils.library.restartPython()` — before any widgets/imports (restart clears state).
  - **Notebook shape:** `%pip install` → `restartPython()` → `catalog`/`schema` widgets → `CREATE SCHEMA IF NOT EXISTS` → per source table a skip-if-exists guard populated by one `dg.DataGenerator(..., seedColumnName="_seq").build()` + `.saveAsTable()`.
  - **Align keys to the transforms' joins:** a child table's foreign-key range must be a subset of its parent's primary-key range, or the migrated transform's joins silently drop rows. (Mind dbldatagen's seed base value when deriving keys.)
  - **Always pass `seedColumnName="_seq"` to *every* `dg.DataGenerator(...)` — unconditionally, not only when you notice an `ID` column.** `DataGenerator` auto-adds a hidden sequence column named `id`; the moment a table also defines an `ID`/`id` column (Matillion sources have them constantly), the build fails with `AMBIGUOUS_REFERENCE: Reference \`ID\` is ambiguous, could be: [\`ID\`, \`ID\`]` (Spark is case-insensitive). Deciding per-table "does this one need it?" is exactly the judgment that gets skipped and ships the bug — so don't decide: rename the seed on every block. `seedColumnName="_seq"` is harmless on tables that have no `ID`, and the `_seq` column is never selected (transforms read explicit column lists). Treat it as a fixed part of the constructor, like `partitions=`.
  - Set `partitions` explicitly and a fixed seed for reproducibility.
- **Make clear it's synthetic and a test convenience.** A header comment must state the notebook fabricates stand-in data for a test run, is run manually (not part of the Job), skips tables that already exist, and that for production the real ingestion should populate these tables instead. The README's synthetic-data section (Step 5b) documents what it creates; the deployment steps and post-migration checklist must tell the user to run it first for a test.
- **Run the notebook and fix it until it succeeds — don't hand over an unexecuted notebook.** If you can execute in the workspace (e.g. Genie can run notebooks/SQL) or via the CLI, **actually run `00_generate_source_data.py`** and iterate on every error until all tables are created: `ModuleNotFoundError` (missing `%pip`/restart), `DataGenError`/`ValueError` (bad dbldatagen option or date format), `AMBIGUOUS_REFERENCE` (`id` seed collision → `seedColumnName`), a bad `values=[None]`/`template` — fix and re-run. A setup notebook that only *looks* right but hasn't been executed is the top source of a failed first `bundle run`. Emitting is not done; **running green is done.**

`references/snowflake-sql.md` explains why source tables are often external (the Snowflake `RAW.*` assumption). The two repo demos instead seed tiny fixtures inline in their first SQL task — a simpler equivalent when the data is trivial and you don't mind it running each time.

## Step 5d — Verify the bundle against the checklist (required)

Before deploying, **run every item in `references/verification-checklist.md` against the generated bundle** and fix each finding. This is a required gate, not a nicety: it converts the dialect/DAB/setup gotchas scattered across the references into a concrete, greppable pass, and it is the step that catches the silent SQL bugs (`"quoted"` identifiers, `SELECT *,x AS existing`, `UPDATE…FROM`, `datediff(unit,…)`, `UNION` misalignment, unresolved `$T{}` placeholders, missing setup columns, …) that otherwise only surface as run-time task failures one at a time. If you can run the CLI, also run `databricks bundle validate -t dev`. Don't present the bundle as complete until this passes.

**First and most important: a transform is only "translated" when every component in it is.** The
worst — and easiest to ship — failure is a component you couldn't translate that gets emitted
as a literal placeholder and left in the file: `WHERE /* TODO: \`AND\` */` (dropped filter),
`(\`Group By\`) AS \`\`` (unresolved aggregate), `ON l.X = r.Left`/`r.Inner` (untranslated join
keys), `r.\`r.RISK_ID\`` (doubled alias), `/* TODO: translate unknown_… */`. These cluster in
the hardest transforms (multi-step joins/aggregates), so the temptation is to emit the easy 80%
and placeholder the rest — **don't.** Each marker is a guaranteed parse/analysis failure and
means that component was never translated; re-derive it from the source component (grep the
markers in `references/verification-checklist.md` → "Half-translated components"). Never hand off
or deploy a bundle that still contains one. Also: `${catalog}.${schema}.TABLE` does **not**
interpolate inside a `.sql` SQL task (`PARSE_SYNTAX_ERROR at or near '$'`) — use
`IDENTIFIER(:catalog || '.' || :schema || '.TABLE')` and pass the namespace parts as the task's
`parameters:`. And run the dialect greps over notebook `.py` files too, not only `src/sql/` —
the same overwrite/quote/`datediff` bugs hide inside `spark.sql("…")` strings.

**If you can execute, don't stop at static checks — actually run it green.** The grep checklist catches the known classes, but the definitive test is execution. Run the setup notebook (Step 5c), deploy, run the Job, and **fix each task failure and re-run until the whole Job is green** — that is the loop that surfaces the SQL-translation and data-shape bugs no static scan can. Only claim the migration works if you've *seen* it run (or you've told the user you couldn't execute and they must verify). "Generated and grep-clean" is weaker than "ran green".

**How much of that loop you can run depends on where you are — there are three execution layers, not two (see Step 6):**
- **CLI agent (e.g. Claude Code):** run the **autonomous run-to-green loop** (Step 6a) — `bundle deploy` → `bundle run` → read each failed task's output → fix → redeploy → rerun, repeating until **every task is SUCCESS**. This is the intended default when you have a CLI: don't stop at "generated" or "deployed", drive it green. Escalate to the user only for (a) destructive ops (`DROP SCHEMA … CASCADE`, `bundle destroy`), (b) a failure you can't diagnose after a genuine attempt, or (c) a data-*semantics* gap that needs a business decision. Then, if the user supplied expected output, run the **reconciliation** pass (Step 6b).
- **Genie in-workspace:** a *partial* loop. Genie can run notebooks/SQL and — via its **job-scoped** CLI (`runDatabricksCli`: trigger runs, list runs, inspect run output) — trigger an **already-deployed** Job and read each task's failure. So once someone has done the *initial* `bundle deploy`, Genie **can** iterate on **transform-content bugs** (the SQL/notebook layer — EXCEPT, UNION, dialect, unresolved-column, MERGE, SPLIT_TO_TABLE — most of what fails) by editing the deployed source files in place and re-triggering the Job. What Genie **cannot** do is any `bundle` command (`validate`/`deploy`/`run`/`destroy` are outside its allow-list), so it can't create the Job initially, and it can't make a **`resources/*.yml` structural change** (task deps, `run_if`, `../` paths, serverless, cross-task params) take effect — those need a redeploy. Genie escalates those to the user.
- **Generate-only (no execution at all):** rely on correct-by-construction + the checklist + `check_setup_coverage.py`, and tell the user they must run it.

## Step 6 — Deploy and validate

**The deployment procedure has four ordered steps — the download and the manual setup-notebook run are part of it, not afterthoughts:**

1. **Download the bundle locally** — if the generated bundle lives in the workspace, `databricks bundle deploy` still runs from a **local directory**, so export it first: `databricks workspace export-dir "<workspace path>" ./<bundle-dir>` then `cd ./<bundle-dir>`. (Skip if the bundle is already local.)
2. **Validate** — `databricks bundle validate -t dev` and fix every error before deploying. This catches emit bugs (wrong resource paths, `depends_on … outcome:` misuse, bad YAML) up front. **`bundle validate` is a bundle command** — available to a shell/CLI agent, but **not to Genie** (bundle commands are outside its allow-list, even though it has a job-scoped CLI). On the Genie path the `resources/*.yml` structure must therefore be correct by construction, since neither `validate` nor a redeploy is available to fix it in-loop (`references/dab-gotchas.md`).
3. **Deploy** the bundle — `databricks bundle deploy` with the config `--var`s (below).
4. **Run the setup notebook** `src/setup/00_generate_source_data.py` **once, manually** (open it, set the `catalog`/`schema` widgets, Run All — or `databricks jobs submit` a one-off notebook task). This creates the synthetic source tables the Job reads. **Required for a test run on a fresh workspace** — the notebook is deliberately not a Job task, so nothing runs it automatically, and a `bundle run` without it fails at the first read (`TABLE_OR_VIEW_NOT_FOUND`). Skip this step *only* when the real source tables already exist.
5. **Run** the Job — `databricks bundle run <job>`.

Whoever performs these steps depends on where you (the agent) are running — but always present them in order, and flag step 4 as required.

**`databricks bundle deploy` has no SDK/REST equivalent and is a *bundle* command. Who can run it depends on where you (the agent) are — and note that "can run the Databricks CLI" is not the same as "can deploy":**

- **If you can run a shell/CLI (e.g. Claude Code):** deploy with the Databricks CLI — `databricks bundle deploy -t dev` (with the `--var`s below), then `databricks bundle run <job> -t dev`. You own the full deploy→run→fix→redeploy loop.
- **If you're Genie in-workspace:** you have a **job-scoped** CLI (`runDatabricksCli` — triggering/listing runs, inspecting run output) but **`bundle validate`/`deploy`/`run`/`destroy` are outside your allow-list.** So you **cannot** create or redeploy the Job. Do this instead: generate the bundle, run the setup notebook, and **ask the user to run the initial `databricks bundle deploy` once** (hand them the exact command + bundle location; never claim you deployed it). Because the committed `databricks.yml` ships placeholders, that command **must pass the config with `--var`** (`warehouse_id`, `catalog`, `schema`, …) from the Step 5 answers — a bare `bundle deploy` fails on the empty `warehouse_id`. **After** that first deploy, you *can* iterate: trigger the Job with the job-scoped CLI, read each failed task's output, fix **transform-content** bugs by editing the deployed source files in place, re-run the setup notebook if a source table changed, and re-trigger — until the SQL layer is green. Escalate back to the user for a **redeploy** only when a fix touches `resources/*.yml` (task graph, `run_if`, paths, serverless, params), since those don't take effect without `bundle deploy`. See `references/deploy-and-validate.md` for the ready-to-run template.

Then validate: run the checklist SQL against the deployed tables — via `databricks sql` / the SQL statements API from a CLI, or in-chat in Genie (Genie can run SQL). Follow the checklist in `references/deploy-and-validate.md`.

## Step 6a — The autonomous run-to-green loop (CLI agent)

When you have a shell CLI, **do not stop at "deployed" — loop until every task is green.**
This is the default, not an extra. The mechanics (they matter — reinventing them wastes runs):

1. **Coverage-gate the setup notebook first.** Run `python3 scripts/check_setup_coverage.py <bundle-dir>` and fix any missing columns *before* the first run — this pre-empts the single most common failure class (`UNRESOLVED_COLUMN` on a missing source column), which otherwise surfaces one task-run at a time.
2. **Deploy + run setup + run the Job** (Step 6, with the `--var`s).
3. **On failure, get the *per-task* error, not the run-level summary.** The run-level message is just "Task X failed". Get the run's task list (`databricks jobs get-run <run_id>`), find the failed task's `run_id`, then read its analysis error with `databricks jobs get-run-output <task_run_id>` (the `error`/`error_trace` field). That gives the exact `UNRESOLVED_COLUMN` / `PARSE_SYNTAX_ERROR` / `COLUMN_ALREADY_EXISTS` / etc. to fix.
4. **Fix the class, not just the instance.** Each error maps to a class in `references/verification-checklist.md` (EXCEPT-of-created-column, UNION-by-position, lateral-alias-in-window, `${}`-in-SQL, `SPLIT_TO_TABLE`, `UPDATE…FROM`, mangled dates, …). When you hit one, **grep the whole bundle for the same pattern and fix every occurrence** — don't fix one and redeploy, or you'll pay a full round-trip per instance.
5. **Redeploy and rerun.** Repeat 3–5 until the run reaches `TERMINATED / SUCCESS` with every task `SUCCESS`.
6. **Stop / escalate rules.** Done = all tasks SUCCESS. Escalate to the user for: destructive ops (`DROP SCHEMA … CASCADE`, `bundle destroy` — never run these without explicit authorization), an error you genuinely can't diagnose after trying, or a **data-semantics** question (what a transform *should* compute) that needs a human. Don't loop forever on an unchanging error — if a fix doesn't change the error, stop and report.
7. **Whenever a run surfaces a *new* bug class, update the skill** (`references/verification-checklist.md` + the relevant reference), per AGENTS.md — the loop's job is both to green *this* bundle and to harden the skill for the next.

## Step 6b — Reconcile against expected output (when provided)

A green run proves the Job *executes*; it does not prove the results are *correct*. If the
user provides **expected output** — a golden table/file, or a spec of row counts + key
aggregates — run a **reconciliation loop** after the Job is green, the value-level twin of the
run-to-green loop (Step 6a): **reconcile → localize the divergence → fix the transform →
re-run → re-reconcile, repeating until every table matches (or only signed-off exceptions
remain).** Use **`superpowers:systematic-debugging`** for the tracing discipline — reconcile
first, form one hypothesis at a time, don't shotgun-edit.

- **Only meaningful against real (or user-supplied) source data.** The synthetic setup
  notebook (Step 5c) fabricates *random* stand-in data, so migrated values won't match the
  real Matillion output — with synthetic data you can only check schema/shape, not values.
  Run value reconciliation only when the Job ran against **real sources** or a user-supplied
  input+expected pair.
- **Compare, per output table — prefer DQX `compare_datasets`.** The migration already uses
  DQX (Step 5c, `references/data-quality.md`), so reconcile a golden table with its
  **dataset-level `compare_datasets` check** rather than hand-rolled SQL. It does a **row-level
  comparison**: row-match the Job output against the **gold** (`ref_table=` the golden table, or
  `ref_df_name=` a loaded DataFrame) on the primary-key `columns`/`ref_columns`, and it flags,
  per row, which rows are **missing on either side** (`row_missing` / `row_extra` — set
  **`check_missing_records=True`** for the full outer join that also finds rows missing from the
  output) and which **columns differ** on matched rows (`changed`), emitted as JSON. That per-row,
  per-column detail *is* your localization — it names the exact diverging rows and fields. Use
  **`abs_tolerance` / `rel_tolerance`** for the rounding/precision class below.
  It's a `@register_rule("dataset")` check applied via `DQEngine` (split valid/quarantine like
  any DQX gate) — **get the exact signature from the DQX docs / `dqx-apply-checks`, don't
  hand-code the API from memory** (`compare_datasets` isn't in the DQX skills' examples yet;
  it lives in `databricks.labs.dqx.check_funcs`).
  - **No DQX available / a quick check** → schema (names + types) → `COUNT(*)` → a key diff
    (`EXCEPT` **both directions** on the PK, or a full-row hash) — report rows missing / extra /
    differing.
  - **Row-count + key-aggregate spec** → each table's `COUNT(*)` and the named
    `SUM`/`MIN`/`MAX`/`COUNT(DISTINCT …)` vs expected; report every mismatch expected-vs-actual.
- **Localize before you fix — this is the hard part.** Narrow a mismatch to the exact
  *field + transform* before touching code, by widening the diff stepwise: schema → row count
  → which **keys** differ → which **columns** differ on those keys (DQX `compare_datasets`
  hands you this row+column breakdown directly via its `changed`/`row_missing`/`row_extra`
  detail; otherwise `EXCEPT` both ways). If the
  final table doesn't localize it, reconcile an **intermediate** output — materialize the
  suspect CTE or the upstream `CDM_*` table and diff *that* — and **bisect the chain** until you
  find the component that introduces the divergence. A whole-table hash mismatch with an
  unknown cause is not yet actionable; keep narrowing.
- **Fix the class, not the instance** (as in 6a). A value mismatch is almost always a
  **semantic dialect difference**: rounding / precision (int vs decimal division), null-handling
  (`NULL` vs `''`, `coalesce` defaults), date/timestamp boundaries and truncation, join
  **multiplicity** (fan-out duplicating rows), `QUALIFY`/`ROW_NUMBER` dedupe ordering, or
  case/collation. Fix the translation so it is *generally* correct, then re-run.
- **Stop / escalate rules.** Done = every table matches, or the only remaining diffs are
  exceptions the user has signed off. **Escalate — don't guess —** when a difference is a
  genuine **business/semantic** decision (the source data has drifted since the golden was
  captured; Matillion did something intentional you can't infer from the export). **Never
  overfit to the gold:** a fix must be a defensible general translation — not a hardcoded
  value, a `WHERE` that just drops the offending rows, or logic reverse-engineered only to
  make this one table match. If a fix doesn't change the diff, stop and report rather than
  looping (same rule as 6a).
- **Report the final reconciliation result** — which tables matched, which diffs remain, and
  what each remaining diff is. Don't claim fidelity you didn't measure.
