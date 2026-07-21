---
name: matillion-to-databricks
description: Guide for migrating Matillion ETL pipelines to Databricks. Trigger when the user wants to migrate Matillion orchestration (*.orch.yaml) and transformation (*.tran.yaml) pipelines to Databricks. Matillion orchestration pipelines become Databricks Jobs; transformation pipelines become Job tasks (SQL task for pure SQL, notebook otherwise), with Lakeflow Declarative Pipelines reserved for incremental/streaming or data-quality needs. Consult the relevant component reference before translating each component.
---

# Matillion → Databricks Migration Guide

An end-to-end workflow for migrating Matillion ETL pipelines to Databricks.

**Terminology (keep the sides unambiguous):** the source artifacts are **Matillion pipelines** — matching the Data Productivity Cloud format (note the top-level `pipeline:` key in each file). The Databricks targets are a **Databricks Job** (always) and its **tasks** (SQL task, notebook task, and — only when justified — a Lakeflow pipeline task). Always qualify which side you mean: "Matillion orchestration pipeline" → "Databricks Job"; "Matillion transformation pipeline" → "a Job task" (usually SQL or notebook).

Matillion projects are made of two pipeline file types:

- `*.orch.yaml` — **orchestration pipeline**: a control-flow DAG of steps connected by `transitions`. Becomes a **Databricks Job** — the outer shell that holds the whole migration.
- `*.tran.yaml` — **transformation pipeline**: a dataflow DAG of components connected by `sources`. Becomes a **task inside that Job** — a SQL task for pure SQL, a notebook task otherwise, or a Lakeflow pipeline only when incremental/streaming or data-quality features are actually needed.

Consult the component reference (below) **before** translating each component, not after something breaks.

---

## The two decisions of every migration

Migrating a Matillion project is two nested decisions, in order:

1. **The shell — always a Databricks Job.** The orchestration pipeline's control flow (ordering, branching, retries, schedules, parameters) becomes the Job's task graph. This is not a judgment call: control flow can only live in a Job.
2. **The executor — how each step/transformation runs *inside* the Job.** Pick per task using the ladder below. This is where the real judgment is, and the default is **not** Lakeflow.

### Decision 1: orchestration → Job (the outer shell)

A **Databricks Job (Workflow)** is an *imperative task orchestrator*: it decides **what runs, in what order, and under what conditions**. Every Matillion `transitions` edge becomes a task dependency; branches/loops/nesting become `run_if` / `for_each` / `run_job_task`. The Job is the outer shell that holds the entire migration — nothing below replaces it.

### Decision 2: pick the executor for each task (the ladder)

For each step (and each transformation pipeline), walk this ladder **top-down and stop at the first match**. The bias is toward the simplest, most debuggable, warehouse-native option — reserve Lakeflow for when its managed features actually earn their cost.

1. **Pure SQL, batch / full-refresh** → **SQL task.** The default for `sql-executor` and for any transformation that consolidates to one full-refresh query (`table-input` → `join` → `aggregate` → `rewrite-table-dl` with a single output). Cheapest, runs on the SQL warehouse, no cluster.
2. **Imperative logic, mixed SQL + Python, or you just want a debuggable migration landing** → **notebook task** (running `spark.sql(...)`). The default for `python-script` and for transformations too tangled for one clean query. Notebooks are the pragmatic migration workhorse: faithful to imperative sources, steppable cell-by-cell, and free of declarative constraints.
3. **Incremental / streaming / CDC, OR you want managed data-quality expectations + auto-lineage** → consider a **Lakeflow Declarative Pipeline** (pipeline task). This is the *escape hatch*, not the default. Even here, a notebook running Structured Streaming is often simpler for a first migration — reach for Lakeflow specifically when you want it to *manage* checkpoints/state, expectations, and lineage for you rather than hand-rolling them. See `references/orchestration/run-transformation.md` for the full Lakeflow-vs-task trade-off.

**Why not Lakeflow by default?** A pipeline is a separate resource with its own compute lifecycle and deploy surface. It only pays off when you use what it provides — incremental maintenance, streaming, `EXPECT` rules, multi-output lineage. A single full-refresh transform uses none of that, so it's just a SQL task wearing extra machinery. Match the tool to the features you actually need.

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

The Job is the outer shell; each step is a task, executor chosen by the ladder:

```
Databricks Job (from the orchestration pipeline)
├─ task: seed/DDL            (sql-executor        → SQL task)
├─ task: run transformation  (run-transformation  → SQL task if pure SQL; else notebook; Lakeflow only if incremental/streaming/DQ)
├─ task: nested orchestration(run-orchestration   → run_job_task)
└─ task: post-process        (python-script       → notebook task, run_if success)
```

**Preserve the task graph — don't collapse control flow.** Choosing executors is orthogonal to the graph's shape. It's tempting to fold everything into one big notebook, but that discards the per-task observability, granular retry/repair-run, and parallelism that *are* the orchestration. Keep one task per Matillion step; only choose *how* each runs.

When unsure about the shell-vs-executor split, ask: *"Is this deciding what-runs-when (→ the Job's graph), or is it the work a single task does (→ pick an executor)?"*

---

## Step 1 — Inventory the Matillion project

Find every pipeline file and map the call graph:

```bash
find . -name '*.orch.yaml' -o -name '*.tran.yaml'
```

- Orchestration pipelines are the entry points.
- For each orchestration pipeline, note every `run-transformation` step (which `.tran.yaml` it names via `transformationJob:`) and every `run-orchestration` step (which `.orch.yaml` it names via `orchestrationJob:`). This tells you which pipeline feeds which Job task and which orchestrations are nested inside others.
- Note every variable the pipelines declare, pass (`setScalarVariables`/`setGridVariables`), or read (`${...}`) — variables migrate alongside the pipelines. See `references/variables.md`.
- **Flag every secret/credential** — connection passwords, API tokens, storage keys, OAuth entries, or values sourced from a cloud secret manager (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager). These migrate to **Databricks secrets**, not to variables or code. See `references/secrets.md`.
- **Sweep for every other hardcoded value** — **catalog/schema names (always parameterize these as bundle variables — never hardcode the namespace)**, warehouse/host IDs, storage paths, connection details, tuning/business literals. Don't carry any literal across blindly: each one is a candidate for a bundle variable, a job parameter, a secret, or staying inline. See `references/hardcoded-values.md`.

Write down: the list of orchestration pipelines, what each one calls (transformations and nested orchestrations), the variables in play, the secrets in play (and their current source), every hardcoded value worth surfacing, and any transformation not called by anything (a standalone pipeline).

## Step 2 — Parse the orchestration graph

For each `.orch.yaml`, walk `transitions` (`unconditional` / `success` / `failure`) from the `start` component to `end-success`. This control-flow DAG becomes the **Job's task graph** — each transition is a task dependency; `failure:` branches become failure-condition task dependencies.

See:

| Matillion orchestration type | Reference |
|---|---|
| `start` / `end-success` | `references/orchestration/start-end.md` |
| `sql-executor` | `references/orchestration/sql-executor.md` |
| `run-transformation` | `references/orchestration/run-transformation.md` |
| `run-orchestration` | `references/orchestration/run-orchestration.md` |
| `python-script` | `references/orchestration/python-script.md` |
| variables (all scopes) | `references/variables.md` |
| secrets / credentials | `references/secrets.md` |

## Step 3 — Parse each transformation graph

For each `.tran.yaml`, walk `sources` refs from `table-input` leaves to the final `rewrite-table-dl`. This dataflow DAG describes one (or a few) output tables and how to compute them.

**Consolidate first, then pick the executor.** A linear chain that yields a single output collapses into **one query** (CTEs for the intermediate `join`/`aggregate` components) — don't emit one dataset per component. Then apply the executor ladder from "The two decisions" above:
- pure full-refresh SQL → **SQL task** (`CREATE OR REPLACE TABLE ... AS <one SELECT>`) — the common case;
- needs Python/imperative glue → **notebook task**;
- genuinely needs incremental/streaming or managed data-quality/lineage → **Lakeflow pipeline** (then the consolidation rule about materialized views vs. CTEs applies — `references/transformation/rewrite-table.md`).

Keep a component as its own dataset only when it earns it: it's **reused**, needs its own **expectations**, or is a genuine **branch point**.

See:

| Matillion transformation type | Reference |
|---|---|
| `table-input` | `references/transformation/table-input.md` |
| `join` | `references/transformation/join.md` |
| `aggregate` | `references/transformation/aggregate.md` |
| `rewrite-table-dl` | `references/transformation/rewrite-table.md` |

Quick lookup for every type: `references/mapping-cheatsheet.md`.

## Step 4 — Map each component

For every component in every file, open its reference and translate it. Default to **SQL** (`CREATE OR REPLACE TABLE ... AS SELECT` for a SQL task, or `CREATE OR REFRESH MATERIALIZED VIEW` inside a Lakeflow pipeline); use **PySpark in a notebook** where SQL can't express it or the source is imperative. Choose the executor per the ladder in "The two decisions".

Before writing any code, read `references/gotchas.md` — it lists the mistakes that waste the most time (unresolved `[Environment Default]` placeholders, seed data mistaken for transforms, Matillion-runtime Python APIs). If the project uses any credentials, also read `references/secrets.md` — secrets go in Databricks secret scopes and are referenced at runtime, never inlined or turned into bundle variables.

**Surface every hardcoded value and let the user choose its target.** Don't silently carry a literal across. For each one, classify it and propose a target — **secret** (credentials), **bundle variable** (per-environment config), **job parameter** (per-run input), or **leave inline** (true constants) — explain why, and confirm before wiring. Present the findings as a table (redact secret values). Full triage: `references/hardcoded-values.md`.

## Step 5 — Assemble the Databricks Asset Bundle

**Ask the user how to name the Job** before emitting the bundle (and each additional Job, if there are nested orchestrations). Don't silently reuse the Matillion pipeline's internal name — propose a clean default derived from the `.orch.yaml` (e.g. `create-maia-demo-data.orch.yaml` → `maia-demo-job`) and let them confirm or override. This sets the job resource key, the `name:`, and how they'll find it in the Workflows UI, so it's worth a quick check rather than a guess.

Emit a DAB (`databricks.yml`) with:
- one **job** resource per orchestration pipeline (`.orch.yaml`), named as agreed above, whose tasks mirror the orchestration graph: SQL tasks for `sql-executor`, a task per `run-transformation` (SQL task if the transformation is pure SQL — the common case; notebook if imperative; pipeline task only if it needs Lakeflow), a `run_job_task` for each `run-orchestration` (nested orchestration), and a notebook task for `python-script`,
- a **pipeline** resource **only** for transformations that actually need Lakeflow (incremental/streaming or managed data-quality/lineage) — most migrations emit none,
- **bundle variables / job parameters** for the Matillion variables (see `references/variables.md`), so per-environment config and per-run inputs are parameterized rather than hardcoded,
- **Databricks secret scopes** for every credential (see `references/secrets.md`) — referenced via `{{secrets/scope/key}}` / `dbutils.secrets.get` / a UC connection, never as a bundle variable or plaintext.

See the worked reference bundle at `examples/demo/databricks/` — an all-SQL-tasks-plus-one-notebook Job with no pipeline resource.

## Step 6 — Deploy and validate

**Deploying runs the Databricks CLI (`databricks bundle deploy`) — there is no SDK/REST equivalent. Who runs it depends on where you (the agent) are running:**

- **If you can run a shell/CLI (e.g. Claude Code):** deploy via the `fe-databricks-tools:databricks-resource-deployment` skill (it handles Jobs + Lakeflow pipelines, prefers serverless, uses `databricks sync`, and UC 3-layer namespaces). Trigger it with: "use the databricks-resource-deployment skill to deploy this bundle".
- **If you're inside the workspace and CANNOT run the CLI (e.g. Databricks Genie):** you cannot deploy. **Generate the bundle, then explicitly ask the user to run `databricks bundle deploy` themselves** — hand them the exact commands and the bundle's location, and never claim you deployed it.

Then validate: in Claude Code use `fe-databricks-tools:databricks-query`; in Genie run the checklist SQL in-chat (Genie can run SQL). Follow the checklist in `references/deploy-and-validate.md`.
