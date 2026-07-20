---
name: matillion-to-databricks
description: Guide for migrating Matillion ETL pipelines to Databricks. Trigger when the user wants to migrate Matillion orchestration (*.orch.yaml) and transformation (*.tran.yaml) pipelines to Databricks. Matillion orchestration pipelines become Databricks Jobs; Matillion transformation pipelines become Lakeflow Declarative Pipelines. Consult the relevant component reference before translating each component.
---

# Matillion → Databricks Migration Guide

An end-to-end workflow for migrating Matillion ETL pipelines to Databricks.

**Terminology (keep the sides unambiguous):** the source artifacts are **Matillion pipelines** — matching the Data Productivity Cloud format (note the top-level `pipeline:` key in each file). The Databricks targets are **Databricks Jobs** and **Lakeflow pipelines**. Always qualify which side you mean: "Matillion orchestration pipeline" → "Databricks Job"; "Matillion transformation pipeline" → "Lakeflow pipeline".

Matillion projects are made of two pipeline file types:

- `*.orch.yaml` — **orchestration pipeline**: a control-flow DAG of steps connected by `transitions`. Becomes a **Databricks Job**.
- `*.tran.yaml` — **transformation pipeline**: a dataflow DAG of components connected by `sources`. Becomes a **Lakeflow Declarative Pipeline**.

Consult the component reference (below) **before** translating each component, not after something breaks.

---

## Step 1 — Inventory the Matillion project

Find every pipeline file and map the call graph:

```bash
find . -name '*.orch.yaml' -o -name '*.tran.yaml'
```

- Orchestration pipelines are the entry points.
- For each orchestration pipeline, note every `run-transformation` step (which `.tran.yaml` it names via `transformationJob:`) and every `run-orchestration` step (which `.orch.yaml` it names via `orchestrationJob:`). This tells you which pipeline feeds which Job task and which orchestrations are nested inside others.
- Note every variable the pipelines declare, pass (`setScalarVariables`/`setGridVariables`), or read (`${...}`) — variables migrate alongside the pipelines. See `references/variables.md`.

Write down: the list of orchestration pipelines, what each one calls (transformations and nested orchestrations), the variables in play, and any transformation not called by anything (a standalone pipeline).

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

## Step 3 — Parse each transformation graph

For each `.tran.yaml`, walk `sources` refs from `table-input` leaves to the final `rewrite-table-dl`. This dataflow DAG becomes a chain of Lakeflow **materialized views / streaming tables**.

See:

| Matillion transformation type | Reference |
|---|---|
| `table-input` | `references/transformation/table-input.md` |
| `join` | `references/transformation/join.md` |
| `aggregate` | `references/transformation/aggregate.md` |
| `rewrite-table-dl` | `references/transformation/rewrite-table.md` |

Quick lookup for every type: `references/mapping-cheatsheet.md`.

## Step 4 — Map each component

For every component in every file, open its reference and translate to Lakeflow SQL (default) or PySpark (only where SQL cannot express it). Default target: `CREATE OR REFRESH MATERIALIZED VIEW`.

Before writing any code, read `references/gotchas.md` — it lists the mistakes that waste the most time (unresolved `[Environment Default]` placeholders, seed data mistaken for transforms, Matillion-runtime Python APIs).

## Step 5 — Assemble the Databricks Asset Bundle

Emit a DAB (`databricks.yml`) with:
- one **pipeline** resource per transformation pipeline (`.tran.yaml`) — the Lakeflow Declarative Pipeline + its SQL/Python source files,
- one **job** resource per orchestration pipeline (`.orch.yaml`), whose tasks mirror the orchestration graph: SQL tasks for `sql-executor`, a pipeline task for each `run-transformation`, a `run_job_task` for each `run-orchestration` (nested orchestration), and a notebook task for `python-script`.
- **bundle variables / job parameters** for the Matillion variables (see `references/variables.md`), so per-environment config and per-run inputs are parameterized rather than hardcoded.

## Step 6 — Deploy and validate

Use the `fe-databricks-tools:databricks-resource-deployment` skill for all deployment (it handles Lakeflow pipelines + Jobs, prefers serverless, uses `databricks sync`, and UC 3-layer namespaces). Trigger it with: "use the databricks-resource-deployment skill to deploy this bundle".

Then use `fe-databricks-tools:databricks-query` to validate. Follow the checklist in `references/deploy-and-validate.md`.
