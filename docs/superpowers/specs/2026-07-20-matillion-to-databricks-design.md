# Matillion → Databricks Migration Skill — Design

**Date:** 2026-07-20
**Status:** Approved (design phase)

## Purpose

Replace the existing `palantir-to-dbx` skill (Palantir Foundry Workshop → Databricks
App) with a new skill that migrates **Matillion ETL pipelines** to **Databricks**. The
skill is an **end-to-end migration workflow**: parse the Matillion project → map each
component → generate Databricks code → deploy and validate.

The Palantir content is discarded (replace in place). The directory may keep its
`palantir-to-dbx` name; the skill's `name:` frontmatter becomes
`matillion-to-databricks`.

## Source material

Two real Matillion sample files anchor the design and every reference example:

- `create-maia-demo-data.orch.yaml` — orchestration
- `sales-by-category-region.tran.yaml` — transformation

### Matillion file model

**Transformation (`*.tran.yaml`)** — a **dataflow DAG**. Each component declares
`sources` (upstream component refs) and compiles to SQL. Observed types:

- `table-input` — reads a UC table; `columnNames` is an explicit projection list.
- `join` — `mainTable`/`mainTableAlias`, `joins` (table, alias, type),
  `joinExpressions` (backticked SQL predicates), `columnMappings` (source col → output col).
- `aggregate` — `groupings` (GROUP BY cols) + `aggregations` (col, func e.g. Sum/Count).
- `rewrite-table-dl` — writes the transformation's output table.

**Orchestration (`*.orch.yaml`)** — a **control-flow DAG**. Components are connected by
`transitions` (`unconditional` / `success` / `failure`) from `start` to `end-success`.
Observed types:

- `start` / `end-success` — graph boundaries.
- `sql-executor` — raw SQL (DDL + seed `INSERT ... VALUES`).
- `run-transformation` — invokes a named `.tran.yaml` (`transformationJob:`).
- `python-script` — Python; in the sample it uses Matillion-runtime APIs
  (`subprocess`, `context.cursor()`) wrapping embedded SQL.

## Target mapping (the core decision)

| Matillion concept | Databricks target |
|---|---|
| Orchestration (`.orch.yaml`) | **Databricks Job (Workflow)** — `transitions` become task dependencies; `failure:` branches become task-failure conditions |
| Transformation (`.tran.yaml`) | **Lakeflow Declarative Pipeline** — invoked as a pipeline task inside the Job |
| `sql-executor` step | Job **SQL task** (or notebook task) |
| `python-script` step | Job **notebook task** (PySpark); Matillion plumbing discarded, embedded SQL preserved |
| `run-transformation` step | The Job task that runs the corresponding Lakeflow pipeline |

**Hybrid model:** orchestration → Job; transformation → Lakeflow Declarative Pipeline.
This cleanly expresses control flow (Job task deps) that Lakeflow's declarative model
does not.

**Generated pipeline language:** SQL by default
(`CREATE OR REFRESH MATERIALIZED VIEW` / `STREAMING TABLE`) for
`table-input`/`join`/`aggregate`/`rewrite-table-dl`, which map 1:1. Use Python (PySpark)
only for components that genuinely need it (non-SQL `python-script` logic).

## Architecture

### `SKILL.md` — the phased workflow

1. **Inventory the Matillion project** — find all `*.orch.yaml` / `*.tran.yaml`; note
   which `run-transformation` steps call which transformations.
2. **Parse the orchestration graph** — walk `transitions` from `start` to `end-success`;
   this becomes the Job's task ordering.
3. **Parse each transformation graph** — walk `sources` refs; this becomes a chain of
   Lakeflow streaming tables / materialized views.
4. **Map each component** — for every component, open its reference file and translate.
5. **Assemble the deliverable** — emit a DAB bundle (`databricks.yml`) with the Job +
   Lakeflow pipeline resources and the SQL/Python source files.
6. **Deploy & validate** — delegate to existing skills (below).

### `references/` — one file per component type (Approach A)

```
references/
  transformation/
    table-input.md
    join.md
    aggregate.md
    rewrite-table.md
  orchestration/
    start-end.md
    sql-executor.md
    run-transformation.md
    python-script.md
  mapping-cheatsheet.md      # one-page type → equivalent index
  deploy-and-validate.md     # phase 6 handoff + validation checklist
  gotchas.md                 # hard-won lessons, grows over time
```

Each component reference uses the same template:
**What the Matillion component does → the Lakeflow/Job equivalent → worked example
(from the real samples) → gotchas.**

### Deployment & validation (Phase 6) — delegate, don't reinvent

- **Deploy** → `fe-databricks-tools:databricks-resource-deployment` (covers Lakeflow
  pipelines + Jobs, prefers serverless, uses `databricks sync`, UC 3-layer namespaces).
  The skill emits the DAB bundle and hands off.
- **Validate** → `fe-databricks-tools:databricks-query` — confirm target tables
  materialized, row counts reasonable, spot-check an aggregate against source.

No `databricks-apps` involvement — Matillion produces ETL pipelines only, no UI.

## Initial gotchas bank (`references/gotchas.md`)

Seeded from the real samples; grows as new issues surface:

- **`[Environment Default]` catalog/schema placeholders** — Matillion resolves these from
  its environment. Must be replaced with real UC 3-layer names. Note the inconsistency in
  the samples: `.tran.yaml` uses `[Environment Default]` while the `python-script`
  hardcodes `marcin_demo.default`. Resolve/parameterize these consistently.
- **Seed data in `sql-executor`** (`CREATE OR REPLACE TABLE ... INSERT INTO VALUES`) is
  demo fixture data, not a transform — keep as a Job SQL/setup task, don't model as a
  pipeline table.
- **`python-script` Matillion-runtime APIs** (`subprocess`, `context.cursor()`) don't
  exist in Databricks. Extract the embedded SQL; discard the plumbing.
- **Backticked identifiers & aliases** in join expressions carry over to Spark SQL;
  preserve `mainTableAlias` wiring.
- **Column projection** — `table-input`'s `columnNames` is an explicit select-list;
  preserve it rather than `SELECT *`.

## Out of scope

- Non-ETL Matillion features (API components, external orchestration triggers) — add to
  references only if encountered.
- Automated round-trip verification of transform equivalence beyond the row-count /
  aggregate spot-check.

## Success criteria

- Given a Matillion project of `.orch.yaml` + `.tran.yaml` files, the skill guides
  producing a deployable DAB bundle (Job + Lakeflow pipeline) plus SQL/Python sources.
- Each component type present in the samples has a reference with a worked example.
- Deployed pipeline materializes the expected target tables with sane row counts.
