# Matillion → Databricks Migration Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Palantir Workshop→App skill with an end-to-end skill that migrates Matillion ETL pipelines (`*.orch.yaml` orchestrations, `*.tran.yaml` transformations) to Databricks Jobs + Lakeflow Declarative Pipelines.

**Architecture:** A `SKILL.md` phased workflow (inventory → parse graphs → map components → assemble DAB bundle → deploy/validate) backed by one reference file per Matillion component type (Approach A). Orchestrations map to Databricks Jobs; transformations map to Lakeflow Declarative Pipelines (SQL by default, Python only where needed). Deployment and validation delegate to existing `fe-databricks-tools` skills.

**Tech Stack:** Markdown skill files (Claude Code skill format with YAML frontmatter). Target platform: Databricks Asset Bundles (`databricks.yml`), Lakeflow Declarative Pipelines (SQL / PySpark), Databricks Jobs.

**Deliverable type note:** This skill's output is documentation (Markdown), not executable code. "Tests" here are structural/content verifications (frontmatter valid, referenced files exist, examples trace to the real sample YAML). Each task ends with a verify step and a commit.

## Global Constraints

- Skill `name:` frontmatter MUST be `matillion-to-databricks`. Directory renamed from `palantir-to-dbx` to `matillion-to-databricks`.
- All Palantir content (old `SKILL.md`, `references/layout`, `references/widgets`, `references/data`) MUST be removed. Do not carry Palantir widget/HAR content forward.
- Target mapping is fixed: orchestration (`.orch.yaml`) → Databricks Job; transformation (`.tran.yaml`) → Lakeflow Declarative Pipeline invoked as a Job task.
- Generated transformation code defaults to **Lakeflow SQL** (`CREATE OR REFRESH MATERIALIZED VIEW` / `STREAMING TABLE`); use Python (PySpark) only for components that cannot be expressed in SQL.
- Deployment delegates to `fe-databricks-tools:databricks-resource-deployment`; validation delegates to `fe-databricks-tools:databricks-query`. No `databricks-apps` (ETL only, no UI).
- Every component reference follows the same template: **What the Matillion component does → the Databricks equivalent → worked example (from the real sample YAML) → gotchas.**
- Worked examples MUST derive from the two real sample files: `create-maia-demo-data.orch.yaml` and `sales-by-category-region.tran.yaml`.
- Use UC 3-layer namespaces (`catalog.schema.table`). Replace Matillion `[Environment Default]` placeholders with real UC names.
- Keep the two sample `.yaml` files in the skill directory as reference fixtures.

---

### Task 1: Reset directory — remove Palantir content, rename skill

**Files:**
- Delete: `SKILL.md` (old Palantir content), `references/layout/`, `references/widgets/`, `references/data/`, `references/.DS_Store`
- Preserve: `create-maia-demo-data.orch.yaml`, `sales-by-category-region.tran.yaml`, `docs/`, `.gitignore`
- Rename: directory `palantir-to-dbx` → `matillion-to-databricks` (defer actual dir rename to Task 9 to avoid breaking in-flight paths; for now just clear contents)

**Interfaces:**
- Produces: a clean `references/` directory (empty) and no old `SKILL.md`, ready for new content.

- [ ] **Step 1: Verify the sample files and docs exist before deleting anything**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
ls create-maia-demo-data.orch.yaml sales-by-category-region.tran.yaml docs/superpowers/specs/2026-07-20-matillion-to-databricks-design.md
```
Expected: all three paths listed, no "No such file".

- [ ] **Step 2: Remove Palantir skill content**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
rm -f SKILL.md
rm -rf references/layout references/widgets references/data references/.DS_Store
mkdir -p references/transformation references/orchestration
ls -R references
```
Expected: `references/transformation` and `references/orchestration` exist, both empty; no `layout`/`widgets`/`data`.

- [ ] **Step 3: Verify no Palantir references remain**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -ril "palantir\|foundry\|workshop\|stemma" . --include='*.md' | grep -v docs/superpowers/specs || echo "CLEAN"
```
Expected: `CLEAN` (the spec doc may mention Palantir for context; that's allowed).

- [ ] **Step 4: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add -A
git commit -m "Remove Palantir skill content; scaffold references dirs"
```

---

### Task 2: Write SKILL.md — frontmatter and phased workflow

**Files:**
- Create: `SKILL.md`

**Interfaces:**
- Consumes: nothing (entry point).
- Produces: the workflow that references every file created in Tasks 3–8 by exact path (`references/transformation/*.md`, `references/orchestration/*.md`, `references/mapping-cheatsheet.md`, `references/deploy-and-validate.md`, `references/gotchas.md`).

- [ ] **Step 1: Write SKILL.md**

Create `SKILL.md` with exactly this content:

````markdown
---
name: matillion-to-databricks
description: Guide for migrating Matillion ETL pipelines to Databricks. Trigger when the user wants to migrate Matillion orchestration (*.orch.yaml) or transformation (*.tran.yaml) jobs to Databricks. Orchestrations become Databricks Jobs; transformations become Lakeflow Declarative Pipelines. Consult the relevant component reference before translating each component.
---

# Matillion → Databricks Migration Guide

An end-to-end workflow for migrating Matillion ETL pipelines to Databricks. Matillion projects are made of two file types:

- `*.orch.yaml` — **orchestration**: a control-flow DAG of steps connected by `transitions`. Becomes a **Databricks Job**.
- `*.tran.yaml` — **transformation**: a dataflow DAG of components connected by `sources`. Becomes a **Lakeflow Declarative Pipeline**.

Consult the component reference (below) **before** translating each component, not after something breaks.

---

## Step 1 — Inventory the Matillion project

Find every pipeline file and map the call graph:

```bash
find . -name '*.orch.yaml' -o -name '*.tran.yaml'
```

- Orchestrations are the entry points.
- For each orchestration, note every `run-transformation` step and which `.tran.yaml` it names (`transformationJob:`). This tells you which transformation feeds which Job task.

Write down: the list of orchestrations, the transformations each one calls, and any transformation not called by any orchestration (a standalone pipeline).

## Step 2 — Parse the orchestration graph

For each `.orch.yaml`, walk `transitions` (`unconditional` / `success` / `failure`) from the `start` component to `end-success`. This control-flow DAG becomes the **Job's task graph** — each transition is a task dependency; `failure:` branches become failure-condition task dependencies.

See:

| Matillion orchestration type | Reference |
|---|---|
| `start` / `end-success` | `references/orchestration/start-end.md` |
| `sql-executor` | `references/orchestration/sql-executor.md` |
| `run-transformation` | `references/orchestration/run-transformation.md` |
| `python-script` | `references/orchestration/python-script.md` |

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
- one **pipeline** resource per `.tran.yaml` (the Lakeflow Declarative Pipeline + its SQL/Python source files),
- one **job** resource per `.orch.yaml`, whose tasks mirror the orchestration graph (SQL tasks for `sql-executor`, a pipeline task for each `run-transformation`, a notebook task for `python-script`).

## Step 6 — Deploy and validate

Use the `fe-databricks-tools:databricks-resource-deployment` skill for all deployment (it handles Lakeflow pipelines + Jobs, prefers serverless, uses `databricks sync`, and UC 3-layer namespaces). Trigger it with: "use the databricks-resource-deployment skill to deploy this bundle".

Then use `fe-databricks-tools:databricks-query` to validate. Follow the checklist in `references/deploy-and-validate.md`.
````

- [ ] **Step 2: Verify frontmatter is valid and all referenced files are listed**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
head -4 SKILL.md
grep -oE 'references/[a-z/-]+\.md' SKILL.md | sort -u
```
Expected: frontmatter shows `name: matillion-to-databricks`; the grep lists exactly these 11 paths — `references/orchestration/start-end.md`, `references/orchestration/sql-executor.md`, `references/orchestration/run-transformation.md`, `references/orchestration/python-script.md`, `references/transformation/table-input.md`, `references/transformation/join.md`, `references/transformation/aggregate.md`, `references/transformation/rewrite-table.md`, `references/mapping-cheatsheet.md`, `references/gotchas.md`, `references/deploy-and-validate.md`.

- [ ] **Step 3: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add SKILL.md
git commit -m "Add matillion-to-databricks SKILL.md workflow"
```

---

### Task 3: Transformation reference — table-input

**Files:**
- Create: `references/transformation/table-input.md`

**Interfaces:**
- Consumes: sample `sales-by-category-region.tran.yaml` (the `Sales`, `Products`, `Regions` components).
- Produces: the `table-input` mapping used by `join.md` (Task 4) as its upstream source pattern.

- [ ] **Step 1: Write the reference**

Create `references/transformation/table-input.md`:

````markdown
# Matillion `table-input` → Lakeflow source read

## What it does in Matillion

Reads a Unity Catalog table. Key parameters:
- `catalog` / `schema` — often `[Environment Default]` (resolve to a real UC catalog/schema).
- `targetTable` — the table name.
- `columnNames` — an **explicit projection**. Only these columns flow downstream.

## Databricks equivalent

A source reference inside the Lakeflow pipeline. Preserve the explicit column list — do **not** use `SELECT *`.

```sql
-- table-input "Sales" reading maia_sample_sales
SELECT sale_id, product_id, region_id, quantity, revenue
FROM my_catalog.my_schema.maia_sample_sales
```

If the source is produced by an upstream pipeline step, reference it as a Lakeflow dataset (e.g. `LIVE.<name>` / a streaming table) instead of a base table.

## Worked example (from sales-by-category-region.tran.yaml)

`Sales`, `Products`, `Regions` are three `table-input` components. Each maps to a `SELECT <columnNames> FROM <catalog>.<schema>.<targetTable>`. These become the source datasets that the `join` components consume.

## Gotchas

- `[Environment Default]` catalog/schema must be replaced with a real UC 3-layer namespace. See `references/gotchas.md`.
- `columnNames` is a whitelist — dropping it silently widens the schema and can break downstream `columnMappings`.
- `offsetType: "None"` means a full read (not incremental). Note it when deciding materialized view vs. streaming table.
````

- [ ] **Step 2: Verify the example traces to the real sample**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "maia_sample_sales" references/transformation/table-input.md && grep -q "SELECT sale_id, product_id, region_id, quantity, revenue" references/transformation/table-input.md && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/transformation/table-input.md
git commit -m "Add table-input transformation reference"
```

---

### Task 4: Transformation reference — join

**Files:**
- Create: `references/transformation/join.md`

**Interfaces:**
- Consumes: `table-input` outputs (Task 3); sample `Join Products` and `Join Regions` components.
- Produces: the joined dataset pattern consumed by `aggregate.md` (Task 5).

- [ ] **Step 1: Write the reference**

Create `references/transformation/join.md`:

````markdown
# Matillion `join` → SQL JOIN

## What it does in Matillion

Joins two or more inputs. Key parameters:
- `sources` — the upstream components (order matters; first is usually the main table).
- `mainTable` / `mainTableAlias` — the driving table and its alias.
- `joins` — list of `[table, alias, joinType]` (e.g. `Inner`, `Left`).
- `joinExpressions` — list of `[predicate, name]`; the predicate is backticked Spark SQL.
- `columnMappings` — list of `[sourceExpr, outputColumn]`; the output projection.

## Databricks equivalent

A SQL `JOIN` inside the pipeline. Aliases, backticked identifiers, and predicates carry over to Spark SQL unchanged. Emit `columnMappings` as the SELECT list.

```sql
-- join "Join Products": Sales (s) INNER JOIN Products (p)
SELECT
  s.sale_id, s.product_id, s.region_id, s.quantity, s.revenue,
  p.product_name, p.category
FROM sales s
INNER JOIN products p ON `s`.`product_id` = `p`.`product_id`
```

Chained joins (a `join` whose `sources` include another `join`) become a CTE chain or a nested pipeline dataset.

## Worked example (from sales-by-category-region.tran.yaml)

- `Join Products`: main `Sales` alias `s` INNER JOIN `Products` alias `p` on `s.product_id = p.product_id`.
- `Join Regions`: main `Join Products` alias `sp` INNER JOIN `Regions` alias `r` on `sp.region_id = r.region_id`.

The two chain: `Join Regions` consumes the output of `Join Products`. In Lakeflow, express as two materialized views or a single view with a CTE.

## Gotchas

- Preserve the exact alias from `mainTableAlias` and each `joins` entry — `columnMappings` reference them (`s.sale_id`, `sp.region_id`).
- `joinExpressions` predicates are already valid Spark SQL (backticked). Copy verbatim.
- The join `columnMappings` may drop columns present upstream (e.g. `Join Regions` drops `region_id` from `sp` and re-takes it from `r`). Follow the mapping exactly.
````

- [ ] **Step 2: Verify**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "Join Products" references/transformation/join.md && grep -q "product_id\` = \`p\`" references/transformation/join.md && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/transformation/join.md
git commit -m "Add join transformation reference"
```

---

### Task 5: Transformation reference — aggregate

**Files:**
- Create: `references/transformation/aggregate.md`

**Interfaces:**
- Consumes: joined dataset (Task 4); sample `Aggregate` component.
- Produces: aggregated dataset consumed by `rewrite-table.md` (Task 6).

- [ ] **Step 1: Write the reference**

Create `references/transformation/aggregate.md`:

````markdown
# Matillion `aggregate` → GROUP BY

## What it does in Matillion

Groups rows and computes aggregates. Key parameters:
- `sources` — the single upstream component.
- `groupings` — the GROUP BY columns.
- `aggregations` — list of `[column, function]` (e.g. `Sum`, `Count`, `Avg`, `Min`, `Max`).

## Databricks equivalent

A SQL `GROUP BY`. Map each `[column, function]` to the SQL aggregate; alias sensibly.

```sql
-- aggregate "Aggregate": group by category, region_name
SELECT
  category,
  region_name,
  SUM(revenue)  AS revenue,
  SUM(quantity) AS quantity,
  COUNT(sale_id) AS sale_id
FROM join_regions
GROUP BY category, region_name
```

## Worked example (from sales-by-category-region.tran.yaml)

`Aggregate` groups the `Join Regions` output by `category`, `region_name` and computes `Sum(revenue)`, `Sum(quantity)`, `Count(sale_id)`.

## Gotchas

- Matillion function names are capitalized (`Sum`, `Count`); map to lowercase SQL funcs (`SUM`, `COUNT`).
- Output column names default to the source column name unless renamed downstream. Keep them stable so the final `rewrite-table-dl` target schema matches.
- Every non-aggregated selected column must appear in `groupings`, or Spark SQL errors.
````

- [ ] **Step 2: Verify**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "GROUP BY category, region_name" references/transformation/aggregate.md && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/transformation/aggregate.md
git commit -m "Add aggregate transformation reference"
```

---

### Task 6: Transformation reference — rewrite-table

**Files:**
- Create: `references/transformation/rewrite-table.md`

**Interfaces:**
- Consumes: aggregated dataset (Task 5); sample `Write Output` component.
- Produces: the target-table pattern (pipeline output) referenced by the DAB assembly in SKILL.md Step 5.

- [ ] **Step 1: Write the reference**

Create `references/transformation/rewrite-table.md`:

````markdown
# Matillion `rewrite-table-dl` → pipeline target table

## What it does in Matillion

Writes (fully replaces) the transformation's output table. Key parameters:
- `sources` — the single upstream component whose rows are written.
- `catalog` / `schema` / `table` — the target location.

"Rewrite" = full overwrite each run (not append/merge).

## Databricks equivalent

The **target dataset** of the Lakeflow pipeline. A full-overwrite rewrite maps to a materialized view (recomputed each run):

```sql
-- rewrite-table-dl "Write Output" → maia_sample_sales_summary
CREATE OR REFRESH MATERIALIZED VIEW my_catalog.my_schema.maia_sample_sales_summary AS
SELECT category, region_name, revenue, quantity, sale_id
FROM aggregate;   -- the upstream aggregate dataset
```

Use a `STREAMING TABLE` instead only if the upstream is append-only and you want incremental processing (the sample's `offsetType: "None"` reads argue for a materialized view).

## Worked example (from sales-by-category-region.tran.yaml)

`Write Output` writes the `Aggregate` result to `maia_sample_sales_summary`. This is the transformation's single output and becomes the pipeline's target materialized view.

## Gotchas

- Resolve `[Environment Default]` to the real target catalog/schema before emitting.
- One `.tran.yaml` typically has one `rewrite-table-dl` = one pipeline target. Multiple write components = multiple targets in the same pipeline.
- "Rewrite" semantics = full refresh. Do not translate to `INSERT INTO` (that would append).
````

- [ ] **Step 2: Verify**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "CREATE OR REFRESH MATERIALIZED VIEW" references/transformation/rewrite-table.md && grep -q "maia_sample_sales_summary" references/transformation/rewrite-table.md && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/transformation/rewrite-table.md
git commit -m "Add rewrite-table transformation reference"
```

---

### Task 7: Orchestration references (start-end, sql-executor, run-transformation, python-script)

**Files:**
- Create: `references/orchestration/start-end.md`
- Create: `references/orchestration/sql-executor.md`
- Create: `references/orchestration/run-transformation.md`
- Create: `references/orchestration/python-script.md`

**Interfaces:**
- Consumes: sample `create-maia-demo-data.orch.yaml` (`Start`, `Dimension Tables`, `Generate Fact Data`, `Run Transformation`, `Create Aggregation Table`, `End Success`).
- Produces: the Job-task mapping for each orchestration component, referenced by SKILL.md Steps 2 & 5.

- [ ] **Step 1: Write start-end.md**

Create `references/orchestration/start-end.md`:

````markdown
# Matillion `start` / `end-success` → Job graph boundaries

## What they do in Matillion

- `start` — the single entry component; its `transitions` name the first real step.
- `end-success` — a terminal marker reached on success.

## Databricks equivalent

No task of their own. They define the Job's boundaries:
- `start`'s transition target is the Job's first task (no `depends_on`).
- `end-success` is implicit — the Job succeeds when all tasks succeed.

## Worked example (from create-maia-demo-data.orch.yaml)

`Start` → (`unconditional`) → `Dimension Tables`. So `Dimension Tables` is the root Job task. `End Success` follows `Create Aggregation Table` and needs no task.

## Gotchas

- Do not emit a Databricks task for `start`/`end-success`.
- If an orchestration has multiple terminal branches, all must complete for the Job to be green.
````

- [ ] **Step 2: Write sql-executor.md**

Create `references/orchestration/sql-executor.md`:

````markdown
# Matillion `sql-executor` → Job SQL task

## What it does in Matillion

Runs a raw SQL script (`sqlScript`) — DDL, seed inserts, or transformations. `scriptLocation: "Component"` means the SQL is inline.

## Databricks equivalent

A **SQL task** in the Job (or a notebook task). The inline `sqlScript` moves into a `.sql` file or notebook cell, run against a SQL warehouse / serverless compute.

```sql
-- sql-executor "Dimension Tables" (DDL + seed)
CREATE OR REPLACE TABLE my_catalog.my_schema.maia_sample_products (
  product_id STRING, product_name STRING, category STRING,
  unit_price DECIMAL(18,2), stock_quantity INTEGER
);
INSERT INTO my_catalog.my_schema.maia_sample_products VALUES ('PROD001', 'Laptop Pro 15', 'Electronics', 1299.99, 45), ...;
```

## Worked example (from create-maia-demo-data.orch.yaml)

- `Dimension Tables`: creates + seeds `maia_sample_products` and `maia_sample_regions`.
- `Generate Fact Data`: `CREATE OR REPLACE TABLE maia_sample_sales AS SELECT ... FROM VALUES (...)`.

Both are **seed/setup** steps, not business transforms — see gotcha below.

## Gotchas

- Seed data (`CREATE OR REPLACE TABLE ... INSERT ... VALUES`) is demo fixture data, **not** a transformation. Keep it as a setup SQL task; do **not** model it as a Lakeflow pipeline table. See `references/gotchas.md`.
- Replace `[Environment Default]` / bare table names with UC 3-layer names.
- Multiple statements in one `sqlScript` are fine in a SQL task; split only if you need per-statement failure handling.
````

- [ ] **Step 3: Write run-transformation.md**

Create `references/orchestration/run-transformation.md`:

````markdown
# Matillion `run-transformation` → pipeline task in the Job

## What it does in Matillion

Invokes a transformation job. Key parameter:
- `transformationJob` — the `.tran.yaml` filename to run.

This is the edge that stitches an orchestration to a transformation.

## Databricks equivalent

A **pipeline task** in the Job that runs the Lakeflow Declarative Pipeline built from that `.tran.yaml`. Its `depends_on` mirrors the incoming `transitions`.

```yaml
# in databricks.yml job tasks
- task_key: run_transformation
  depends_on:
    - task_key: generate_fact_data
  pipeline_task:
    pipeline_id: ${resources.pipelines.sales_by_category_region.id}
```

## Worked example (from create-maia-demo-data.orch.yaml)

`Run Transformation` has `transformationJob: "sales-by-category-region.tran.yaml"` and runs after `Generate Fact Data` (`success`). It becomes a pipeline task depending on the `generate_fact_data` task, pointing at the pipeline built in Tasks 3–6.

## Gotchas

- Match `transformationJob` to the pipeline resource built from that exact `.tran.yaml`.
- The transformation's `table-input` sources must exist before this task runs — ensure the seeding `sql-executor` tasks are upstream in `depends_on`.
- `setScalarVariables` / `setGridVariables` (if populated) become pipeline configuration / parameters.
````

- [ ] **Step 4: Write python-script.md**

Create `references/orchestration/python-script.md`:

````markdown
# Matillion `python-script` → Job notebook task (extract the SQL)

## What it does in Matillion

Runs Python in the Matillion runtime. In practice the Python often just wraps SQL, using Matillion-specific APIs (`context.cursor()`, `subprocess`) that **do not exist** in Databricks.

## Databricks equivalent

A **notebook task** (PySpark) in the Job — or, if it only runs SQL, a SQL task. Extract the real work (usually embedded SQL); discard the Matillion plumbing.

```python
# python-script "Create Aggregation Table" — keep the SQL, drop context.cursor()/subprocess
spark.sql("""
CREATE OR REPLACE TABLE my_catalog.my_schema.maia_sample_category_summary AS
SELECT p.category,
       COUNT(s.sale_id)  AS total_sales,
       SUM(s.quantity)   AS total_quantity,
       SUM(s.revenue)    AS total_revenue,
       AVG(s.revenue)    AS avg_revenue,
       MIN(s.revenue)    AS min_revenue
FROM maia_sample_sales s JOIN maia_sample_products p ON s.product_id = p.product_id
GROUP BY p.category
""")
```

## Worked example (from create-maia-demo-data.orch.yaml)

`Create Aggregation Table` is a `python-script` that builds `maia_sample_category_summary` by running SQL through `context.cursor()`. In Databricks: a notebook task running `spark.sql(...)` with the same SQL, or a plain SQL task. It runs after `Run Transformation` and before `End Success`.

## Gotchas

- `context`, `context.cursor()`, `subprocess`, `interpreter`, `user: "Privileged"` are Matillion-runtime concepts — drop them.
- If the script's real payload is pure SQL, prefer a SQL task over a notebook task (simpler, no cluster).
- The hardcoded catalog in the sample (`marcin_demo.default`) is an environment leak — parameterize it. See `references/gotchas.md`.
````

- [ ] **Step 5: Verify all four files trace to the sample**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "Dimension Tables" references/orchestration/start-end.md && \
grep -q "maia_sample_products" references/orchestration/sql-executor.md && \
grep -q "sales-by-category-region.tran.yaml" references/orchestration/run-transformation.md && \
grep -q "maia_sample_category_summary" references/orchestration/python-script.md && echo OK
```
Expected: `OK`.

- [ ] **Step 6: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/orchestration/
git commit -m "Add orchestration component references"
```

---

### Task 8: Cross-cutting references — cheatsheet, gotchas, deploy-and-validate

**Files:**
- Create: `references/mapping-cheatsheet.md`
- Create: `references/gotchas.md`
- Create: `references/deploy-and-validate.md`

**Interfaces:**
- Consumes: all component references (Tasks 3–7).
- Produces: the fast-lookup index, the gotchas bank, and the Phase 6 handoff — all referenced by SKILL.md.

- [ ] **Step 1: Write mapping-cheatsheet.md**

Create `references/mapping-cheatsheet.md`:

````markdown
# Matillion → Databricks mapping cheatsheet

## File types

| Matillion | Databricks | Detail |
|---|---|---|
| `*.orch.yaml` (orchestration) | Databricks **Job** | `transitions` → task deps |
| `*.tran.yaml` (transformation) | Lakeflow **Declarative Pipeline** | `sources` → dataset chain |

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
| `python-script` | Job notebook/SQL task | `orchestration/python-script.md` |

## Default choices

- Transformation code: **SQL** (`CREATE OR REFRESH MATERIALIZED VIEW`); Python only when SQL can't express it.
- Full-overwrite (`rewrite-table-dl`) → materialized view. Append-only incremental → streaming table.
````

- [ ] **Step 2: Write gotchas.md**

Create `references/gotchas.md`:

````markdown
# Matillion → Databricks migration gotchas

Read before translating any component. Grows as new issues surface.

## `[Environment Default]` catalog/schema placeholders

Matillion resolves `catalog: "[Environment Default]"` / `schema: "[Environment Default]"` from its environment config at runtime. Databricks has no equivalent — you must substitute a real UC 3-layer namespace (`catalog.schema.table`).

Watch for inconsistency: in the samples, `sales-by-category-region.tran.yaml` uses `[Environment Default]` while the `python-script` in `create-maia-demo-data.orch.yaml` hardcodes `marcin_demo.default`. Pick one target catalog/schema and apply it everywhere (ideally as a bundle variable).

## Seed data in `sql-executor` is not a transformation

`CREATE OR REPLACE TABLE ... INSERT INTO ... VALUES (...)` blocks are demo/fixture data. Keep them as a Job setup SQL task. Do **not** model them as Lakeflow pipeline tables — the pipeline should read them as sources, not own them.

## `python-script` uses Matillion-runtime APIs

`context`, `context.cursor()`, `subprocess`, `interpreter: "Python 3"`, `user: "Privileged"` exist only in Matillion. Extract the real payload (usually embedded SQL) and run it via `spark.sql(...)` or a SQL task. Discard the plumbing.

## Backticked identifiers & aliases carry over

Matillion `joinExpressions` predicates (e.g. `` `s`.`product_id` = `p`.`product_id` ``) are already valid Spark SQL. Copy verbatim. Preserve the `mainTableAlias` and per-join aliases — `columnMappings` depend on them.

## Preserve explicit column projections

`table-input.columnNames` and `join.columnMappings` are explicit whitelists. Do not replace with `SELECT *` — downstream steps and the final target schema depend on the exact columns and order.

## Rewrite ≠ append

`rewrite-table-dl` means full overwrite each run. Map to a materialized view (full refresh) or `CREATE OR REPLACE`, never `INSERT INTO` (which appends).
````

- [ ] **Step 3: Write deploy-and-validate.md**

Create `references/deploy-and-validate.md`:

````markdown
# Deploy and validate

## Deploy — delegate to databricks-resource-deployment

Emit a DAB bundle (`databricks.yml`) with the Job + Lakeflow pipeline resources and their source files, then hand off:

> "use the databricks-resource-deployment skill to deploy this bundle"

That skill handles Lakeflow pipelines + Jobs, prefers serverless compute, uses `databricks sync`, and enforces UC 3-layer namespaces. Do not hand-roll deploy commands.

## Validate — delegate to databricks-query

After deploy, use the `fe-databricks-tools:databricks-query` skill to run this checklist:

- [ ] Every target table from each `rewrite-table-dl` and every `sql-executor`/`python-script` output exists.
  ```sql
  SHOW TABLES IN my_catalog.my_schema;
  ```
- [ ] Target tables have a sane row count (not zero, not wildly off from source).
  ```sql
  SELECT COUNT(*) FROM my_catalog.my_schema.maia_sample_sales_summary;
  ```
- [ ] Spot-check one aggregate against the source. For the sample, total revenue must match between source and summary:
  ```sql
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales;          -- source
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales_summary;  -- must equal
  ```
- [ ] The Job ran green end-to-end (all tasks succeeded in the run history).

## Gotcha

Grant the pipeline/job's principal UC access (`USE CATALOG`, `USE SCHEMA`, `SELECT`/`MODIFY`) before the first run, or tasks fail with permission errors. The databricks-resource-deployment skill covers the grant pattern.
````

- [ ] **Step 4: Verify all three files and that the cheatsheet lists every component reference**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
for f in mapping-cheatsheet gotchas deploy-and-validate; do test -f references/$f.md && echo "$f OK"; done
grep -c "transformation/\|orchestration/" references/mapping-cheatsheet.md
```
Expected: three `OK` lines; the grep count is ≥ 8 (all component references linked).

- [ ] **Step 5: Commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
git add references/mapping-cheatsheet.md references/gotchas.md references/deploy-and-validate.md
git commit -m "Add cheatsheet, gotchas, and deploy-and-validate references"
```

---

### Task 9: Final integrity check and directory rename

**Files:**
- Modify: none (verification + rename only)

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces: the finished, correctly-named skill.

- [ ] **Step 1: Verify every SKILL.md reference resolves to a real file**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
missing=0
for ref in $(grep -oE 'references/[a-z/-]+\.md' SKILL.md | sort -u); do
  test -f "$ref" || { echo "MISSING: $ref"; missing=1; }
done
[ $missing -eq 0 ] && echo "ALL REFERENCES RESOLVE"
```
Expected: `ALL REFERENCES RESOLVE`.

- [ ] **Step 2: Verify no Palantir residue and frontmatter name is correct**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads/palantir-to-dbx
grep -q "name: matillion-to-databricks" SKILL.md && echo "NAME OK"
grep -ril "palantir\|foundry\|workshop\|stemma\|HAR" . --include='*.md' | grep -v 'docs/superpowers' || echo "NO PALANTIR RESIDUE"
```
Expected: `NAME OK` and `NO PALANTIR RESIDUE`.

- [ ] **Step 3: Rename the directory**

Run:
```bash
cd /Users/marcin.wojtyczka/Downloads
git -C palantir-to-dbx add -A && git -C palantir-to-dbx commit -m "Final integrity check" --allow-empty
mv palantir-to-dbx matillion-to-databricks
cd matillion-to-databricks && git status && ls
```
Expected: directory is now `matillion-to-databricks`; `git status` clean; `SKILL.md`, `references/`, both sample `.yaml` files, and `docs/` present.

- [ ] **Step 4: Final commit**

```bash
cd /Users/marcin.wojtyczka/Downloads/matillion-to-databricks
git add -A
git commit -m "Rename skill directory to matillion-to-databricks" --allow-empty
git log --oneline
```
Expected: full commit history from Tasks 1–9.

---

## Self-Review

**1. Spec coverage:**
- Replace in place, remove Palantir, rename → Task 1 + Task 9. ✓
- End-to-end workflow (inventory→parse→map→assemble→deploy/validate) → SKILL.md, Task 2. ✓
- Orchestration→Job, transformation→Pipeline hybrid → Task 2 (SKILL.md), Task 7, cheatsheet (Task 8). ✓
- SQL default, Python when needed → stated in SKILL.md Step 4, rewrite-table.md, python-script.md, cheatsheet. ✓
- One reference per component type (Approach A) → Tasks 3–7 (table-input, join, aggregate, rewrite-table, start-end, sql-executor, run-transformation, python-script). ✓
- Cheatsheet, gotchas, deploy-and-validate → Task 8. ✓
- Delegate deploy → databricks-resource-deployment; validate → databricks-query → Task 8 deploy-and-validate.md, SKILL.md Step 6. ✓
- Initial gotchas bank (5 items) → Task 8 gotchas.md (all 5 present). ✓
- Worked examples from the two real samples → every component task references the sample by name. ✓

**2. Placeholder scan:** No TBD/TODO/"handle appropriately". All file content is written out in full. ✓ (The literal `...` inside SQL examples is intentional shorthand for long VALUES lists, clearly marked.)

**3. Type/name consistency:** The 11 reference paths in SKILL.md (Task 2 Step 2) exactly match the files created in Tasks 3–8 and re-verified in Task 9 Step 1. Component type names (`table-input`, `join`, `aggregate`, `rewrite-table-dl`, `start`, `end-success`, `sql-executor`, `run-transformation`, `python-script`) are used identically across SKILL.md, references, and cheatsheet. ✓

No gaps found.
