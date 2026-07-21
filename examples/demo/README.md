# Demo: Matillion → Databricks (before / after)

A complete worked example — the same pipeline shown as Matillion source and as the
converted Databricks output the skill produces.

```
demo/
├─ matillion/     ← BEFORE: the original Matillion pipelines
│  ├─ create-maia-demo-data.orch.yaml     (orchestration pipeline)
│  └─ sales-by-category-region.tran.yaml  (transformation pipeline)
└─ databricks/    ← AFTER: the converted Databricks Asset Bundle
   ├─ databricks.yml
   ├─ resources/
   │  └─ job.yml            (Job built from the orchestration pipeline — no pipeline resource needed)
   └─ src/
      ├─ setup/             (sql-executor + the transformation -> Job SQL tasks)
      │  ├─ 01_dimension_tables.sql
      │  ├─ 02_generate_fact_data.sql
      │  └─ 03_sales_by_category_region.sql   (the transformation, consolidated into one SQL task)
      └─ notebooks/
         └─ create_aggregation_table.py    (python-script step -> notebook task)
```

> **Note on the transformation:** this transformation is pure full-refresh SQL with a
> single output — it uses none of Lakeflow's features (incremental/streaming,
> data-quality expectations, multi-output lineage). Per the skill's executor ladder it
> is therefore a plain **SQL task**, not a Lakeflow pipeline, so this bundle has **no
> pipeline resource at all**. Lakeflow is the escape hatch, used only when those
> features are actually needed.

## What maps to what

| Matillion (before) | Databricks (after) | Why |
|---|---|---|
| `create-maia-demo-data.orch.yaml` (orchestration) | **Job** `maia_demo_job` (`resources/job.yml`) | Control flow → Job |
| `Start` / `End Success` | *(no task — graph boundaries)* | Boundaries carry no work |
| `Dimension Tables` (`sql-executor`) | SQL task → `src/setup/01_dimension_tables.sql` | Seed/DDL is setup, not dataflow |
| `Generate Fact Data` (`sql-executor`) | SQL task → `src/setup/02_generate_fact_data.sql` | Seed/DDL setup |
| `Run Transformation` (`run-transformation`) | **SQL task** → `src/setup/03_sales_by_category_region.sql` | Pure full-refresh SQL, single output → SQL task (not Lakeflow) |
| `Create Aggregation Table` (`python-script`) | notebook task → `src/notebooks/create_aggregation_table.py` | Side-effecting SQL; plumbing dropped |
| `sales-by-category-region.tran.yaml` (transformation) | one **SQL task** (`src/setup/03_sales_by_category_region.sql`) | Dataflow with no Lakeflow features → a task, not a pipeline |
| `table-input` / `join` / `aggregate` / `rewrite-table-dl` | one consolidated query (CTEs) in that SQL file | Whole chain → one `CREATE OR REPLACE TABLE`, not one dataset per component |

The Job is the outer shell; the transformation runs **inside** it as a task. Picking
*which* task type (SQL here) is the executor decision the skill's ladder describes —
Lakeflow would only be used if the transformation needed incremental/streaming or
managed data-quality.

## Conversions worth noting

- **`[Environment Default]` → real UC names.** The Matillion source left catalog/schema
  as `[Environment Default]`; the output resolves them to `main.matillion_demo`,
  parameterized as bundle variables (`${var.catalog}` / `${var.schema}`).
- **python-script plumbing removed.** The original used `context.cursor()` / `subprocess`
  (Matillion-runtime only). The notebook keeps just the SQL, run via `spark.sql(...)`,
  and parameterizes the hardcoded `marcin_demo.default` that leaked into the script.
- **Rewrite = full overwrite** → `CREATE OR REPLACE TABLE` (not `INSERT INTO`); it would
  be a materialized view only if this ran as a Lakeflow pipeline.
- **Whole transformation consolidated** into one query — the `join`/`aggregate`
  components are CTEs feeding a single `CREATE OR REPLACE TABLE`, not seven separate
  datasets.
- **Explicit column projections** from `columnNames` / `columnMappings` are preserved
  (no `SELECT *`).

## Try it yourself

Point the skill at `matillion/` and compare its output to `databricks/`:

> "Using the matillion-to-databricks skill, convert the pipelines in
> `examples/demo/matillion/`."

## Deploy (optional)

The bundle targets serverless and Unity Catalog. Edit `databricks.yml` (`workspace.host`,
the `catalog`/`schema`/`warehouse_id` variables), then:

```bash
cd databricks
databricks bundle deploy -t dev
databricks bundle run maia_demo_job -t dev
```

> This is illustrative output for learning the mapping. Review and adjust
> (warehouse ID, catalog/schema, host) before running against a real workspace.
