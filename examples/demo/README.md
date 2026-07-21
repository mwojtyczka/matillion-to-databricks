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
   │  ├─ job.yml            (Job built from the orchestration pipeline)
   │  └─ pipelines.yml      (Lakeflow pipeline built from the transformation pipeline)
   └─ src/
      ├─ setup/             (sql-executor steps -> Job SQL tasks)
      │  ├─ 01_dimension_tables.sql
      │  └─ 02_generate_fact_data.sql
      ├─ pipelines/
      │  └─ sales_by_category_region.sql   (the Lakeflow transformation)
      └─ notebooks/
         └─ create_aggregation_table.py    (python-script step -> notebook task)
```

## What maps to what

| Matillion (before) | Databricks (after) | Why |
|---|---|---|
| `create-maia-demo-data.orch.yaml` (orchestration) | **Job** `maia_demo_job` (`resources/job.yml`) | Control flow → Job |
| `Start` / `End Success` | *(no task — graph boundaries)* | Boundaries carry no work |
| `Dimension Tables` (`sql-executor`) | SQL task → `src/setup/01_dimension_tables.sql` | Seed/DDL is setup, not dataflow |
| `Generate Fact Data` (`sql-executor`) | SQL task → `src/setup/02_generate_fact_data.sql` | Seed/DDL setup |
| `Run Transformation` (`run-transformation`) | **pipeline task** → the Lakeflow pipeline | Invokes the transformation |
| `Create Aggregation Table` (`python-script`) | notebook task → `src/notebooks/create_aggregation_table.py` | Side-effecting SQL; plumbing dropped |
| `sales-by-category-region.tran.yaml` (transformation) | **Lakeflow pipeline** (`resources/pipelines.yml` + `src/pipelines/…sql`) | Pure dataflow → declarative pipeline |
| `table-input` / `join` / `aggregate` / `rewrite-table-dl` | materialized views chained in `src/pipelines/sales_by_category_region.sql` | Each component → one MV |

The Job is the outer shell; the transformation runs **inside** it as a pipeline task —
the composition pattern the skill's decision guide describes.

## Conversions worth noting

- **`[Environment Default]` → real UC names.** The Matillion source left catalog/schema
  as `[Environment Default]`; the output resolves them to `main.matillion_demo`,
  parameterized as bundle variables (`${var.catalog}` / `${var.schema}`).
- **python-script plumbing removed.** The original used `context.cursor()` / `subprocess`
  (Matillion-runtime only). The notebook keeps just the SQL, run via `spark.sql(...)`,
  and parameterizes the hardcoded `marcin_demo.default` that leaked into the script.
- **Rewrite = full overwrite** → materialized view (not `INSERT INTO`).
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
