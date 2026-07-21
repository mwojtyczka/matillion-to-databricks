# Matillion `run-transformation` → a task in the Job

## What it does in Matillion

Invokes a transformation pipeline. Key parameter:
- `transformationJob` — the `.tran.yaml` filename to run.

This is the edge that stitches an orchestration to a transformation.

## Databricks equivalent — pick the executor (ladder)

`run-transformation` becomes a **task in the Job**, with `depends_on` mirroring the incoming `transitions`. The transformation is *dataflow*, so it never itself carries control flow — but it does **not** default to a Lakeflow pipeline. Walk this ladder top-down and stop at the first match:

1. **Pure SQL, batch / full-refresh (the common case)** → **SQL task.** Consolidate the `.tran.yaml` chain (`table-input` → `join` → `aggregate` → `rewrite-table-dl`) into one `CREATE OR REPLACE TABLE ... AS SELECT` with CTEs, and run it on the SQL warehouse. No separate resource, no cluster.

   ```yaml
   - task_key: run_transformation
     depends_on:
       - task_key: generate_fact_data
     sql_task:
       warehouse_id: ${var.warehouse_id}
       file:
         path: ../src/setup/03_sales_by_category_region.sql
   ```

2. **Needs Python / imperative glue, or is too tangled for one clean query** → **notebook task** running `spark.sql(...)`.

3. **Genuinely needs incremental/streaming, CDC, managed data-quality expectations, or multi-output lineage** → **Lakeflow pipeline** (pipeline task). The escape hatch, not the default.

   ```yaml
   - task_key: run_transformation
     depends_on:
       - task_key: generate_fact_data
     pipeline_task:
       pipeline_id: ${resources.pipelines.sales_by_category_region.id}
   ```

## Why Lakeflow is not the default

A Lakeflow pipeline is a separate resource with its own compute lifecycle and deploy surface. It pays off only when you use what it provides: incremental MV maintenance (enzyme), streaming tables, `EXPECT` data-quality rules, and automatic multi-table lineage. A single full-refresh transform uses **none** of those — wrapping it in a pipeline is a SQL task plus overhead. Match the tool to the features you actually need:

| Signal in the `.tran.yaml` | Executor |
|---|---|
| Full read (`offsetType: "None"`), single output, no expectations | **SQL task** |
| Imperative logic / Python / external calls mixed in | **notebook task** |
| Append-only/CDC source you want processed incrementally | **Lakeflow** (or notebook Structured Streaming) |
| Want managed checkpoints + expectations + lineage | **Lakeflow** |

Even for streaming, a notebook running Structured Streaming is often simpler for a first migration; choose Lakeflow specifically when you want it to *manage* the streaming state/quality/lineage for you.

## Worked example (from create-maia-demo-data.orch.yaml)

`Run Transformation` has `transformationJob: "sales-by-category-region.tran.yaml"` and runs after `Generate Fact Data` (`success`). That transformation is a linear chain producing one full-refresh table with no expectations — so the reference bundle (`examples/demo/databricks/`) implements it as a **SQL task** (`src/setup/03_sales_by_category_region.sql`), not a Lakeflow pipeline. Its `depends_on` is `generate_fact_data`.

## Gotchas

- The transformation's source tables must exist before this task runs — ensure the seeding `sql-executor` tasks are upstream in `depends_on`.
- `setScalarVariables` / `setGridVariables` (if populated) become job parameters (SQL/notebook task) or pipeline configuration (Lakeflow).
- Don't reflexively emit a pipeline resource "because it's a transformation." Emit one only when the ladder lands on Lakeflow — most migrations emit none.
- Whatever the executor, keep it as its **own task** in the Job graph — don't fold the transform into an upstream task and lose per-step retry/observability.
