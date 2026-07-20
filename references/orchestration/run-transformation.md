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
