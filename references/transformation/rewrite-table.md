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
