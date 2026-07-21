# Matillion `rewrite-table-dl` → the transformation's target table

## What it does in Matillion

Writes (fully replaces) the transformation's output table. Key parameters:
- `sources` — the single upstream component whose rows are written.
- `catalog` / `schema` / `table` — the target location.

"Rewrite" = full overwrite each run (not append/merge).

## Databricks equivalent

The transformation's **output table**. "Rewrite" = full overwrite, which maps to:
- **SQL task (default):** `CREATE OR REPLACE TABLE catalog.schema.table AS SELECT ...`
- **Lakeflow pipeline (only if the executor ladder lands there):** `CREATE OR REFRESH MATERIALIZED VIEW ... AS SELECT ...`, recomputed each run. Use a `STREAMING TABLE` instead only if the upstream is append-only and you want incremental processing (the sample's `offsetType: "None"` reads argue for full refresh, i.e. a plain table / MV).

Which one you emit is the executor decision in `references/orchestration/run-transformation.md` — most transformations are a SQL task. **Never** map "rewrite" to `INSERT INTO` (that appends).

## Consolidate the chain — one query, not one dataset per component

This is the key transformation decision. The upstream `table-input` / `join` / `aggregate` components are **not** each a separate table/view. A 1:1 mapping is faithful to Matillion but materializes every intermediate as its own object — recomputed on every run (and in Lakeflow, storage-backed). For a linear chain producing one output, that is pure waste.

**Default: collapse the whole chain into the single target statement, using CTEs for the intermediate components.** In a SQL task that's one `CREATE OR REPLACE TABLE ... AS WITH ... SELECT`; in Lakeflow it's one `CREATE OR REFRESH MATERIALIZED VIEW`. Below the example is written for Lakeflow, but the CTE body is identical in a SQL task — only the leading DDL differs.

**Default: collapse a linear chain that yields a single `rewrite-table-dl` output into ONE materialized view, using CTEs for the intermediate components.** Identical result, one object to store and refresh instead of N.

```sql
-- The whole sales-by-category-region.tran.yaml chain as a single target MV.
CREATE OR REFRESH MATERIALIZED VIEW my_catalog.my_schema.maia_sample_sales_summary AS
WITH join_products AS (        -- join "Join Products"
  SELECT s.sale_id, s.product_id, s.region_id, s.quantity, s.revenue,
         p.product_name, p.category
  FROM my_catalog.my_schema.maia_sample_sales s
  INNER JOIN my_catalog.my_schema.maia_sample_products p
    ON `s`.`product_id` = `p`.`product_id`
),
join_regions AS (              -- join "Join Regions"
  SELECT sp.sale_id, sp.quantity, sp.revenue, sp.category, r.region_name
  FROM join_products sp
  INNER JOIN my_catalog.my_schema.maia_sample_regions r
    ON `sp`.`region_id` = `r`.`region_id`
)
SELECT category, region_name,  -- aggregate "Aggregate"
       SUM(revenue) AS revenue, SUM(quantity) AS quantity, COUNT(sale_id) AS sale_id
FROM join_regions
GROUP BY category, region_name;
```

**Keep a component as its own materialized view only when it earns it:**
- it is **reused** — more than one downstream dataset reads it (materialize once, not per-consumer),
- it needs its own **data-quality expectations** (`EXPECT`) or independent monitoring, or
- it is a genuine **branch/fan-out point** in the DAG (not a link in a linear chain).

**Middle ground — lineage without the storage cost:** declare the intermediates as non-materialized `VIEW`s (`CREATE OR REFRESH VIEW`) and materialize only the target. You keep per-step nodes in the pipeline graph and can inspect them, but Lakeflow doesn't persist them.

> Migration tip: during initial cutover it's fine to emit the 1:1 mapping so each dataset cross-references its Matillion component for validation. Once outputs are reconciled against the source, consolidate. **Note:** Lakeflow does not drop datasets you remove from a pipeline — after consolidating, manually `DROP` the now-orphaned intermediate MVs or they linger in the schema.

## Worked example (from sales-by-category-region.tran.yaml)

`Write Output` writes the `Aggregate` result to `maia_sample_sales_summary` — the transformation's single output. The chain (`Sales`/`Products`/`Regions` → `Join Products` → `Join Regions` → `Aggregate` → `Write Output`) is linear and yields just this one table, so the reference implementation (`examples/demo/databricks/src/pipelines/sales_by_category_region.sql`) consolidates it into the **single** MV above rather than seven. None of the intermediates are reused or carry expectations, so materializing them separately would only add storage and refresh cost.

## Gotchas

- Resolve `[Environment Default]` to the real target catalog/schema before emitting.
- One `.tran.yaml` typically has one `rewrite-table-dl` = one pipeline target. Multiple write components = multiple targets in the same pipeline.
- "Rewrite" semantics = full refresh. Do not translate to `INSERT INTO` (that would append).
