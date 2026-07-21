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

`Aggregate` groups the `Join Regions` output by `category`, `region_name` and computes `Sum(revenue)`, `Sum(quantity)`, `Count(sale_id)`. Since it feeds straight into the single `rewrite-table-dl` target, the reference implementation makes this `GROUP BY` the **final SELECT of the target MV** (consuming the `join_regions` CTE) rather than a standalone `mv_aggregate`. See `references/transformation/rewrite-table.md` → "Consolidate the chain".

## Gotchas

- Matillion function names are capitalized (`Sum`, `Count`); map to lowercase SQL funcs (`SUM`, `COUNT`).
- Output column names default to the source column name unless renamed downstream. Keep them stable so the final `rewrite-table-dl` target schema matches.
- Every non-aggregated selected column must appear in `groupings`, or Spark SQL errors.
