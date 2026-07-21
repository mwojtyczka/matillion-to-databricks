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

Chained joins (a `join` whose `sources` include another `join`) become a **CTE chain** by default — each join is one CTE feeding the next, all inside the target MV. Promote a join to its own materialized view only if it's reused, branches, or needs expectations (see `references/transformation/rewrite-table.md` → "Consolidate the chain").

## Worked example (from sales-by-category-region.tran.yaml)

- `Join Products`: main `Sales` alias `s` INNER JOIN `Products` alias `p` on `s.product_id = p.product_id`.
- `Join Regions`: main `Join Products` alias `sp` INNER JOIN `Regions` alias `r` on `sp.region_id = r.region_id`.

The two chain: `Join Regions` consumes the output of `Join Products`. In the reference implementation both are CTEs (`join_products`, `join_regions`) inside the single target MV — not two separate materialized views.

## Gotchas

- Preserve the exact alias from `mainTableAlias` and each `joins` entry — `columnMappings` reference them (`s.sale_id`, `sp.region_id`).
- `joinExpressions` predicates are already valid Spark SQL (backticked). Copy verbatim.
- The join `columnMappings` may drop columns present upstream (e.g. `Join Regions` drops `region_id` from `sp` and re-takes it from `r`). Follow the mapping exactly.
