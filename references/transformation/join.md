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

Chained joins (a `join` whose `sources` include another `join`) become a **CTE chain** by default — each join is one CTE feeding the next, all inside the target MV. Promote a join to its own materialized view only if it's reused, branches, or needs its own quality gate (see `references/transformation/rewrite-table.md` → "Consolidate the chain", and `references/data-quality.md` for DQX).

## Worked example (from sales-by-category-region.tran.yaml)

- `Join Products`: main `Sales` alias `s` INNER JOIN `Products` alias `p` on `s.product_id = p.product_id`.
- `Join Regions`: main `Join Products` alias `sp` INNER JOIN `Regions` alias `r` on `sp.region_id = r.region_id`.

The two chain: `Join Regions` consumes the output of `Join Products`. In the reference implementation both are CTEs (`join_products`, `join_regions`) inside the single target MV — not two separate materialized views.

## Gotchas

- Preserve the exact alias from `mainTableAlias` and each `joins` entry — `columnMappings` reference them (`s.sale_id`, `sp.region_id`).
- `joinExpressions` predicates are already valid Spark SQL (backticked). Copy verbatim.
- The join `columnMappings` may drop columns present upstream (e.g. `Join Regions` drops `region_id` from `sp` and re-takes it from `r`). Follow the mapping exactly.

## Matillion "Unite" (UNION) — align the schema across all inputs

A Matillion Unite component maps input columns to a **common output schema**, so each input contributes the same columns. When it's translated to `UNION ALL`, **Spark matches columns by position**, so every branch must select the **same columns in the same order** — a common failure is each branch exposing only *its own* metric column (e.g. one `… AS RISKS`, another `… AS AUDITS`); the union then keeps only the first branch's name and downstream `SUM(\`AUDITS\`)` fails with `UNRESOLVED_COLUMN`. Fix: give **every** branch the full column set — its own value populated and the others as `CAST(0 AS BIGINT)` (or `NULL`) — so all branches are union-compatible:

```sql
-- each branch exposes ALL metric columns; only its own is non-zero
SELECT affected_unit, region, site, count_x AS RISKS, CAST(0 AS BIGINT) AS AUDITS, … FROM risks_by_unit
UNION ALL
SELECT affected_unit, region, site, CAST(0 AS BIGINT) AS RISKS, count_x AS AUDITS, … FROM audits_by_unit
```
