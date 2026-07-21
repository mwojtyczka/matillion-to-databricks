# Matillion `table-input` → source read (a `FROM`)

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

`table-input` reads a real UC table, so it becomes a `FROM my_catalog.my_schema.<targetTable>` — usually **inlined** into the consuming query (a CTE or the source of a `JOIN`), not its own materialized view. A bare projection over a source table earns nothing by being materialized. Only promote it to its own dataset if it's reused by several downstream datasets or needs its own expectations (see `references/transformation/rewrite-table.md` → "Consolidate the chain"). If the source is instead produced by an upstream pipeline step, reference that step by its plain dataset name. (`LIVE.<name>` is legacy-compatible DLT syntax; current Lakeflow SQL can use the plain dataset name directly.)

## Worked example (from sales-by-category-region.tran.yaml)

`Sales`, `Products`, `Regions` are three `table-input` components. Each maps to a `FROM <catalog>.<schema>.<targetTable>` with the `columnNames` applied as the projection. In the consolidated reference implementation these are inlined directly into the `JOIN`s (their explicit column lists become the CTE SELECT lists) rather than becoming three standalone materialized views.

## Gotchas

- `[Environment Default]` catalog/schema must be replaced with a real UC 3-layer namespace. See `references/gotchas.md`.
- `columnNames` is a whitelist — dropping it silently widens the schema and can break downstream `columnMappings`.
- `offsetType: "None"` means a full read (not incremental). Note it when deciding materialized view vs. streaming table.
