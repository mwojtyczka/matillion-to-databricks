# Matillion `table-input` → Lakeflow source read

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

If the source is produced by an upstream pipeline step, reference it as a Lakeflow dataset (e.g. `LIVE.<name>` / a streaming table) instead of a base table.

## Worked example (from sales-by-category-region.tran.yaml)

`Sales`, `Products`, `Regions` are three `table-input` components. Each maps to a `SELECT <columnNames> FROM <catalog>.<schema>.<targetTable>`. These become the source datasets that the `join` components consume.

## Gotchas

- `[Environment Default]` catalog/schema must be replaced with a real UC 3-layer namespace. See `references/gotchas.md`.
- `columnNames` is a whitelist — dropping it silently widens the schema and can break downstream `columnMappings`.
- `offsetType: "None"` means a full read (not incremental). Note it when deciding materialized view vs. streaming table.
