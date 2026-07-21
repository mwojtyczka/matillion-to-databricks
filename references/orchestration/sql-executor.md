# Matillion `sql-executor` → Job SQL task

## What it does in Matillion

Runs a raw SQL script (`sqlScript`) — DDL, seed inserts, or transformations. `scriptLocation: "Component"` means the SQL is inline.

## Databricks equivalent

A **SQL task** in the Job (or a notebook task). The inline `sqlScript` moves into a `.sql` file or notebook cell, run against a SQL warehouse / serverless compute.

```sql
-- sql-executor "Dimension Tables" (DDL + seed)
CREATE OR REPLACE TABLE my_catalog.my_schema.sample_products (
  product_id STRING, product_name STRING, category STRING,
  unit_price DECIMAL(18,2), stock_quantity INTEGER
);
INSERT INTO my_catalog.my_schema.sample_products VALUES ('PROD001', 'Laptop Pro 15', 'Electronics', 1299.99, 45), ...;
```

## Worked example (from matillion-migration-demo.orch.yaml)

- `Dimension Tables`: creates + seeds `sample_products` and `sample_regions`.
- `Generate Fact Data`: `CREATE OR REPLACE TABLE sample_sales AS SELECT ... FROM VALUES (...)`.

Both are **seed/setup** steps, not business transforms — see gotcha below.

## Gotchas

- Seed data (`CREATE OR REPLACE TABLE ... INSERT ... VALUES`) is demo fixture data, **not** a transformation. Keep it as a setup SQL task; do **not** model it as a Lakeflow pipeline table. See `references/gotchas.md`.
- Replace `[Environment Default]` / bare table names with UC 3-layer names.
- Multiple statements in one `sqlScript` are fine in a SQL task; split only if you need per-statement failure handling.
