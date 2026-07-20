# Matillion `python-script` → Job notebook task (extract the SQL)

## What it does in Matillion

Runs Python in the Matillion runtime. In practice the Python often just wraps SQL, using Matillion-specific APIs (`context.cursor()`, `subprocess`) that **do not exist** in Databricks.

## Databricks equivalent

A **notebook task** (PySpark) in the Job — or, if it only runs SQL, a SQL task. Extract the real work (usually embedded SQL); discard the Matillion plumbing.

```python
# python-script "Create Aggregation Table" — keep the SQL, drop context.cursor()/subprocess
spark.sql("""
CREATE OR REPLACE TABLE my_catalog.my_schema.maia_sample_category_summary AS
SELECT p.category,
       COUNT(s.sale_id)  AS total_sales,
       SUM(s.quantity)   AS total_quantity,
       SUM(s.revenue)    AS total_revenue,
       AVG(s.revenue)    AS avg_revenue,
       MIN(s.revenue)    AS min_revenue,
       MAX(s.revenue)    AS max_revenue
FROM maia_sample_sales s JOIN maia_sample_products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
""")
```

## Worked example (from create-maia-demo-data.orch.yaml)

`Create Aggregation Table` is a `python-script` that builds `maia_sample_category_summary` by running SQL through `context.cursor()`. In Databricks: a notebook task running `spark.sql(...)` with the same SQL, or a plain SQL task. It runs after `Run Transformation` and before `End Success`.

## Gotchas

- `context`, `context.cursor()`, `subprocess`, `interpreter`, `user: "Privileged"` are Matillion-runtime concepts — drop them.
- If the script's real payload is pure SQL, prefer a SQL task over a notebook task (simpler, no cluster).
- The hardcoded catalog in the sample (`marcin_demo.default`) is an environment leak — parameterize it. See `references/gotchas.md`.
