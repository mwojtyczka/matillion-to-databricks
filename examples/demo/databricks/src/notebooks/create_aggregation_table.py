# Databricks notebook source
# Converted from the Matillion python-script step "Create Aggregation Table"
#   Source: matillion/matillion-migration-demo.orch.yaml
#
# The original wrapped SQL in Matillion-runtime plumbing (`context.cursor()`,
# `subprocess`, `import json`) that does not exist in Databricks. Per the skill's
# python-script rule we keep the real payload (the embedded SQL) and run it via
# spark.sql(), discarding the plumbing.
#
# It is a side-effecting CREATE-from-JOIN (not part of the declarative dataflow),
# so it stays a Job task rather than moving into the Lakeflow pipeline.
#
# The Matillion script hardcoded `marcin_demo.default`; that environment leak is
# parameterized here via job parameters (see resources/job.yml).

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "matillion_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.sample_category_summary AS
SELECT
  p.category,
  COUNT(s.sale_id) AS total_sales,
  SUM(s.quantity) AS total_quantity,
  SUM(s.revenue) AS total_revenue,
  AVG(s.revenue) AS avg_revenue,
  MIN(s.revenue) AS min_revenue,
  MAX(s.revenue) AS max_revenue
FROM {catalog}.{schema}.sample_sales s
JOIN {catalog}.{schema}.sample_products p
  ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
""")

print(f"Successfully created {catalog}.{schema}.sample_category_summary table")
