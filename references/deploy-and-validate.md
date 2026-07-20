# Deploy and validate

## Deploy — delegate to databricks-resource-deployment

Emit a DAB bundle (`databricks.yml`) with the Job + Lakeflow pipeline resources and their source files, then hand off:

> "use the databricks-resource-deployment skill to deploy this bundle"

That skill handles Lakeflow pipelines + Jobs, prefers serverless compute, uses `databricks sync`, and enforces UC 3-layer namespaces. Do not hand-roll deploy commands.

## Validate — delegate to databricks-query

After deploy, use the `fe-databricks-tools:databricks-query` skill to run this checklist:

- [ ] Every target table from each `rewrite-table-dl` and every `sql-executor`/`python-script` output exists.
  ```sql
  SHOW TABLES IN my_catalog.my_schema;
  ```
- [ ] Target tables have a sane row count (not zero, not wildly off from source).
  ```sql
  SELECT COUNT(*) FROM my_catalog.my_schema.maia_sample_sales_summary;
  ```
- [ ] Spot-check one aggregate against the source. For the sample, total revenue must match between source and summary:
  ```sql
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales;          -- source
  SELECT SUM(revenue) FROM my_catalog.my_schema.maia_sample_sales_summary;  -- must equal
  ```
- [ ] The Job ran green end-to-end (all tasks succeeded in the run history).

## Gotcha

Grant the pipeline/job's principal UC access (`USE CATALOG`, `USE SCHEMA`, `SELECT`/`MODIFY`) before the first run, or tasks fail with permission errors. The databricks-resource-deployment skill covers the grant pattern.
