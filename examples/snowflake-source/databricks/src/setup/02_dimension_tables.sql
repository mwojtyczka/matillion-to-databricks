-- Converted from Matillion sql-executor step "Create Dimension Tables"
--   Source: matillion/acme_sales.json  (orchestration "Load Sales Summary")
--
-- The source was SNOWFLAKE SQL. Translation applied (see references/snowflake-sql.md):
--   * ${v_e_sales_db}.SALES.DIM_PRODUCTS  ->  unqualified DIM_PRODUCTS
--       The Snowflake database var maps to the UC catalog; schema is SALES. Both come
--       from SQL task parameters via USE ... IDENTIFIER(), so nothing is hardcoded.
--   * PRODUCT_ID::NUMBER   ->  CAST(product_id AS BIGINT)   (:: cast is Snowflake-only)
--   * IFF(cond, a, b)      ->  IF(cond, a, b)               (Databricks has IF)
--   * CURRENT_TIMESTAMP()  ->  current_timestamp()
--   * GRANT ... TO ROLE    ->  dropped from the transform; UC grants are managed
--                              separately (GRANT ... TO `group`), not inside an ETL step.
--                              See references/snowflake-sql.md -> "GRANT".
--
-- As the Job's first task, this also ensures the target schema exists so the Job runs
-- against a fresh catalog. (Source tables products_src/regions_src/sales_src must already
-- exist — populate them with src/setup/00_generate_source_data.py for a standalone test,
-- or point at the real RAW.* tables in production.)

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE SCHEMA IDENTIFIER(:schema);

CREATE OR REPLACE TABLE dim_products AS
SELECT
  CAST(product_id AS BIGINT)               AS product_id,
  product_name,
  IF(category IS NULL, 'UNKNOWN', category) AS category,
  current_timestamp()                       AS loaded_at
FROM products_src;

CREATE OR REPLACE TABLE dim_regions AS
SELECT CAST(region_id AS BIGINT) AS region_id, region_name
FROM regions_src;
