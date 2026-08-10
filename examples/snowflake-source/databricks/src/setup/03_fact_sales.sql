-- Converted from Matillion sql-executor step "Load Fact Data"
--   Source: matillion/acme_sales.json  (orchestration "Load Sales Summary")
--
-- Snowflake -> Databricks translation (see references/snowflake-sql.md):
--   * ::NUMBER / ::FLOAT casts    ->  CAST(... AS BIGINT) / CAST(... AS DOUBLE)
--   * QUALIFY ROW_NUMBER() OVER (...) = 1  ->  Databricks SQL supports QUALIFY directly,
--       so this dedupe carries over unchanged (keep the latest row per sale_id).
--   * ${v_e_sales_db}.RAW.SALES_SRC  ->  unqualified sales_src (namespace via parameters)

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- Keep region_id (and the other keys) in the projection: the downstream transform
-- 04_sales_by_region.sql joins fact_sales on region_id, so this table must expose it.
CREATE OR REPLACE TABLE fact_sales AS
SELECT
  CAST(sale_id AS BIGINT)     AS sale_id,
  CAST(product_id AS BIGINT)  AS product_id,
  CAST(region_id AS BIGINT)   AS region_id,   -- join key for 04_sales_by_region.sql
  CAST(quantity AS BIGINT)    AS quantity,
  CAST(revenue AS DOUBLE)     AS revenue
FROM sales_src
QUALIFY ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY loaded_at DESC) = 1;
