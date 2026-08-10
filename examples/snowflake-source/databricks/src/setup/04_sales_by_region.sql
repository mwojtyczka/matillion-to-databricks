-- Converted from the Matillion transformation job "sales_by_region"
--   Source: matillion/acme_sales.json  (transformationJobs[0])
--
-- In the classic JSON format a transformation is a graph of components wired by the
-- `connectors` list (not `sources`/`transitions`). Here the graph is:
--
--   table-input "FACT_SALES" (335239555)  ┐
--   table-input "DIM_REGIONS" (335239555) ┴─> join "Join Regions" (-629958239)
--       -> aggregate "Aggregate By Region" (1701703136)
--       -> table-output "SALES_BY_REGION" (1354890871)
--
-- It's a linear chain producing one full-refresh output with no Lakeflow features, so
-- per the skill's ladder it is a single SQL task (not a Lakeflow pipeline). The chain is
-- consolidated into one CREATE OR REPLACE TABLE with a CTE for the join; each CTE names
-- its originating Matillion component.
--
-- The join expression came from Snowflake with double-quoted UPPERCASE identifiers
-- ("s"."REGION_ID" = "r"."REGION_ID"). Databricks uses backticks; column refs are
-- case-insensitive, so we lower-case for readability. Target namespace via parameters.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE OR REPLACE TABLE sales_by_region AS
WITH join_regions AS (
  -- join "Join Regions": FACT_SALES (s) INNER JOIN DIM_REGIONS (r)
  SELECT
    s.sale_id,
    s.revenue,
    s.quantity,
    r.region_name
  FROM fact_sales s
  INNER JOIN dim_regions r
    ON `s`.`region_id` = `r`.`region_id`
)
-- aggregate "Aggregate By Region": group by region_name
SELECT
  region_name,
  SUM(revenue)   AS revenue,
  COUNT(sale_id) AS sale_id
FROM join_regions
GROUP BY region_name;
