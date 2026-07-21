-- Converted from the Matillion transformation pipeline
--   matillion/sales-by-category-region.tran.yaml
--
-- This is pure, full-refresh SQL producing a single output table. It uses none of
-- Lakeflow's features (no incremental/streaming, no data-quality expectations, no
-- reused intermediates, single target), so per the skill's ladder it is a plain
-- Job SQL task, not a Lakeflow pipeline. The linear component chain is consolidated
-- into one query via CTEs; each CTE names its originating Matillion component.
--
--   table-input (Sales / Products / Regions)   -> source reads, inlined
--   join  "Join Products"                       -> CTE join_products
--   join  "Join Regions"                        -> CTE join_regions
--   aggregate "Aggregate"                        -> the final GROUP BY
--   rewrite-table-dl "Write Output"              -> sample_sales_summary target
--
-- "Rewrite" = full overwrite each run -> CREATE OR REPLACE TABLE.
--
-- Target catalog/schema are NOT hardcoded: they arrive as SQL task parameters
-- (:catalog / :schema, sourced from the bundle variables in databricks.yml) and are
-- applied via USE ... IDENTIFIER(), so every table below is referenced unqualified.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE OR REPLACE TABLE sample_sales_summary AS
WITH join_products AS (
  -- join "Join Products": Sales (s) INNER JOIN Products (p)
  SELECT
    s.sale_id,
    s.product_id,
    s.region_id,
    s.quantity,
    s.revenue,
    p.product_name,
    p.category
  FROM sample_sales s
  INNER JOIN sample_products p
    ON `s`.`product_id` = `p`.`product_id`
),
join_regions AS (
  -- join "Join Regions": Join Products (sp) INNER JOIN Regions (r)
  SELECT
    sp.sale_id,
    sp.quantity,
    sp.revenue,
    sp.category,
    r.region_name
  FROM join_products sp
  INNER JOIN sample_regions r
    ON `sp`.`region_id` = `r`.`region_id`
)
-- aggregate "Aggregate": group by category, region_name
SELECT
  category,
  region_name,
  SUM(revenue)   AS revenue,
  SUM(quantity)  AS quantity,
  COUNT(sale_id) AS sale_id
FROM join_regions
GROUP BY category, region_name;
