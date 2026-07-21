-- Lakeflow Declarative Pipeline converted from the Matillion transformation pipeline
--   matillion/sales-by-category-region.tran.yaml
--
-- The transformation is pure table-to-table dataflow, so it becomes a Lakeflow
-- pipeline (declarative, auto dependency + lineage). Each Matillion component
-- becomes one materialized view; downstream components reference the upstream
-- view by name, mirroring the .tran.yaml `sources` graph:
--
--   table-input (Sales / Products / Regions)   -> source SELECTs below
--   join  "Join Products"                       -> mv_join_products
--   join  "Join Regions"                        -> mv_join_regions
--   aggregate "Aggregate"                        -> mv_aggregate
--   rewrite-table-dl "Write Output"              -> maia_sample_sales_summary (target)
--
-- Matillion [Environment Default] is resolved via the pipeline's configured
-- catalog/schema (set in resources/pipelines.yml), so tables are referenced
-- unqualified here and Lakeflow places them in the target schema.

-- table-input "Sales" — explicit projection preserved from columnNames
CREATE OR REFRESH MATERIALIZED VIEW mv_sales AS
SELECT sale_id, product_id, region_id, quantity, revenue
FROM main.matillion_demo.maia_sample_sales;

-- table-input "Products"
CREATE OR REFRESH MATERIALIZED VIEW mv_products AS
SELECT product_id, product_name, category
FROM main.matillion_demo.maia_sample_products;

-- table-input "Regions"
CREATE OR REFRESH MATERIALIZED VIEW mv_regions AS
SELECT region_id, region_name, country
FROM main.matillion_demo.maia_sample_regions;

-- join "Join Products": Sales (s) INNER JOIN Products (p)
CREATE OR REFRESH MATERIALIZED VIEW mv_join_products AS
SELECT
  s.sale_id,
  s.product_id,
  s.region_id,
  s.quantity,
  s.revenue,
  p.product_name,
  p.category
FROM mv_sales s
INNER JOIN mv_products p
  ON `s`.`product_id` = `p`.`product_id`;

-- join "Join Regions": Join Products (sp) INNER JOIN Regions (r)
-- Note: region_id is re-taken from r (matching the Matillion columnMappings).
CREATE OR REFRESH MATERIALIZED VIEW mv_join_regions AS
SELECT
  sp.sale_id,
  sp.product_id,
  sp.quantity,
  sp.revenue,
  sp.product_name,
  sp.category,
  r.region_id,
  r.region_name,
  r.country
FROM mv_join_products sp
INNER JOIN mv_regions r
  ON `sp`.`region_id` = `r`.`region_id`;

-- aggregate "Aggregate": group by category, region_name
CREATE OR REFRESH MATERIALIZED VIEW mv_aggregate AS
SELECT
  category,
  region_name,
  SUM(revenue)   AS revenue,
  SUM(quantity)  AS quantity,
  COUNT(sale_id) AS sale_id
FROM mv_join_regions
GROUP BY category, region_name;

-- rewrite-table-dl "Write Output" -> the transformation's target table.
-- "Rewrite" = full overwrite each run, which is exactly materialized-view semantics.
CREATE OR REFRESH MATERIALIZED VIEW maia_sample_sales_summary AS
SELECT category, region_name, revenue, quantity, sale_id
FROM mv_aggregate;
