-- Seed data for the demo (not part of the Matillion source).
--
-- The Snowflake source reads from ${v_e_sales_db}.RAW.* tables produced upstream.
-- To make this migrated bundle runnable on its own, we seed small RAW fixtures here,
-- exactly as the databricks-source demo seeds its sample_* tables. In a real migration
-- these RAW tables already exist in UC (produced by ingestion), so you would NOT emit
-- this file — you'd point the DDL below at the real source tables.
--
-- catalog/schema are NOT hardcoded: they arrive as SQL task parameters (:catalog /
-- :schema, from the bundle variables) and are applied via USE ... IDENTIFIER().

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE OR REPLACE TABLE products_src (
  product_id STRING,
  product_name STRING,
  category STRING
);
INSERT INTO products_src VALUES
  ('1', 'Laptop Pro 15', 'Electronics'),
  ('2', 'Wireless Mouse', 'Electronics'),
  ('3', 'USB-C Cable', NULL),
  ('4', 'Office Chair', 'Furniture'),
  ('5', 'Notebook Set', 'Stationery');

CREATE OR REPLACE TABLE regions_src (
  region_id STRING,
  region_name STRING
);
INSERT INTO regions_src VALUES
  ('1', 'North America'),
  ('2', 'Europe'),
  ('3', 'Asia Pacific');

CREATE OR REPLACE TABLE sales_src (
  sale_id STRING,
  product_id STRING,
  region_id STRING,
  quantity STRING,
  revenue STRING,
  loaded_at TIMESTAMP
);
INSERT INTO sales_src VALUES
  ('1', '1', '1', '2', '2599.98', TIMESTAMP '2024-01-05 10:00:00'),
  ('2', '2', '1', '5', '149.95',  TIMESTAMP '2024-01-05 11:00:00'),
  ('3', '3', '2', '10', '129.90', TIMESTAMP '2024-01-06 09:30:00'),
  ('4', '4', '3', '1', '299.99',  TIMESTAMP '2024-01-06 14:00:00'),
  ('5', '1', '2', '1', '1299.99', TIMESTAMP '2024-01-07 08:00:00'),
  -- duplicate sale_id 5, older load — the QUALIFY dedupe below keeps the newer row
  ('5', '1', '2', '1', '1199.99', TIMESTAMP '2024-01-06 08:00:00');
