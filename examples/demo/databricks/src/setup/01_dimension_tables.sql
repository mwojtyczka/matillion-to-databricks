-- Converted from Matillion sql-executor step "Dimension Tables"
-- Source: matillion/create-maia-demo-data.orch.yaml
-- Seed/DDL is control-flow setup, not dataflow -> a Job SQL task (NOT a Lakeflow table).
-- Matillion [Environment Default] resolved to main.matillion_demo.

CREATE OR REPLACE TABLE main.matillion_demo.maia_sample_products (
  product_id STRING,
  product_name STRING,
  category STRING,
  unit_price DECIMAL(18, 2),
  stock_quantity INTEGER
);

INSERT INTO main.matillion_demo.maia_sample_products VALUES
  ('PROD001', 'Laptop Pro 15', 'Electronics', 1299.99, 45),
  ('PROD002', 'Wireless Mouse', 'Electronics', 29.99, 250),
  ('PROD003', 'USB-C Cable', 'Accessories', 12.99, 500),
  ('PROD004', 'Monitor 4K', 'Electronics', 499.99, 30),
  ('PROD005', 'Keyboard Mechanical', 'Electronics', 149.99, 85),
  ('PROD006', 'Desk Lamp LED', 'Office', 59.99, 120),
  ('PROD007', 'Office Chair', 'Furniture', 299.99, 25),
  ('PROD008', 'Notebook Set', 'Stationery', 15.99, 300),
  ('PROD009', 'Pen Pack 12', 'Stationery', 8.99, 400),
  ('PROD010', 'Desk Organizer', 'Office', 34.99, 150),
  ('PROD011', 'Monitor Stand', 'Accessories', 49.99, 75),
  ('PROD012', 'Webcam HD', 'Electronics', 89.99, 60),
  ('PROD013', 'Headphones Noise Cancel', 'Electronics', 199.99, 40),
  ('PROD014', 'Screen Protector', 'Accessories', 19.99, 200),
  ('PROD015', 'Laptop Stand Adjustable', 'Furniture', 79.99, 110);

CREATE OR REPLACE TABLE main.matillion_demo.maia_sample_regions (
  region_id STRING,
  region_name STRING,
  country STRING,
  sales_manager STRING,
  timezone_offset INTEGER
);

INSERT INTO main.matillion_demo.maia_sample_regions VALUES
  ('REG001', 'North America East', 'United States', 'John Smith', -5),
  ('REG002', 'North America West', 'United States', 'Sarah Johnson', -8),
  ('REG003', 'Canada Central', 'Canada', 'Michael Brown', -6),
  ('REG004', 'Western Europe', 'Germany', 'Klaus Mueller', 1),
  ('REG005', 'Northern Europe', 'United Kingdom', 'Emma Wilson', 0),
  ('REG006', 'Southern Europe', 'Italy', 'Marco Rossi', 1),
  ('REG007', 'Eastern Europe', 'Poland', 'Anna Kowalski', 1),
  ('REG008', 'Asia Pacific North', 'Japan', 'Yuki Tanaka', 9),
  ('REG009', 'Asia Pacific South', 'Australia', 'David Chen', 10),
  ('REG010', 'Southeast Asia', 'Singapore', 'Priya Sharma', 8),
  ('REG011', 'Middle East', 'United Arab Emirates', 'Fatima Al-Mansouri', 4),
  ('REG012', 'Latin America North', 'Mexico', 'Carlos Rodriguez', -6),
  ('REG013', 'Latin America South', 'Brazil', 'Paulo Silva', -3);
