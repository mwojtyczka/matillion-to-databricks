# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: synthetic source data (manual pre-step — NOT a Job task)
# MAGIC
# MAGIC The Matillion source read from `${v_e_sales_db}.RAW.*` tables produced by upstream
# MAGIC ingestion — the pipeline never created them. In a real deployment those source
# MAGIC tables already exist and this notebook is **skipped**. For a **quick standalone
# MAGIC test**, run this notebook **once, by hand**, before `databricks bundle run` — it
# MAGIC fabricates the three input tables with synthetic data.
# MAGIC
# MAGIC - This is **not** a task in the Job (the production pipeline never fabricates data).
# MAGIC - Writes are guarded with `IF NOT EXISTS` / skip-if-present, so running it against a
# MAGIC   workspace that already has the real source tables **no-ops** — it won't clobber them.
# MAGIC - Data is synthetic stand-in only, generated with `dbldatagen`.

# COMMAND ----------

# MAGIC %pip install dbldatagen
# dbldatagen ships in some DBR ML images but NOT on serverless / vanilla clusters,
# so install it explicitly to keep this notebook runnable anywhere.

# COMMAND ----------

# Defaults MUST match the bundle variables in databricks.yml (catalog=main, schema=sales)
# so a default-run test seeds the same namespace the Job reads. If you override them at
# deploy/run time, pass the SAME values to this notebook's widgets.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "sales")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import StringType, TimestampType


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{name}")


N_PRODUCTS, N_REGIONS, N_SALES = 8, 3, 20
CATEGORIES = ["Electronics", "Furniture", "Stationery"]
REGION_NAMES = ["North America", "Europe", "Asia Pacific"]

# dbldatagen's implicit `id` column is 0-based (0..rows-1). The sales_src foreign keys
# below are generated 1-based, so PKs use `id + 1` to produce 1..N and match the FK ranges
# exactly — otherwise the transform's INNER JOIN would silently drop unmatched rows.

# products_src — product_id 1..N_PRODUCTS; some NULL categories exercise the IF(...) branch downstream
if not table_exists("products_src"):
    products = (
        dg.DataGenerator(spark, name="products_src", rows=N_PRODUCTS, partitions=1, randomSeed=42)
        .withColumn("product_id", StringType(), expr="cast(id + 1 as string)")
        .withColumn("product_name", StringType(), expr="concat('Product ', id + 1)")
        .withColumn("category", StringType(), values=CATEGORIES, percentNulls=0.2, random=True)
        .build()
    )
    products.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.products_src")

# regions_src — region_id 1..N_REGIONS
if not table_exists("regions_src"):
    regions = (
        dg.DataGenerator(spark, name="regions_src", rows=N_REGIONS, partitions=1, randomSeed=42)
        .withColumn("region_id", StringType(), expr="cast(id + 1 as string)")
        .withColumn("region_name", StringType(), values=REGION_NAMES)
        .build()
    )
    regions.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.regions_src")

# sales_src — FK ranges match the parents; sale_id has fewer unique values than rows,
# so duplicates occur and the transform's QUALIFY ROW_NUMBER() dedupe has something to do.
if not table_exists("sales_src"):
    sales = (
        dg.DataGenerator(spark, name="sales_src", rows=N_SALES, partitions=1, randomSeed=42)
        .withColumn("sale_id", StringType(), expr="cast(cast(rand(1)*15 as int) + 1 as string)")
        .withColumn("product_id", StringType(),
                    expr=f"cast(cast(rand(2)*{N_PRODUCTS - 1} as int) + 1 as string)")
        .withColumn("region_id", StringType(),
                    expr=f"cast(cast(rand(3)*{N_REGIONS - 1} as int) + 1 as string)")
        .withColumn("quantity", StringType(), expr="cast(cast(rand(4)*10 as int) + 1 as string)")
        .withColumn("revenue", StringType(), expr="cast(round(rand(5)*2000 + 10, 2) as string)")
        .withColumn("loaded_at", TimestampType(),
                    begin="2024-01-01 00:00:00", end="2024-01-31 23:59:59", interval="1 hour",
                    random=True)
        .build()
    )
    sales.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_src")

# COMMAND ----------

for t in ("products_src", "regions_src", "sales_src"):
    print(f"{catalog}.{schema}.{t}: {spark.table(f'{catalog}.{schema}.{t}').count()} rows")
