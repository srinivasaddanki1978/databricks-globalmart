# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Bronze Ingestion (Auto Loader → Delta Live Tables)
# MAGIC
# MAGIC Ingests all 16 source files into 6 streaming Bronze tables.
# MAGIC Each table uses Auto Loader (`cloudFiles`) with schema evolution enabled.
# MAGIC Zero transformations — raw data preserved exactly as the source sent it.
# MAGIC Metadata columns `_source_file` and `_load_timestamp` added to every record.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# Source data volume path
SOURCE_BASE = "/Volumes/globalmart/bronze/source_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_customers
# MAGIC 6 CSV files from Region1–Region6. Four different ID column names across files.

# COMMAND ----------

@dlt.table(
    name="raw_customers",
    comment="Raw customer records from all 6 regional systems — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/customers")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/Region*/customers_*.csv")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_orders
# MAGIC 3 CSV files from Region1–Region3 subfolders.

# COMMAND ----------

@dlt.table(
    name="raw_orders",
    comment="Raw order records from regional systems — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/Region*/orders_*.csv")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_transactions
# MAGIC 3 CSV files — 2 inside Region subfolders + 1 at root level.
# MAGIC Requires TWO streams unioned via `unionByName(allowMissingColumns=True)`.

# COMMAND ----------

@dlt.table(
    name="raw_transactions",
    comment="Raw transaction line items from regional + root files — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_transactions():
    regional = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/transactions_regional")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/Region*/transactions_*.csv")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

    root = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/transactions_root")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/transactions_*.csv")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

    return regional.unionByName(root, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_returns
# MAGIC 2 JSON files — 1 in Region6 + 1 at root level.
# MAGIC Same two-stream union pattern as transactions.

# COMMAND ----------

@dlt.table(
    name="raw_returns",
    comment="Raw return records from Region6 + root JSON files — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_returns():
    region6 = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/returns_region6")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/Region*/returns_*.json")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

    root = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/returns_root")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/returns_*.json")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

    return region6.unionByName(root, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_products
# MAGIC Single JSON file at the root level. UPC in scientific notation, high NULL rates.

# COMMAND ----------

@dlt.table(
    name="raw_products",
    comment="Raw product catalog from products.json — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/products")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/products*.json")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_vendors
# MAGIC Single CSV file at root level. Relatively clean data.

# COMMAND ----------

@dlt.table(
    name="raw_vendors",
    comment="Raw vendor reference data from vendors.csv — zero transformations",
    table_properties={"quality": "bronze"},

)
def raw_vendors():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SOURCE_BASE}/_checkpoints/vendors")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("readerCaseSensitive", "false")
        .load(f"{SOURCE_BASE}/vendors*.csv")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )
