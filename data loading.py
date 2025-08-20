# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT file_name DEFAULT '';
# MAGIC CREATE WIDGET TEXT batch_date DEFAULT '';

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")
batch_date = dbutils.widgets.get("batch_date")

print(f"File path: {file_name}")
print(f"batch_date: {batch_date}")

# COMMAND ----------

from pyspark.sql import functions as F
import os

if file_name == 'booking':
  df = spark.read.parquet(f"abfss://bronze@storageaccountadfproject.dfs.core.windows.net/sql/")
else:
  df = spark.read.parquet(f"abfss://bronze@storageaccountadfproject.dfs.core.windows.net/onprem/{file_name.replace('.csv', '.parquet')}")

# Add batchrun_date column
df = df.withColumn("batchrun_date", F.lit(batch_date))

# Write into Bronze Delta
table_name = os.path.splitext(file_name)[0]  # removes any extension if present
(
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("batchrun_date")
      .saveAsTable(f"dev.bronze.{table_name}")
)