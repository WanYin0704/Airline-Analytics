# Databricks notebook source
# MAGIC %md
# MAGIC #d_airport
# MAGIC
# MAGIC This query is to populate the gold layer table for d_airport
# MAGIC
# MAGIC Source Table :
# MAGIC - silver.d_airport
# MAGIC
# MAGIC Target Table: 
# MAGIC - gold.d_airport

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT mnt_path DEFAULT 'abfss://gold@storageaccountadfproject.dfs.core.windows.net';
# MAGIC CREATE WIDGET TEXT batch_date DEFAULT '';

# COMMAND ----------

mnt_path = dbutils.widgets.get("mnt_path")
batch_date = dbutils.widgets.get("batch_date")

print(f"mnt_path: {mnt_path}")
print(f"batch_date: {batch_date}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW d_airport AS
# MAGIC SELECT 
# MAGIC airport_id, 
# MAGIC airport_id_sid_bigint,
# MAGIC airport_code,
# MAGIC airport_name,
# MAGIC city, 
# MAGIC country,
# MAGIC batchrun_date
# MAGIC FROM dev.silver.d_airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.d_airport
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date)
# MAGIC LOCATION '${mnt_path}/d_airport/'
# MAGIC AS
# MAGIC SELECT * FROM d_airport
# MAGIC WHERE 1=0;

# COMMAND ----------

data = spark.sql("""SELECT * FROM d_airport""")

data.write.format("delta").mode("overwrite").save(mnt_path + "/d_airport/")

# COMMAND ----------

print("d_airline gold notebook ran successfully")