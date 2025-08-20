# Databricks notebook source
# MAGIC %md
# MAGIC #d_passenger
# MAGIC
# MAGIC This query is to populate the gold layer table for d_passenger
# MAGIC
# MAGIC Source Table :
# MAGIC - silver.d_passenger
# MAGIC
# MAGIC Target Table: 
# MAGIC - gold.d_passenger

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW d_passenger AS
# MAGIC SELECT 
# MAGIC passenger_id, 
# MAGIC passenger_id_sid_bigint,
# MAGIC full_name,
# MAGIC gender,
# MAGIC age,
# MAGIC batchrun_date
# MAGIC FROM dev.silver.d_passenger;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.d_passenger
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date)
# MAGIC LOCATION '${mnt_path}/d_passenger/'
# MAGIC AS
# MAGIC SELECT * FROM d_passenger
# MAGIC WHERE 1=0;

# COMMAND ----------

data = spark.sql("""SELECT * FROM d_passenger""")

data.write.format("delta").mode("overwrite").save(mnt_path + "/d_passenger/")

# COMMAND ----------

print("d_passenger gold notebook ran successfully")