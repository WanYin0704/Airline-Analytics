# Databricks notebook source
# MAGIC %md
# MAGIC #d_airport
# MAGIC
# MAGIC This query is to populate the silver layer table for d_airport
# MAGIC
# MAGIC Source Table :
# MAGIC - bronze.d_airport
# MAGIC
# MAGIC Target Table: 
# MAGIC - silver.d_airport

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT mnt_path DEFAULT 'abfss://silver@storageaccountadfproject.dfs.core.windows.net';
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
# MAGIC crc32(cast(airport_id as String)) as airport_id_sid_bigint,
# MAGIC airport_code,
# MAGIC airport_name,
# MAGIC city, 
# MAGIC   CASE
# MAGIC     WHEN country = 'USA' THEN 'United States'
# MAGIC     WHEN country = 'UK' THEN 'United Kingdom'
# MAGIC     WHEN country = 'UAE' THEN 'United Arab Emirates'
# MAGIC     ELSE country
# MAGIC   END AS country,
# MAGIC   '${batch_date}' AS batchrun_date
# MAGIC FROM dev.bronze.airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.d_airport
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

print("d_airline silver notebook ran successfully")