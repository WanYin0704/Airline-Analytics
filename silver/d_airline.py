# Databricks notebook source
# MAGIC %md
# MAGIC #d_airline
# MAGIC
# MAGIC This query is to populate the silver layer table for d_airline
# MAGIC
# MAGIC Source Table :
# MAGIC - bronze.d_airline
# MAGIC
# MAGIC Target Table: 
# MAGIC - silver.d_airline
# MAGIC

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW d_airline AS
# MAGIC SELECT 
# MAGIC airline_id, 
# MAGIC crc32(airline_id) as airline_id_sid_bigint,
# MAGIC airline_code,
# MAGIC airline_name, 
# MAGIC  CASE
# MAGIC     WHEN country = 'USA' THEN 'United States'
# MAGIC     WHEN country = 'UK' THEN 'United Kingdom'
# MAGIC     WHEN country = 'UAE' THEN 'United Arab Emirates'
# MAGIC     ELSE country
# MAGIC   END AS country,
# MAGIC   '${batch_date}' AS batchrun_date
# MAGIC FROM dev.bronze.airline;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.d_airline
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date)
# MAGIC LOCATION '${mnt_path}/d_airline/'
# MAGIC AS
# MAGIC SELECT * FROM d_airline
# MAGIC WHERE 1=0;
# MAGIC
# MAGIC

# COMMAND ----------

data = spark.sql("""SELECT * FROM d_airline""")

data.write.format("delta").mode("overwrite").save(mnt_path + "/d_airline/")
