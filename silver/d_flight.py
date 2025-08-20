# Databricks notebook source
# MAGIC %md
# MAGIC #d_flight
# MAGIC
# MAGIC This query is to populate the silver layer table for d_flight
# MAGIC
# MAGIC Source Table :
# MAGIC - bronze.d_flight
# MAGIC
# MAGIC Target Table: 
# MAGIC - silver.d_flight

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW d_flight AS
# MAGIC SELECT 
# MAGIC flight_id, 
# MAGIC crc32(cast(flight_id as String)) as flight_id_sid_bigint,
# MAGIC airline_id,
# MAGIC origin_airport_id,
# MAGIC destination_airport_id,
# MAGIC flight_number,
# MAGIC CAST(departure_date AS DATE) AS departure_date,
# MAGIC DATE_FORMAT(departure_date, 'HH:mm:ss') AS departure_time,
# MAGIC CAST(arrival_date AS DATE) AS arrival_date,
# MAGIC DATE_FORMAT(arrival_date, 'HH:mm:ss') AS arrival_time,
# MAGIC ROUND(
# MAGIC     (unix_timestamp(arrival_date) - unix_timestamp(departure_date)) / 60
# MAGIC ) AS flight_duration_minutes,
# MAGIC '${batch_date}' AS batchrun_date
# MAGIC FROM dev.bronze.flight;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.d_flight
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date)
# MAGIC LOCATION '${mnt_path}/d_flight/'
# MAGIC AS
# MAGIC SELECT * FROM d_flight
# MAGIC WHERE 1=0;

# COMMAND ----------

data = spark.sql("""SELECT * FROM d_flight""")

data.write.format("delta").mode("overwrite").save(mnt_path + "/d_flight/")

# COMMAND ----------

print("d_flight silver notebook ran successfully")