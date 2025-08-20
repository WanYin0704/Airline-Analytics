# Databricks notebook source
# MAGIC %md
# MAGIC #d_flight
# MAGIC
# MAGIC This query is to populate the gold layer table for d_flight
# MAGIC
# MAGIC Source Table :
# MAGIC - silver.d_flight
# MAGIC
# MAGIC Target Table: 
# MAGIC - gold.d_flight

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW d_flight AS
# MAGIC SELECT 
# MAGIC f.flight_id, 
# MAGIC f.flight_id_sid_bigint, 
# MAGIC f.airline_id, 
# MAGIC f.origin_airport_id, 
# MAGIC f.destination_airport_id, 
# MAGIC CONCAT(a1.airport_code, ' → ', a2.airport_code) AS route,
# MAGIC f.flight_number, 
# MAGIC f.departure_date, 
# MAGIC f.departure_time, 
# MAGIC f.arrival_date, 
# MAGIC f.arrival_time, 
# MAGIC f.flight_duration_minutes,
# MAGIC f.batchrun_date
# MAGIC FROM dev.silver.d_flight f
# MAGIC JOIN dev.silver.f_booking b ON f.flight_id = b.flight_id
# MAGIC JOIN dev.silver.d_airport a1 ON f.origin_airport_id = a1.airport_id
# MAGIC JOIN dev.silver.d_airport a2 ON f.destination_airport_id = a2.airport_id
# MAGIC WHERE year(b.booking_date) >= year(to_date(f.batchrun_date, 'yyyyMMdd')) - 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.d_flight
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date)
# MAGIC LOCATION '${mnt_path}/d_flight/'
# MAGIC AS
# MAGIC SELECT * FROM d_flight
# MAGIC WHERE 1=0;

# COMMAND ----------

data = spark.sql("""SELECT * FROM d_flight""")

data.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(mnt_path + "/d_flight/")

# COMMAND ----------

print("d_flight gold notebook ran successfully")