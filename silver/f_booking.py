# Databricks notebook source
# MAGIC %md
# MAGIC #f_booking
# MAGIC
# MAGIC This query is to populate the silver layer table for f_booking
# MAGIC
# MAGIC Source Table :
# MAGIC - bronze.f_booking
# MAGIC
# MAGIC Target Table: 
# MAGIC - silver.f_booking

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW f_booking AS
# MAGIC SELECT 
# MAGIC booking_id, 
# MAGIC crc32(cast(booking_id as String)) as booking_id_sid_bigint,
# MAGIC flight_id,
# MAGIC passenger_id,
# MAGIC CAST(from_utc_timestamp(to_timestamp(booking_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "UTC") AS DATE) AS booking_date,
# MAGIC DATE_FORMAT(from_utc_timestamp(to_timestamp(booking_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "UTC"), 'HH:mm:ss') AS booking_time,
# MAGIC ROUND(ticket_price,2) as ticket_price,
# MAGIC booking_status,
# MAGIC '${batch_date}' AS batchrun_date,
# MAGIC YEAR(booking_date) as booking_year
# MAGIC FROM dev.bronze.booking

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.f_booking
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (batchrun_date, booking_year)
# MAGIC LOCATION '${mnt_path}/f_booking/'
# MAGIC AS
# MAGIC SELECT * FROM f_booking
# MAGIC WHERE 1=0;

# COMMAND ----------

data = spark.sql("""SELECT * FROM f_booking""")

data.write.format("delta").mode("overwrite").save(mnt_path + "/f_booking/")

# COMMAND ----------

print("f_booking silver notebook ran successfully")