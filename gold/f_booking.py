# Databricks notebook source
# MAGIC %md
# MAGIC #f_booking
# MAGIC
# MAGIC This query is to populate the gold layer table for f_booking
# MAGIC
# MAGIC Source Table :
# MAGIC - silver.f_booking
# MAGIC
# MAGIC Target Table: 
# MAGIC - gold.f_booking

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW f_booking AS
# MAGIC SELECT 
# MAGIC booking_id, 
# MAGIC booking_id_sid_bigint,
# MAGIC flight_id,
# MAGIC passenger_id,
# MAGIC booking_date,
# MAGIC booking_time,
# MAGIC ticket_price,
# MAGIC booking_status,
# MAGIC batchrun_date,
# MAGIC booking_year
# MAGIC FROM dev.silver.f_booking
# MAGIC WHERE year(booking_date) >= year(to_date(batchrun_date, 'yyyyMMdd')) - 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.f_booking
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

print("f_booking gold notebook ran successfully")