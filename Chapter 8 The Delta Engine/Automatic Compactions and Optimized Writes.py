# Databricks notebook source
# MAGIC %sql
# MAGIC describe detail airlines.flights;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/airlines.db/flights'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE flights_csv
# MAGIC USING csv
# MAGIC OPTIONS(
# MAGIC   path '/databricks-datasets/asa/airlines/',
# MAGIC   header true,
# MAGIC   inferSchema true
# MAGIC );

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS flights_unoptimized;
# MAGIC CREATE TABLE flights_unoptimized USING DELTA AS SELECT * FROM flights_csv;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/flights_unoptimized/'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS flights_optimized;
# MAGIC CREATE TABLE flights_optimized USING DELTA TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true) AS SELECT * FROM flights_csv;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/flights_optimized/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Month, Origin, count(*) as TotalFlights 
# MAGIC FROM flights_unoptimized
# MAGIC WHERE DayOfWeek = 1 
# MAGIC GROUP BY Month, Origin 
# MAGIC ORDER BY TotalFlights DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Month, Origin, count(*) as TotalFlights 
# MAGIC FROM flights_optimized
# MAGIC WHERE DayOfWeek = 1 
# MAGIC GROUP BY Month, Origin 
# MAGIC ORDER BY TotalFlights DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS flights_optimized_no_compact;
# MAGIC CREATE TABLE flights_optimized_no_compact USING DELTA TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = false) AS SELECT * FROM flights_csv;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/flights_optimized_no_compact/'))
