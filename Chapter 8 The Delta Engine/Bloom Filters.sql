-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select * from airlines.flights;

-- COMMAND ----------

-- DBTITLE 1,Single Column Partition
-- MAGIC %sql 
-- MAGIC DROP TABLE IF EXISTS flights1;
-- MAGIC --CREATE TABLE flights_yp USING DELTA PARTITIONED BY (year) AS SELECT * FROM airlines.flights;
-- MAGIC CREATE TABLE flights1 USING DELTA TBLPROPERTIES (delta.dataSkippingNumIndexedCols  = 9) AS SELECT * FROM airlines.flights_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/flights1/'))

-- COMMAND ----------

SELECT * FROM flights1 WHERE tailnum = 'N641DL'

-- COMMAND ----------

CREATE BLOOMFILTER INDEX
ON TABLE flights1
FOR COLUMNS(TailNum OPTIONS (fpp=1, numItems=200000))

-- COMMAND ----------

DROP BLOOMFILTER INDEX ON TABLE flights1 FOR COLUMNS(TailNum);

-- COMMAND ----------

SELECT * FROM flights1 WHERE tailnum in ('N641DL', 'N687DL')

-- COMMAND ----------

CACHE SELECT * FROM airlines.flights

-- COMMAND ----------

select * from airlines.flights

