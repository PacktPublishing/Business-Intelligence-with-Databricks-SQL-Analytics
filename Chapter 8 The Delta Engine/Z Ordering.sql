-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select * from airlines.flights;

-- COMMAND ----------



-- COMMAND ----------

use airlines;
explain SELECT f.FlightNum, f.TailNum, p.manufacturer, p.model FROM flights f LEFT OUTER JOIN planes p WHERE f.FlightNum between 1460 and 1470;

-- COMMAND ----------

use airlines;
SELECT to_date(concat(f.Year,'-',f.Month,'-',f.DateofMonth)), f.FlightNum, f.TailNum, p.manufacturer, p.model FROM flights f LEFT OUTER JOIN planes p WHERE f.FlightNum between 1460 and 1470;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from airlines.planes where manufacturer is not null;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select year, count(*) from airlines.flights group by year

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from (select UniqueCarrier, count(*) from airlines.flights group by UniqueCarrier)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from (select FlightNum, count(*) from airlines.flights group by FlightNum)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from (select TailNum, count(*) from airlines.flights group by TailNum)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from (select Origin, count(*) from airlines.flights group by Origin)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from (select Dest, count(*) from airlines.flights group by Dest)

-- COMMAND ----------

-- DBTITLE 1,Single Column Partition
-- MAGIC %sql 
-- MAGIC DROP TABLE IF EXISTS flights1;
-- MAGIC --CREATE TABLE flights_yp USING DELTA PARTITIONED BY (year) AS SELECT * FROM airlines.flights;
-- MAGIC CREATE TABLE flights1 USING DELTA AS SELECT * FROM airlines.flights_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/flights_yp/'))

-- COMMAND ----------

SELECT * FROM flights1 WHERE tailnum = 'N641DL'

-- COMMAND ----------

OPTIMIZE flights1 ZORDER BY tailnum

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --with year filter = 2008, 14 out of the 19 files were pruned out. 
-- MAGIC --The stats collection helps with that. read data size 40.9 mb, parquet groups read 5, size of files read: 447 mb
-- MAGIC select count(*) from flights_yp where crsarrtime between 1800 and 1900 and crsdeptime between 1800 and 1900;
-- MAGIC --with no year filter, 18 of the 19 files were read. only 1 got pruned.
-- MAGIC --the stats collection on the crs arr and crs dep must have helped a bit as 1 prune happened,but the values are distributed too evenly and rndomly, so more pruning does not happen
-- MAGIC --read data size NA, parquet group read 18, size of files read 1500 mb

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC OPTIMIZE flights_yp ZORDER BY crsarrtime, crsdeptime

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --post z order, 5 files produced. with y=208 filter, 3 files pruned, 2 read, size of files 613, read data 33 mb
-- MAGIC select count(*) from flights_yp where crsarrtime between 1800 and 1900 and crsdeptime between 1800 and 1900;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = False;
-- MAGIC VACUUM flights_yp RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --all 10 data files read for month 10
-- MAGIC select count(*) from flights_yp_cl where year = 2008 and month = 10 and dayofmonth = 10;
