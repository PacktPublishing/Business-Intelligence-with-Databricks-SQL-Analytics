# Databricks notebook source
# DBTITLE 1,Create unpartitioned table
# MAGIC %sql 
# MAGIC drop table if exists chess_cells;
# MAGIC create table chess_cells(x int, y int, occupant string);

# COMMAND ----------

# DBTITLE 1,Disable stats collection on columns y and occupant
# MAGIC %sql
# MAGIC ALTER TABLE chess_cells SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1');

# COMMAND ----------

# DBTITLE 1,Insert 1 Record into 1 File
# MAGIC %sql
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(1,1,"white_queens_rook"),(1,2,"white_queens_knight"),(1,3,"white_queens_bishop"),(1,4,"white_queen");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(1,5,"white_king"),(1,6,"white_kings_bishop"),(1,7,"white_kings_knight"),(1,8,"white_kings_knight");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(2,1,"white_pawn_1"),(2,2,"white_pawn_2"),(2,3,"white_pawn_3"),(2,4,"white_pawn_4");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(2,5,"white_pawn_5"),(2,6,"white_pawn_6"),(2,7,"white_pawn_7"),(2,8,"white_pawn_8");
# MAGIC 
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(8,1,"black_queens_rook"),(8,2,"black_queens_knight"),(8,3,"black_queens_bishop"),(8,4,"black_queen");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(8,5,"black_king"),(8,6,"black_kings_bishop"),(8,7,"black_kings_knight"),(8,8,"black_kings_rook");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(7,1,"black_pawn_1"),(7,2,"black_pawn_2"),(7,3,"black_pawn_3"),(7,4,"black_pawn_4");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(7,5,"black_pawn_5"),(7,6,"black_pawn_6"),(7,7,"black_pawn_7"),(7,8,"black_pawn_8");
# MAGIC 
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(3,1,"empty"),(3,2,"empty"),(3,3,"empty"),(3,4,"empty");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(3,5,"empty"),(3,6,"empty"),(3,7,"empty"),(3,8,"empty");
# MAGIC 
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(4,1,"empty"),(4,2,"empty"),(4,3,"empty"),(3,4,"empty");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(4,5,"empty"),(4,6,"empty"),(4,7,"empty"),(3,8,"empty");
# MAGIC 
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(5,1,"empty"),(5,2,"empty"),(5,3,"empty"),(5,4,"empty");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(5,5,"empty"),(5,6,"empty"),(5,7,"empty"),(5,8,"empty");
# MAGIC 
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(6,1,"empty"),(6,2,"empty"),(6,3,"empty"),(6,4,"empty");
# MAGIC INSERT INTO chess_cells(x,y,occupant) VALUES(6,5,"empty"),(6,6,"empty"),(6,7,"empty"),(6,8,"empty");

# COMMAND ----------

# DBTITLE 1,Insert 4 records into one file
# MAGIC %scala
# MAGIC 
# MAGIC import scala.collection.JavaConversions._
# MAGIC import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
# MAGIC import org.apache.spark.sql.Row
# MAGIC val schema = StructType( Array(
# MAGIC                  StructField("x", IntegerType,true),
# MAGIC                  StructField("y", IntegerType,true),
# MAGIC                  StructField("occupant", StringType,true)
# MAGIC              ))
# MAGIC 
# MAGIC val rowData1= Seq(
# MAGIC                  Row(1,1,"white_queens_rook"), 
# MAGIC                  Row(1,2,"white_queens_knight"), 
# MAGIC                  Row(1,3,"white_queens_bishop"),
# MAGIC                  Row(1,4,"white_queen"),
# MAGIC                 )
# MAGIC var df1 = spark.createDataFrame(rowData1,schema).coalesce(1)
# MAGIC df1.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData2= Seq(
# MAGIC                  Row(1,5,"white_king"), 
# MAGIC                  Row(1,6,"white_kings_bishop"), 
# MAGIC                  Row(1,7,"white_kings_knight"),
# MAGIC                  Row(1,8,"white_kings_knight"),
# MAGIC                 )
# MAGIC var df2 = spark.createDataFrame(rowData2,schema).coalesce(1)
# MAGIC df2.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData3= Seq(
# MAGIC                  Row(2,1,"white_pawn_1"), 
# MAGIC                  Row(2,2,"white_pawn_2"), 
# MAGIC                  Row(2,3,"white_pawn_3"),
# MAGIC                  Row(2,4,"white_pawn_4"),
# MAGIC                 )
# MAGIC var df3 = spark.createDataFrame(rowData3,schema).coalesce(1)
# MAGIC df3.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData4= Seq(
# MAGIC                  Row(2,5,"white_pawn_5"), 
# MAGIC                  Row(2,6,"white_pawn_6"), 
# MAGIC                  Row(2,7,"white_pawn_7"),
# MAGIC                  Row(2,8,"white_pawn_8"),
# MAGIC                 )
# MAGIC var df4 = spark.createDataFrame(rowData4,schema).coalesce(1)
# MAGIC df4.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData5= Seq(
# MAGIC                  Row(8,1,"black_queens_rook"), 
# MAGIC                  Row(8,2,"black_queens_knight"), 
# MAGIC                  Row(8,3,"black_queens_bishop"),
# MAGIC                  Row(8,4,"black_queen"),
# MAGIC                 )
# MAGIC var df5 = spark.createDataFrame(rowData5,schema).coalesce(1)
# MAGIC df5.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData6= Seq(
# MAGIC                  Row(8,5,"black_king"), 
# MAGIC                  Row(8,6,"black_kings_bishop"), 
# MAGIC                  Row(8,7,"black_kings_knight"),
# MAGIC                  Row(8,8,"black_kings_rook"),
# MAGIC                 )
# MAGIC var df6 = spark.createDataFrame(rowData6,schema).coalesce(1)
# MAGIC df6.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData7= Seq(
# MAGIC                  Row(7,1,"black_pawn_1"), 
# MAGIC                  Row(7,2,"black_pawn_2"), 
# MAGIC                  Row(7,3,"black_pawn_3"),
# MAGIC                  Row(7,4,"black_pawn_4"),
# MAGIC                 )
# MAGIC var df7 = spark.createDataFrame(rowData7,schema).coalesce(1)
# MAGIC df7.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData8= Seq(
# MAGIC                  Row(7,5,"black_pawn_5"), 
# MAGIC                  Row(7,6,"black_pawn_6"), 
# MAGIC                  Row(7,7,"black_pawn_7"),
# MAGIC                  Row(7,8,"black_pawn_8"),
# MAGIC                 )
# MAGIC var df8 = spark.createDataFrame(rowData8,schema).coalesce(1)
# MAGIC df8.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData9= Seq(
# MAGIC                  Row(3,1,"empty"), 
# MAGIC                  Row(3,2,"empty"), 
# MAGIC                  Row(3,3,"empty"),
# MAGIC                  Row(3,4,"empty"),
# MAGIC                 )
# MAGIC var df9 = spark.createDataFrame(rowData9,schema).coalesce(1)
# MAGIC df9.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData10= Seq(
# MAGIC                  Row(3,5,"empty"), 
# MAGIC                  Row(3,6,"empty"), 
# MAGIC                  Row(3,7,"empty"),
# MAGIC                  Row(3,8,"empty"),
# MAGIC                 )
# MAGIC var df10 = spark.createDataFrame(rowData10,schema).coalesce(1)
# MAGIC df10.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData11= Seq(
# MAGIC                  Row(4,1,"empty"), 
# MAGIC                  Row(4,2,"empty"), 
# MAGIC                  Row(4,3,"empty"),
# MAGIC                  Row(4,4,"empty"),
# MAGIC                 )
# MAGIC var df11 = spark.createDataFrame(rowData11,schema).coalesce(1)
# MAGIC df11.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData12= Seq(
# MAGIC                  Row(4,5,"empty"), 
# MAGIC                  Row(4,6,"empty"), 
# MAGIC                  Row(4,7,"empty"),
# MAGIC                  Row(4,8,"empty"),
# MAGIC                 )
# MAGIC var df12 = spark.createDataFrame(rowData12,schema).coalesce(1)
# MAGIC df12.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData13= Seq(
# MAGIC                  Row(5,1,"empty"), 
# MAGIC                  Row(5,2,"empty"), 
# MAGIC                  Row(5,3,"empty"),
# MAGIC                  Row(5,4,"empty"),
# MAGIC                 )
# MAGIC var df13 = spark.createDataFrame(rowData13,schema).coalesce(1)
# MAGIC df13.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData14= Seq(
# MAGIC                  Row(5,5,"empty"), 
# MAGIC                  Row(5,6,"empty"), 
# MAGIC                  Row(5,7,"empty"),
# MAGIC                  Row(5,8,"empty"),
# MAGIC                 )
# MAGIC var df14 = spark.createDataFrame(rowData14,schema).coalesce(1)
# MAGIC df14.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData15= Seq(
# MAGIC                  Row(6,1,"empty"), 
# MAGIC                  Row(6,2,"empty"), 
# MAGIC                  Row(6,3,"empty"),
# MAGIC                  Row(6,4,"empty"),
# MAGIC                 )
# MAGIC var df15 = spark.createDataFrame(rowData15,schema).coalesce(1)
# MAGIC df15.write.format("delta").mode("append").saveAsTable("chess_cells")
# MAGIC 
# MAGIC val rowData16= Seq(
# MAGIC                  Row(6,5,"empty"), 
# MAGIC                  Row(6,6,"empty"), 
# MAGIC                  Row(6,7,"empty"),
# MAGIC                  Row(6,8,"empty"),
# MAGIC                 )
# MAGIC var df16 = spark.createDataFrame(rowData16,schema).coalesce(1)
# MAGIC df16.write.format("delta").mode("append").saveAsTable("chess_cells")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM chess_cells WHERE y=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL chess_cells

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/chess_cells/'))

# COMMAND ----------


