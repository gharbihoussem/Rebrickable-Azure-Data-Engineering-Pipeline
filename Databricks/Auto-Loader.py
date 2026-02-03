# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp


file_path = f"abfss://raw@ADLS_Name.dfs.core.windows.net/AutoLoaderInput/"
target_path = f"abfss://conformed@ADLS_Name.dfs.core.windows.net/AutoLoaderOutput"
checkpoint_path = f"abfss://checkpoint@ADLS_Name.dfs.core.windows.net/"
table_name = "GharbiDB.AutoLoader"


# Clear out data from previous demo execution
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)
# dbutils.fs.rm(target_path, True)


(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  #Infer column types
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("path", target_path) 
  .option("mergeSchema", "true")
  .trigger(processingTime='1 seconds') # For Streamming
  #.trigger(availableNow=True)  # For Batch Processing
  .toTable(table_name))
