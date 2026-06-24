# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results folder
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema(preserve the nested srtucture)
# MAGIC 3. Add metadata columns 
# MAGIC - Source File
# MAGIC - Ingestion timestamp
# MAGIC 4. Write to broze delta table

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

# MAGIC %run "../00 - Common/02. Bronze Helper"

# COMMAND ----------

source_file = f"{landing_folder_path}/results/"
table_name = f"{catlog_name}.{bronze_schema}.results"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,StringType,DateType,IntegerType,FloatType,DoubleType


results_schema = StructType([
    StructField('date', DateType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', FloatType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType()),

])

# COMMAND ----------

spark.read.json("/Volumes/formula1/landing/files/drivers.json")

# COMMAND ----------

# Read Json File 

results_df = (
    spark.read
    .format('json')
    .schema(results_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

final_results_df = add_metadata(results_df)

# COMMAND ----------

display(final_results_df)

# COMMAND ----------

(
    final_results_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT season, count(*)
# MAGIC FROM formula1.bronze.results
# MAGIC GROUP BY season
# MAGIC ORDER BY season