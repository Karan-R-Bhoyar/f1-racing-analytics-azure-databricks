# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest sprints folder
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

source_file = f"{landing_folder_path}/sprints/"
table_name = f"{catlog_name}.{bronze_schema}.sprints"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,StringType,DateType,IntegerType,FloatType,DoubleType


sprints_schema = StructType([
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

sprints_df = (
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode','FAILFAST')
    .option('multiLine', True)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

final_sprints_df = add_metadata(sprints_df)

# COMMAND ----------

display(final_sprints_df)

# COMMAND ----------

(
    final_sprints_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.sprints;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT season, count(*)
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season

# COMMAND ----------

