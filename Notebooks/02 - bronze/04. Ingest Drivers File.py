# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructor.json file
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

source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catlog_name}.{bronze_schema}.drivers"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,StringType,DateType

new_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', new_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())

])

# COMMAND ----------

spark.read.json("/Volumes/formula1/landing/files/drivers.json")

# COMMAND ----------

# Read Json File 

drivers_df = (
    spark.read
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

final_drivers_df = add_metadata(drivers_df)

# COMMAND ----------

display(final_drivers_df)

# COMMAND ----------

(
    final_drivers_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.drivers;