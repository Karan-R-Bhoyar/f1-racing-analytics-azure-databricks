# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuits.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add metadata columns 
# MAGIC - Source File
# MAGIC - Ingestion timestamp
# MAGIC 3. Write to broze delta table
# MAGIC

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

# MAGIC %run "../00 - Common/02. Bronze Helper"

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catlog_name}.{bronze_schema}.circuits"

# COMMAND ----------

table_name

# COMMAND ----------

source_file
#.load('/Volumes/formula1/landing/files/races.csv'))
#/Volumes/formula1/landing/files/circuits.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the file using Dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType()),
])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header',True)
#.option('inferSchema', True)
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load(source_file))

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Column
# MAGIC - source file
# MAGIC - ingest timestamp

# COMMAND ----------


circuits_final_df = add_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to broze delta table

# COMMAND ----------

(
    circuits_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.circuits;

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))