# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructor.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add metadata columns 
# MAGIC - Source File
# MAGIC - Ingestion timestamp
# MAGIC 3. Write to broze delta table

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

# MAGIC %run "../00 - Common/02. Bronze Helper"

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catlog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# Define Schema

constructors_schema = """
                constructorId STRING,
                name STRING,
                nationality STRING,
                url STRING """                

# COMMAND ----------

spark.read.json("/Volumes/formula1/landing/files/constructors.json")

# COMMAND ----------

# Read Json File 

constructors_df = (
    spark.read
    .format('json')
    .schema(constructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

final_constructors_df = add_metadata(constructors_df)

# COMMAND ----------

display(final_constructors_df)

# COMMAND ----------

(
    final_constructors_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.constructors;