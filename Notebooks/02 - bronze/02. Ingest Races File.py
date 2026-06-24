# Databricks notebook source
# MAGIC %md
# MAGIC # Read the file using spark dataframe reader API
# MAGIC
# MAGIC - Add metadata columns
# MAGIC - Source File
# MAGIC - Ingestion timestamp
# MAGIC - Write to broze delta table

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catlog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, DateType,StructField

races_schema = StructType([
    StructField('season', IntegerType()),
    StructField('round', IntegerType()),
    StructField('url', StringType()),
    StructField('raceName', StringType()),
    StructField('date', DateType()),
    StructField('circuitId', StringType()),
])


# COMMAND ----------

# test_df = (spark.read
#     .format('csv')
#     .option('header', True)
#     .option('inferSchema', True)
#     .load('dbfs:/Volumes/formula1/landing/files/races.csv'))

# test_df.printSchema()
# display(test_df.limit(3))

# COMMAND ----------



races_df = (
    spark.read
    .format('csv')
    .option('header', True)
    .option('mode', 'FAILFAST')
    .schema(races_schema)
    .load(source_file))

# COMMAND ----------

# DBTITLE 1,Cell 4
display(races_df)



# COMMAND ----------

from pyspark.sql import functions as f

final_races_df = (
    races_df
        .withColumn('ingestion_timestamp', f.current_timestamp())
        .withColumn('source_file', f.col('_metadata.file_path'))
                  
    )

# COMMAND ----------

display(final_races_df)

# COMMAND ----------

(
    final_races_df
    .write
    .mode('overwrite')
    .format('delta')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.bronze.races;

# COMMAND ----------

display(spark.table(table_name))
