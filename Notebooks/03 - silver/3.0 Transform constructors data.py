# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.constructors"
silver_table = f"{catlog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = spark.read.table(bronze_table)

# COMMAND ----------

constructors_select_df =  constructors_df.select (
                          f.col("constructorId"),
                          f.col("name").alias("constructor_name"),
                          f.col("nationality"),
                          f.col("ingestion_timestamp"),
                          f.col("source_file"),
)


# COMMAND ----------

constructors_rename_df = (
    constructors_select_df
    .withColumnRenamed("constructorId", "constructor_id")
)

# COMMAND ----------

constructors_valid_df = constructors_rename_df.dropDuplicates(["constructor_id"])


# COMMAND ----------

constructors_final_df = ( constructors_valid_df
    .withColumn("nationality", f.initcap(f.col("nationality")))
                  )

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.silver.constructors;