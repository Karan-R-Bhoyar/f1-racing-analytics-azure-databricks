# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Circuits data
# MAGIC
# MAGIC - 1.Read brozne circuits table
# MAGIC - 2. Keep only required columm (drop url column)
# MAGIC - 3. Standardize column name using snake_case
# MAGIC - 4. rename the column to make them more meaning full
# MAGIC - 5. Filter out null values
# MAGIC - 6. Remove duplicate records
# MAGIC - 7. Transform values of column circuits_name and locality to title case
# MAGIC - 8. Write the transformed data to silver circuits table

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.circuits"
silver_table = f"{catlog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %run "../00 - Common/02. Bronze Helper"

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 Read data from Bronze table**

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 Keep only required column**

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

circuits_selected_df = circuits_df.select (
    f.col("circuitId"),
    f.col("circuitName"),
    f.col("lat"),
    f.col("long").alias("longitude"),
    f.col("locality"),
    f.col("country"),
    f.col("ingestion_timestamp"),
    f.col("source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step 3 Standerdise column name**

# COMMAND ----------

circuits_rename_df = (
    circuits_selected_df
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("longitude", "longitude")
    .withColumnRenamed("circuitId","circuit_id")
    .withColumnRenamed("circuitName","circuit_name")
)

# COMMAND ----------

circuits_rename_df = (
    circuits_selected_df
    .withColumnsRenamed({
        "lat":"latitude",
        "longitude":"longitude",
        "circuitId":"circuit_id",
        "circuitName":"circuit_name"
    })
)

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Remove null values**
# MAGIC

# COMMAND ----------

circuits_valid_df = circuits_rename_df.filter(f.col("circuit_id").isNotNull())

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Remove Duplicates**

# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Transform column values in Title Case**

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn("circuit_name", f.initcap(f.col("circuit_name")))
    .withColumn("locality", f.initcap(f.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 Write the tranformed data to silver circuits table**

# COMMAND ----------

(
    circuits_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
    
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.silver.circuits;