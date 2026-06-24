# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC - Read Bronze races data
# MAGIC - Drop URL column
# MAGIC - Change column name form raceName to race_name, circuitId to circuit_it
# MAGIC - rename column from date to race_date
# MAGIC - remove duplicate records
# MAGIC - race_name values to Title Case
# MAGIC - transfer the data to silver layer

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.races"
silver_table = f"{catlog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 read data**

# COMMAND ----------

races_df = spark.read.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 Drop unwanted column**

# COMMAND ----------

races_selected_df = races_df.select (
    f.col("season"),
    f.col("round"),
    f.col("raceName"),
    f.col("date"),
    f.col("circuitId"),
    f.col("ingestion_timestamp"),
    f.col("source_file")
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 Rename Column**

# COMMAND ----------

races_rename_df = ( races_selected_df
.withColumnRenamed("raceName", "race_name")
.withColumnRenamed("circuitId", "circuit_id")
.withColumnRenamed("date","race_date")
)

# COMMAND ----------

races_valid_df = races_rename_df.dropDuplicates(["season", "round"])

# COMMAND ----------

display(races_valid_df)

# COMMAND ----------

races_final_df = ( races_valid_df
    .withColumn("race_name", f.initcap(f.col("race_name")))
                  )

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")  
    .saveAsTable(silver_table)        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.silver.races;