# Databricks notebook source
# MAGIC %md
# MAGIC **Step 1 Read table from bronze table**

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.results"
silver_table = f"{catlog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 drop url column**

# COMMAND ----------

results_selected_df = results_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 & 4 std case for name**

# COMMAND ----------

results_rename_df = (
    results_selected_df
    .withColumnsRenamed({
        "raceName":"race_name",
        "constructorId":"constructor_id",
        "driverId":"driver_id",
        "positionText":"position_text",
        "position":"finish_position",
        "date":"race_date",
        "grid":"grid_position",
        "laps":"completed_laps",
        "number":"car_number",
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Remove Null values**

# COMMAND ----------

results_valid_df = ( 
                    results_rename_df
                    .filter(
                        f.col("season").isNotNull() &
                        f.col("round").isNotNull() &
                        f.col("constructor_id").isNotNull() &
                        f.col("driver_id").isNotNull() )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Remove Duplicate**

# COMMAND ----------

results_distinct_df = (
    results_valid_df
    .dropDuplicates (["season","round","constructor_id","driver_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform values to title case**

# COMMAND ----------

results_final_df = (
    results_distinct_df
    .withColumn("race_name",f.initcap(f.col("race_name")))
)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 write in silver table**

# COMMAND ----------

(
    results_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)