# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.drivers"
silver_table = f"{catlog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.read.table(bronze_table)

# COMMAND ----------

drivers_selected_df = drivers_df.drop(f.col("url"))

# COMMAND ----------

drivers_rename_df = (
    drivers_selected_df
    .withColumnsRenamed({
        "driverId":"driver_id",
        "dateOfBirth":"date_of_birth"
      
    })
)

# COMMAND ----------

drivers_concat_df = (
    drivers_rename_df
    .withColumn ("driver_name",
        f.initcap(f.concat_ws(" " , f.col("name.givenName"), f.col("name.familyName"))))
 .drop("name")  )


# COMMAND ----------

display(drivers_concat_df)

# COMMAND ----------

drivers_distinct_df = drivers_concat_df.dropDuplicates(["driver_id"])

# COMMAND ----------

driver_final_df = (
 drivers_distinct_df
    .withColumn("nationality", f.initcap(f.col("nationality"))))

# COMMAND ----------

(
    driver_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")  
    .saveAsTable(silver_table)        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.silver.drivers;