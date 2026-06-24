# Databricks notebook source
# MAGIC %md
# MAGIC #Build Drivers Dimension
# MAGIC - Read silver drivers table
# MAGIC - Read  gold ref_nationality_region table
# MAGIC - Join driver and ref_nationality_region table using nationality column
# MAGIC - Select the required column
# MAGIC     - drivers.driver_id
# MAGIC     - drivers.drivers_name
# MAGIC     - drivers.date_of_birth
# MAGIC     - drivers.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC - write transform data in gold layer

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

table_name = f"{catlog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

drivers_df = spark.table(f"{catlog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.table(f"{catlog_name}.{gold_schema}.ref_nationality_referance")

# COMMAND ----------

dim_drivers = (
    drivers_df
    .join(
        ref_nationality_region_df,
        drivers_df.nationality  == ref_nationality_region_df.nationality,
        "left"
    )
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_df.region
    )
)

# COMMAND ----------

(
    dim_drivers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema",True)
    .saveAsTable(table_name)
)