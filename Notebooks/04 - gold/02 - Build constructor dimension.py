# Databricks notebook source
# MAGIC %md
# MAGIC #Build Constructors Dimension
# MAGIC - Read silver constructors table
# MAGIC - Read gold ref_nationality_referance
# MAGIC - Join constructors and ref_nationality with nationality
# MAGIC - Select the required column
# MAGIC     - constructors.constructors_id
# MAGIC     - constructors.constructors_name
# MAGIC     - constructors.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC - Write the transformed data in gold layer

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

target_table = f"{catlog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #read source table
# MAGIC - silver.contrucotrs
# MAGIC - gold.ref_nationality_region
# MAGIC

# COMMAND ----------

constructors_df = spark.table(f"{catlog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catlog_name}.{gold_schema}.ref_nationality_referance")

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
        ref_nationality_region_df,
        constructors_df.nationality == ref_nationality_region_df.nationality,
        "left"
    )
    .select(
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region"),
    )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

(
    dim_constructors_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.dim_constructors;