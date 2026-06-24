# Databricks notebook source
# MAGIC %md
# MAGIC #Build Race Fact
# MAGIC - 1. Read results silver table
# MAGIC - 2. Read silver sprints table
# MAGIC - 3. Add new column session type with race or sprint
# MAGIC - 4. Union results and sprints
# MAGIC - 5. Derive additional column
# MAGIC     - is_win -> Indicate that drive own the race
# MAGIC     - is_podium -> Indicates that drives scored a podium results 
# MAGIC     - has_points -> Indicates that driver has scored a point
# MAGIC - 6. write transformed data in gold layer

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

target_table = f"{catlog_name}.{gold_schema}.fact_session_results_df"

# COMMAND ----------

results_df = (
spark.table(f"{catlog_name}.{silver_schema}.results")
    .withColumn("session_type", f.lit("RACE"))   
    .drop("race_date","race_name","ingestion_timestamp","source_file")                     
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catlog_name}.{silver_schema}.sprints")
    .withColumn("session_type",f.lit("SPRINT"))
    .drop("race_date","race_name","ingestion_timestamp","source_file")      
)

# COMMAND ----------

results_sprint_df = results_df.unionByName(sprints_df)

# COMMAND ----------

display(results_sprint_df)

# COMMAND ----------

fact_session_results_df = (results_sprint_df
    .withColumn("is_witn", f.col("finish_position")==1)
    .withColumn("is_podium",f.col("finish_position").between(1,3))
    .withColumn("has_points",f.col("points")>0))

# COMMAND ----------

display(fact_session_results_df)

# COMMAND ----------

(
    fact_session_results_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)