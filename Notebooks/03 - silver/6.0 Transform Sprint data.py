# Databricks notebook source
# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

bronze_table = f"{catlog_name}.{bronze_schema}.sprints"
silver_table = f"{catlog_name}.{silver_schema}.sprints"

# COMMAND ----------

sprints_df = (
    spark.read.table(bronze_table)
        .select (
        f.col("date"),
        f.col("raceName"),
        f.col("round"),
        f.col("season"),
        f.col("constructorId"),
        f.col("driverId"),
        f.col("grid"),
        f.col("laps"),
        f.col("number"),
        f.col("points"),
        f.col("position"),
        f.col("positionText"),
        f.col("status"),
        f.col("ingestion_timestamp"),
        f.col("source_file"))
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

display(sprints_df)

# COMMAND ----------

sprint_valid_df = ( 
                    sprints_df
                    .filter(
                        f.col("season").isNotNull() &
                        f.col("round").isNotNull() &
                        f.col("constructor_id").isNotNull() &
                        f.col("driver_id").isNotNull() )
)

# COMMAND ----------

display(sprint_valid_df)

# COMMAND ----------

sprints_distinct_df = (
    sprint_valid_df
    .dropDuplicates (["season","round","constructor_id","driver_id"])
)

# COMMAND ----------

display(sprints_distinct_df)

# COMMAND ----------

sprints_final_df = (
    sprints_distinct_df
    .withColumn("race_name",f.initcap(f.col("race_name")))
)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

(
    sprints_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema",True)
    .saveAsTable(silver_table)
)