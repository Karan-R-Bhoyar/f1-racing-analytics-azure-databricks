# Databricks notebook source
# MAGIC %md
# MAGIC #Build Races Daimension
# MAGIC - 1. Read Silver races table
# MAGIC - 2. Read Silver circuits table
# MAGIC - 3. Join data from races & circuits using circuits_id
# MAGIC - 4. Select the required column
# MAGIC         - races.season    
# MAGIC         - races.round
# MAGIC         - races.race_name
# MAGIC         - races.race_date
# MAGIC         - circuits.circuit_name
# MAGIC         - circuit.locality
# MAGIC         - circuit.country
# MAGIC - 5. Write the transform data into gold dim_races table

# COMMAND ----------

# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

table_name = f"{catlog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 Read races and circuit table from silver**

# COMMAND ----------

circuits_df = spark.table(f"{catlog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catlog_name}.{silver_schema}.races")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 join races and circuits table**
# MAGIC - 4. Select the required column
# MAGIC         - races.season    
# MAGIC         - races.round
# MAGIC         - races.race_name
# MAGIC         - races.race_date
# MAGIC         - circuits.circuit_name
# MAGIC         - circuit.locality
# MAGIC         - circuit.country

# COMMAND ----------

dim_races = (
    races_df
        .join(circuits_df,
              races_df.circuit_id == circuits_df.circuit_id,
              "inner"
            )
        .select (
            races_df.season,
            races_df.round,
            races_df.race_name,
            races_df.race_date,
            circuits_df.circuit_name,
            circuits_df.locality,
            circuits_df.country
        )
)


# COMMAND ----------

display(dim_races)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 write data into gold layer**

# COMMAND ----------

(
    dim_races
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from formula1.gold.dim_races;