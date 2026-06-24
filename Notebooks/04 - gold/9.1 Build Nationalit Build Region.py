# Databricks notebook source
# MAGIC %run "../00 - Common/01. Environment config"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

target_table = f"{catlog_name}.{gold_schema}.ref_nationality_referance"

# COMMAND ----------

from pyspark.sql import Row

nationality_region_map_rows = [
    # Europe
    Row(nationality="British",      region="Europe"),
    Row(nationality="Argentine-italian",      region="Europe"),
    Row(nationality="American-italian",      region="Europe"),
    Row(nationality="German",       region="Europe"),
    Row(nationality="Spanish",      region="Europe"),
    Row(nationality="Finnish",      region="Europe"),
    Row(nationality="French",       region="Europe"),
    Row(nationality="Italian",      region="Europe"),
    Row(nationality="Austrian",     region="Europe"),
    Row(nationality="Dutch",        region="Europe"),
    Row(nationality="Belgian",      region="Europe"),
    Row(nationality="Swiss",        region="Europe"),
    Row(nationality="Swedish",      region="Europe"),
    Row(nationality="Danish",       region="Europe"),
    Row(nationality="Hungarian",    region="Europe"),
    Row(nationality="Portuguese",   region="Europe"),
    Row(nationality="Polish",       region="Europe"),
    Row(nationality="Czech",        region="Europe"),
    Row(nationality="Monegasque",   region="Europe"),
    Row(nationality="Irish",        region="Europe"),
    Row(nationality="Scottish",     region="Europe"),
    Row(nationality="Norwegian",    region="Europe"),
    Row(nationality="Greek",        region="Europe"),
    Row(nationality="Romanian",     region="Europe"),
    Row(nationality="Russian",      region="Europe"),

    # North America
    Row(nationality="American",     region="North America"),
    Row(nationality="Canadian",     region="North America"),
    Row(nationality="Mexican",      region="North America"),

    # South America
    Row(nationality="Brazilian",    region="South America"),
    Row(nationality="Argentine",    region="South America"),
    Row(nationality="Colombian",    region="South America"),
    Row(nationality="Venezuelan",   region="South America"),
    Row(nationality="Chilean",      region="South America"),
    Row(nationality="Uruguayan",    region="South America"),

    # Asia
    Row(nationality="Japanese",     region="Asia"),
    Row(nationality="Chinese",      region="Asia"),
    Row(nationality="Indian",       region="Asia"),
    Row(nationality="Thai",         region="Asia"),
    Row(nationality="Malaysian",    region="Asia"),
    Row(nationality="Indonesian",   region="Asia"),

    # Middle East
    Row(nationality="Bahraini",     region="Middle East"),
    Row(nationality="Saudi",        region="Middle East"),
    Row(nationality="Emirati",      region="Middle East"),

    # Oceania
    Row(nationality="Australian",   region="Oceania"),
    Row(nationality="New Zealander",region="Oceania"),

    # Africa
    Row(nationality="South African",region="Africa"),
    Row(nationality="Moroccan",     region="Africa"),
]

ref_nationality_region_df = spark.createDataFrame(nationality_region_map_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write dataframe in gold layer**

# COMMAND ----------

(
    ref_nationality_region_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.ref_nationality_referance;