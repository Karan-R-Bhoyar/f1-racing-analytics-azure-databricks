-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the Project Environment for F1 Project
-- MAGIC 1. Create extreal location databricks-course-ext-dl-formula1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl_formula1
URL 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Catalog for formula 1

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1
MANAGED LOCATION 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/'
COMMENT 'This is the main catalog for formula project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create schema Landing, Bronze, Silver & Gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/gold';

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://formula1@databrickssourceextdl.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files