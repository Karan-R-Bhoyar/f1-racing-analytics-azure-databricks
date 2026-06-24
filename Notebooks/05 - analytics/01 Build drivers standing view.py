# Databricks notebook source
# MAGIC %md
# MAGIC #Build Drivers Standing
# MAGIC ###Sources
# MAGIC
# MAGIC - 1.fact_session_results
# MAGIC - 2.dim_drivers
# MAGIC
# MAGIC ###Output Column
# MAGIC
# MAGIC - season
# MAGIC - driverid
# MAGIC - driver name
# MAGIC - nationality
# MAGIC - racestart
# MAGIC - total point
# MAGIC - no of wins
# MAGIC - no of podium
# MAGIC - standing position

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_driver_standing
# MAGIC AS
# MAGIC     WITH driver_session_summary
# MAGIC     AS (
# MAGIC         SELECT 
# MAGIC             s.season,
# MAGIC             d.driver_id,
# MAGIC             d.driver_name,
# MAGIC             d.nationality,
# MAGIC             COUNT(*) as race_start,
# MAGIC             SUM(s.points) as total_points,
# MAGIC             count_if(s.is_witn) as no_of_wins,
# MAGIC             count_if(s.is_podium) as no_of_podiums
# MAGIC         FROM formula1.gold.fact_session_results_df as s 
# MAGIC         JOIN formula1.gold.dim_drivers as d
# MAGIC             On s.driver_id = d.driver_id
# MAGIC             GROUP BY 
# MAGIC             s.season, 
# MAGIC             d.driver_id,
# MAGIC             d.driver_name,
# MAGIC             d.nationality )
# MAGIC     SELECT season,
# MAGIC         driver_id,
# MAGIC         driver_name,
# MAGIC         nationality,
# MAGIC         rank() over ( partition by season order by total_points DESC, no_of_wins DESC) as standing,
# MAGIC         race_start,
# MAGIC         no_of_wins,
# MAGIC         no_of_podiums,
# MAGIC         total_points
# MAGIC     FROM driver_session_summary
# MAGIC     ORDER BY season DESC,total_points DESC, no_of_wins DESC
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.v_driver_standing;