# Databricks notebook source
# MAGIC %md
# MAGIC #Build Constructos Standing
# MAGIC ###Sources
# MAGIC - fact_session_results
# MAGIC - dim_constructors
# MAGIC ###Output Column
# MAGIC - 1.season
# MAGIC - 2. constructors_id
# MAGIC - 3. constructos_name
# MAGIC - 4. nationality
# MAGIC - 5. race_starts
# MAGIC - 6. total_points
# MAGIC - 7. no_of_wins
# MAGIC - 8. no_of_podiums
# MAGIC - 9. standing postion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_constructors_standing
# MAGIC AS
# MAGIC WITH constructors_session_summary
# MAGIC AS (
# MAGIC     SELECT 
# MAGIC         r.season,
# MAGIC         c.constructor_id,
# MAGIC         c.constructor_name,
# MAGIC         c.nationality,
# MAGIC         count(*) as race_starts,
# MAGIC         sum(r.points) as total_points,
# MAGIC         count_if(is_witn) as no_win,
# MAGIC         count_if(is_podium) as no_of_podium 
# MAGIC     FROM formula1.gold.fact_session_results_df AS r
# MAGIC     JOIN formula1.gold.dim_constructors AS c
# MAGIC     ON r.constructor_id = c.constructor_id
# MAGIC     GROUP BY 
# MAGIC         r.season,
# MAGIC         c.constructor_id,
# MAGIC         c.constructor_name,
# MAGIC         c.nationality)
# MAGIC SELECT 
# MAGIC     season,
# MAGIC     constructor_id,
# MAGIC     constructor_name,
# MAGIC     nationality,
# MAGIC     race_starts,
# MAGIC     total_points,
# MAGIC     RANK() OVER (PARTITION BY season ORDER BY total_points DESC, no_win DESC) AS standing_position,
# MAGIC     no_win,
# MAGIC     no_of_podium
# MAGIC FROM constructors_session_summary
# MAGIC ORDER BY season DESC, total_points DESC, no_win DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.v_constructors_standing;