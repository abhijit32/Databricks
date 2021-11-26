# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat, desc, sum, count, rank
from pyspark.sql.window import Window 

# COMMAND ----------

# MAGIC %md ###Reading the Datasets

# COMMAND ----------

races_df = spark.read.parquet('/mnt/processed/races')
circuits_df = spark.read.parquet('/mnt/processed/circuits')
drivers_df = spark.read.parquet('/mnt/processed/drivers')
constructors_df = spark.read.parquet('/mnt/processed/constructors')
results_df = spark.read.parquet('/mnt/processed/results')

# COMMAND ----------

# MAGIC %md ###Deriving the final results

# COMMAND ----------

results_final_df = results_df.join(races_df, races_df.race_id == results_df.race_id,'inner') \
                    .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
                    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
                    .join(drivers_df, drivers_df.driver_id == results_df.driver_id) \
                    .select(races_df.race_year, races_df.name.alias('race_name'), \
                            circuits_df.location.alias('circuit_location'), constructors_df.name.alias('team'), drivers_df.name, results_df.points) \
                    .withColumn('created_date',current_timestamp()) \
                    .orderBy(desc('points'))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode('overwrite').parquet('/mnt/transformation/points')

# COMMAND ----------

rs = results_final_df.groupBy('race_year','name','team').agg(sum('points').alias('total_points'))

# COMMAND ----------

display(rs)

# COMMAND ----------

window_schema = Window.partitionBy('race_year').orderBy(desc('total_points'))
final_df = rs.withColumn('rank', rank().over(window_schema))

# COMMAND ----------

display(final_df.filter('race_year >= 2010'))

# COMMAND ----------

final_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/transformation/driver_standings')

# COMMAND ----------


