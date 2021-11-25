# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('constructorId', IntegerType(), False),
                                    StructField('number', IntegerType(), True),
                                    StructField('grid', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionText', StringType(), True),
                                   StructField('positionOrder', IntegerType(), True),
                                   StructField('points', FloatType(), True),
                                   StructField('laps', IntegerType(), True),
                                   StructField('time', StringType(), True),
                                   StructField('milliseconds', IntegerType(), True),
                                   StructField('fastestLap', IntegerType(), True),
                                   StructField('rank', IntegerType(), True),
                                   StructField('fastestLapTime', StringType(), True),
                                   StructField('fastestLapSpeed', StringType(), True),
                                   StructField('statusId', IntegerType(), True)])

# COMMAND ----------

results_df = spark.read.option('header',True).schema(results_schema).json('/mnt/raw/results.json')
display(results_df)

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed('resultId', 'result_id') \
.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id') \
.withColumnRenamed('positionText','position_text') \
.withColumnRenamed('positionOrder','position_order') \
.withColumnRenamed('fastestLap','fastest_lap') \
.withColumnRenamed('fastestLapTime','fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

results_final_df = results_renamed_df.drop(col('statusId'))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/processed/results')

# COMMAND ----------

dbutils.notebook.exit(f'success run {dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}')

# COMMAND ----------


