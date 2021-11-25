# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

pitStops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('stop', IntegerType(), False),
                                    StructField('lap', IntegerType(), False),
                                    StructField('time', StringType(), False),
                                    StructField('duration', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

pitStops_df = spark.read.option('header',True).option('multiLine', True).schema(pitStops_schema).json('/mnt/raw/pit_stops.json')
display(pitStops_df)

# COMMAND ----------

pitStops_final_df = pitStops_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id') \
.withColumn('ingestion_date',current_timestamp())

display(pitStops_final_df)

# COMMAND ----------

pitStops_final_df.write.mode('overwrite').parquet('mnt/processed/pit_stops')

# COMMAND ----------


