# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                       StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('constructorId', IntegerType(), False),
                                    StructField('number', IntegerType(), False),
                                    StructField('position', IntegerType(), True),
                                    StructField('q1', StringType(), True),
                                    StructField('q2', StringType(), True),
                                      StructField('q3', StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read.option('header',True).option('multiLine', True).schema(qualifying_schema).json('/mnt/raw/qualifying/*.json')
display(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id') \
.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet('/mnt/processed/qualifying')

# COMMAND ----------


