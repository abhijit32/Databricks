# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

 race_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                     StructField('year', IntegerType(), True),
                                     StructField('round', IntegerType(), True),
                                     StructField('circuitId', IntegerType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('date', DateType(), True),
                                     StructField('time', StringType(), True),
                                     StructField('url', StringType(), True)])

# COMMAND ----------

race_df = spark.read.option("header",True).schema(race_schema).csv('/mnt/raw/races.csv')
display(race_df)

# COMMAND ----------

race_df = race_df.drop(col('url'))

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_renamed_df = race_df.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('year','race_year') \
.withColumnRenamed('circuitId','circuit_id') \
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(race_renamed_df)

# COMMAND ----------

race_final_df = race_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

race_final_df = race_final_df.drop('date').drop('time')

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

race_final_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/processed/races')

# COMMAND ----------


