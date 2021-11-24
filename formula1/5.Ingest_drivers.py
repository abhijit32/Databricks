# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(), True),
                                    StructField('surname', StringType(), True)])

drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), False),
                                    StructField('driverRef', StringType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('code', StringType(), True),
                                    StructField('name', name_schema),
                                    StructField('dob', DateType(), True),
                                    StructField('nationality', StringType(), True),
                                    StructField('url', StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.option('header',True).schema(drivers_schema).json('/mnt/raw/drivers.json')
display(drivers_df)

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('driverRef', 'driver_ref') \
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_renamed_df = drivers_renamed_df.drop(col('url'))

# COMMAND ----------

drivers_final_df = drivers_renamed_df.withColumn('name', concat(col('name.forename'), lit(' '), concat('name.surname')))
display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('/mnt/processed/drivers')

# COMMAND ----------


