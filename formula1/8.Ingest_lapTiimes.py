# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

lapTime_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), False),
                                    StructField('lap', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

laptimes_df = spark.read.option('header',True).option('multiLine', True).schema(lapTime_schema).csv('/mnt/raw/lap_times')
display(laptimes_df)

# COMMAND ----------

laptimes_final_df = laptimes_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('ingestion_date',current_timestamp())
display(laptimes_final_df)

# COMMAND ----------

laptimes_final_df.write.mode('overwrite').parquet('mnt/processed/lap_times')

# COMMAND ----------

dbutils.notebook.exit(f'success run {dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}')

# COMMAND ----------


