# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

constructors_schema = StructType(fields=[StructField('constructorId', IntegerType(), False),
                                     StructField('constructorRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('nationality', StringType(), True),
                                     StructField('url', StringType(), True)   ])

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).option('header',True).json('/mnt/raw/constructors.json')
display(constructors_df)

# COMMAND ----------

constructors_df = constructors_df.drop(col('url'))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructor_renamed_df = constructors_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

constructor_final_df = constructor_renamed_df.withColumn('ingestion_date',current_timestamp())
display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/processed/constructors')

# COMMAND ----------


