# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

 circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False),
                                     StructField('circuitRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('location', StringType(), True),
                                     StructField('country', StringType(), True),
                                     StructField('lat', DoubleType(), True),
                                     StructField('lang', DoubleType(), True),
                                     StructField('alt', IntegerType(), True),
                                     StructField('url', StringType(), True)])

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.schema(circuits_schema) \
.csv("dbfs:/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()


# COMMAND ----------

circuits_selected_df =  circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lang'), col('alt'))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df =  circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
.withColumnRenamed('circuitRef', 'circuit_ref') \
.withColumnRenamed('lat', 'latitude') \
.withColumnRenamed('lang', 'longitude') \
.withColumnRenamed('alt', 'altitude') \
        

# COMMAND ----------

display (circuits_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp()) \
.withColumn('environment',lit('production'))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet('mnt/processed/circuits')

# COMMAND ----------

dbutils.fs.ls('/mnt/processed/circuits')

# COMMAND ----------

display(spark.read.parquet('/mnt/processed/circuits'))

# COMMAND ----------


