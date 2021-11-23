# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/abhijitstorageaccount32/raw")

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.option("inferSchema", True) \
.csv("dbfs:/mnt/abhijitstorageaccount32/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------


