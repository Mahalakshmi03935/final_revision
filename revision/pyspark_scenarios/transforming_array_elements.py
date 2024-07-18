# Databricks notebook source
# MAGIC %md
# MAGIC You have a DataFrame df with a column numbers containing arrays of integers. Use the transform function to create a new column where each array element is squared.

# COMMAND ----------


from pyspark.sql.functions import col, expr

data = [(1, [1, 2, 3]), (2, [4, 5, 6])]
schema = ["id", "numbers"]
df = spark.createDataFrame(data, schema)

# Transform the array elements
df_with_transformed = df.withColumn("squared_numbers", expr("transform(numbers, x -> x * x)"))

df_with_transformed.show()

