# Databricks notebook source
# MAGIC %md
# MAGIC  You have a dataset of product sales with the following schema: product_id, year, sales. Write a PySpark query to pivot the data so that each row represents a product, and columns represent sales for each year.

# COMMAND ----------


import pyspark.sql.functions as F



data = [
    ('prod1', 2020, 1000),
    ('prod1', 2021, 1500),
    ('prod2', 2020, 2000),
    ('prod2', 2021, 2500),
    ('prod3', 2020, 3000),
    ('prod3', 2021, 3500)
]

df = spark.createDataFrame(data, ['product_id', 'year', 'sales'])

# Pivot
pivot_df = df.groupBy('product_id').pivot('year').agg(F.sum('sales'))

pivot_df.show()

