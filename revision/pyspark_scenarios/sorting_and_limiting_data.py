# Databricks notebook source
# MAGIC %md
# MAGIC You have a dataset of products with the following schema: product_id, category, price, rating. Write a PySpark query to find the top 3 most expensive products in each category.

# COMMAND ----------


from pyspark.sql.window import Window
import pyspark.sql.functions as F


data = [
    ('prod1', 'cat1', 100, 4.5),
    ('prod2', 'cat1', 200, 4.0),
    ('prod3', 'cat1', 150, 4.2),
    ('prod4', 'cat2', 300, 4.8),
    ('prod5', 'cat2', 250, 4.6),
    ('prod6', 'cat2', 400, 4.9)
]


df = spark.createDataFrame(data, ['product_id', 'category', 'price', 'rating'])

# Define window specification
window_spec = Window.partitionBy('category').orderBy(F.desc('price'))

# Add rank column
df = df.withColumn('rank', F.rank().over(window_spec))

# Filter top 3 most expensive products in each category
result_df = df.filter(df.rank <= 3).drop('rank')

result_df.show()

