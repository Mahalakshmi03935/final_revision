# Databricks notebook source
# MAGIC %md
# MAGIC  You have a dataset of sales transactions with the following schema: transaction_id, product_id, quantity, price, discount. Some rows have missing values in the discount column. Write a PySpark query to fill missing discount values with 0 and calculate the total revenue (quantity * price * (1 - discount)) for each product.

# COMMAND ----------


import pyspark.sql.functions as F

data = [
    (1, 'prod1', 10, 100, 0.1),
    (2, 'prod2', 5, 200, None),
    (3, 'prod1', 7, 100, 0.05),
    (4, 'prod3', 3, 300, None),
    (5, 'prod2', 2, 200, 0.2)
]

df = spark.createDataFrame(data, ['transaction_id', 'product_id', 'quantity', 'price', 'discount'])

# Fill missing discount values with 0
df = df.fillna({'discount': 0})

# Calculate total revenue
df = df.withColumn('total_revenue', df.quantity * df.price * (1 - df.discount))

# Group by product_id and calculate total revenue per product
result_df = df.groupBy('product_id').agg(F.sum('total_revenue').alias('total_revenue'))

result_df.show()

