# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Given a DataFrame df with columns year, product, and sales for each quarter (Q1, Q2, Q3, Q4), unpivot the DataFrame so that you have a quarter column and a sales column

# COMMAND ----------


from pyspark.sql.functions import expr


data = [(2020, "Product1", 100, 200, 300, 400), (2021, "Product2", 150, 250, 350, 450)]
schema = ["year", "product", "Q1", "Q2", "Q3", "Q4"]
df = spark.createDataFrame(data, schema)

# Unpivot 
unpivoted_df = df.selectExpr("year", "product", "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, sales)")

unpivoted_df.show()

