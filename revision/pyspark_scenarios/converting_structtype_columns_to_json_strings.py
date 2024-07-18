# Databricks notebook source
# MAGIC %md
# MAGIC Given a DataFrame df with several columns, convert these columns into a single JSON string column.

# COMMAND ----------

from pyspark.sql.functions import to_json, struct,col

# Sample DataFrame creation (replace with your actual DataFrame)
data = [("Alice", 30), ("Bob", 25)]
schema = ["name", "age"]
df = spark.createDataFrame(data, schema)

# Convert columns to a JSON string column
df_with_json = df.withColumn("json_data", to_json(struct([col(c) for c in df.columns])))

# Show the resulting DataFrame
df_with_json.show()

