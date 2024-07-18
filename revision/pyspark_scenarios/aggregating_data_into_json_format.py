# Databricks notebook source
from pyspark.sql.functions import to_json, struct,col

data = [("Alice", 30, "F"), ("Bob", 25, "M")]
schema = ["name", "age", "gender"]
df = spark.createDataFrame(data, schema)

# Convert columns to a JSON string column
df_with_json = df.withColumn("json_data", to_json(struct([col(c) for c in df.columns])))

# Show the resulting DataFrame
df_with_json.show()

