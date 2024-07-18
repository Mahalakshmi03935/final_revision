# Databricks notebook source
# MAGIC %md
# MAGIC You have a DataFrame df with a column event_date containing dates. Add 30 days to each date and create a new column event_plus_30.

# COMMAND ----------

from pyspark.sql.functions import col, date_add

data = [("2024-07-18",), ("2023-05-15",)]
schema = ["event_date"]
df = spark.createDataFrame(data, schema)

# Adding 30 days
df_with_event_plus_30 = df.withColumn("event_plus_30", date_add(col("event_date"), 30))

# Show the DataFrame with event_plus_30
df_with_event_plus_30.show()

