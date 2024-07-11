# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce


spark = SparkSession.builder.appName("NullsAndDuplicatesExample").getOrCreate()


data = [
    (1, "Ammu", 2000, None),
    (2, "Maha", None, 300),
    (3, "lakshmi", 2000, None),
    (None, "saro", 4000, 500),
    (4, "chenn", None, 300),
    (5, None, 5000, 700)
]


schema = ["id", "name", "salary", "bonus"]


df = spark.createDataFrame(data, schema)
print("Original DataFrame:")
df.show()

# Filtering out rows with NULL values in specific columns
filtered_df = df.na.drop(subset=["id", "name"])
print("Filtered DataFrame (removed rows with NULL id or name):")
filtered_df.show()

# Replaced NULL values with specified values
filled_df = filtered_df.fillna({"salary": 0, "bonus": 100})
print("DataFrame after filling NULLs:")
filled_df.show()

# Using coalesce to choose non-null values
coalesced_df = filled_df.withColumn("final_bonus", coalesce(col("bonus"), col("salary")))
print("DataFrame after coalesce:")
coalesced_df.show()

# Removed duplicates using all columns
distinct_all_df = coalesced_df.distinct()
print("DataFrame after removing duplicates (all columns):")
distinct_all_df.show()

# Removed duplicates using specific columns
distinct_specific_df = coalesced_df.dropDuplicates(["name", "salary"])
print("DataFrame after removing duplicates (name, salary):")
distinct_specific_df.show()

