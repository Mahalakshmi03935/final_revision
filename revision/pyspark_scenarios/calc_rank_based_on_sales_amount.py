# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


spark = SparkSession.builder.appName("RankBasedOnSales").getOrCreate()


sales_data = [("Product A", 100), ("Product B", 150), ("Product A", 200), ("Product B", 100), ("Product A", 150)]
columns = ["product", "sales"]

df = spark.createDataFrame(sales_data, columns)

# Define window specification
window_spec = Window.partitionBy("product").orderBy(df.sales.desc())

# Calculate rank
df = df.withColumn("rank", rank().over(window_spec))

df.show()

