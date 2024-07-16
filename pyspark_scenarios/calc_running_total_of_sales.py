# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum


spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()


sales_data = [("Product A", 100), ("Product B", 150), ("Product A", 200), ("Product B", 100), ("Product A", 150)]
columns = ["product", "sales"]

df = spark.createDataFrame(sales_data, columns)

# Define window specification
window_spec = Window.partitionBy("product").orderBy("sales").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate running total
df = df.withColumn("running_total", sum("sales").over(window_spec))

df.show()

