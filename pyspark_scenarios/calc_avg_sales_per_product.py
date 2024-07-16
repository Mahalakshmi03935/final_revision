# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


spark = SparkSession.builder.appName("AggregateFunctions").getOrCreate()


sales_data = [("Product A", 100), ("Product B", 150), ("Product A", 200), ("Product B", 100), ("Product A", 150)]
columns = ["product", "sales"]

df = spark.createDataFrame(sales_data, columns)

# Calculate average sales per product
df_avg_sales = df.groupBy("product").agg(avg("sales").alias("average_sales"))

df_avg_sales.show()

