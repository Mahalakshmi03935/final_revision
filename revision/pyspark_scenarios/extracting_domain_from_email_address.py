# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()


data = [("maha.lakshmi@example.com",), ("padmalaya.seshadri@domain.com",), ("hemanth.krown@website.org",)]
columns = ["email"]

df = spark.createDataFrame(data, columns)

# Extract domain from email addresses
df = df.withColumn("domain", regexp_extract("email", r"@(\w+\.\w+)", 1))

df.show()

