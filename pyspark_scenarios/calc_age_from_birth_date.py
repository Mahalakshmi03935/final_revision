# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, datediff, year


spark = SparkSession.builder.appName("DateFunctions").getOrCreate()


data = [("Maha Lakshmi", "1980-05-15"), ("Hemanth Krishna", "1990-08-25"), ("Leela Vedhanayagi", "2000-11-30")]
columns = ["name", "birthdate"]

df = spark.createDataFrame(data, columns)

# Calculate age
df = df.withColumn("birthdate", df.birthdate.cast("date"))
df = df.withColumn("age", (datediff(current_date(), df.birthdate) / 365).cast("int"))

df.show()

