# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("String_Functions").getOrCreate()

data = [("Maha  ","Lakshmi", "Switzerland"),
        ("Divya", "Darshini  ", "England"),
        ("Hemanth", "Krishna", "Chicago")]


schema = ["first_name", "last_name", "city"]


df = spark.createDataFrame(data, schema)


print("DataFrame is:")
df.show()

# upper()
df_upper = df.withColumn("first_name_upper", upper(col("first_name")))
print("Applying upper() function:")
df_upper.show()

# trim()
df_trim = df.withColumn("first_name_trimmed", trim(col("first_name")))
print("Applying trim() function:")
df_trim.show()

# ltrim()
df_ltrim = df.withColumn("first_name_ltrimmed", ltrim(col("first_name")))
print("Applying ltrim() function:")
df_ltrim.show()

# rtrim()
df_rtrim = df.withColumn("last_name_rtrimmed", rtrim(col("last_name")))
print("Applying rtrim() function:")
df_rtrim.show()

# translate()
df_translate = df.withColumn("city_translated", translate(col("city"), "aeiou", "12345"))
print("Applying translate() function:")
df_translate.show()

# substring_index()
df_substring_index = df.withColumn("first_name_substring", substring_index(col("first_name"), " ", 1))
print("Applying substring_index() function:")
df_substring_index.show()

# substring()
df_substring = df.withColumn("last_name_substring", substring(col("last_name"), 2, 4))
print("Applying substring() function:")
df_substring.show()

# split()
df_split = df.withColumn("city_split", split(col("city"), " "))
print("Applying split() function:")
df_split.show(truncate=False)

# repeat()
df_repeat = df.withColumn("last_name_repeated", repeat(col("last_name"), 2))
print("Applying repeat() function:")
df_repeat.show()

# rpad()
df_rpad = df.withColumn("first_name_rpad", rpad(col("first_name"), 15, "_"))
print("Applying rpad() function:")
df_rpad.show()

# lpad()
df_lpad = df.withColumn("last_name_lpad", lpad(col("last_name"), 10, "_"))
print("Applying lpad() function:")
df_lpad.show()

# regex_replace()
df_regex_replace = df.withColumn("city_replaced", regexp_replace(col("city"), "New", "Old"))
print("Applying regex_replace() function:")
df_regex_replace.show()

# lower()
df_lower = df.withColumn("last_name_lower", lower(col("last_name")))
print("Applying lower() function:")
df_lower.show()

# regex_extract()
df_regex_extract = df.withColumn("first_name_extracted", regexp_extract(col("first_name"), "\w+", 0))
print("Applying regex_extract() function:")
df_regex_extract.show()

# length()
df_length = df.withColumn("first_name_length", length(col("first_name")))
print("Applying length() function:")
df_length.show()

# Stop the Spark session
spark.stop()

