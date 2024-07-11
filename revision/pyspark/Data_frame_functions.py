# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("DataFrame Func").getOrCreate()

data = [(1, "Maha", 28),
        (2, "Ammu", 25),
        (3, "swetha", 33),
        (4, "Ramnesh", 24),
        (5, "Leela", 35)]

schema = ["id", "name", "age"]

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

# count()
df_count = df.count()
print("Number of rows:", df_count)

# select()
df_select = df.select('name', 'age')
print("Selecting name and age columns:")
df_select.show()

# filter() & where()
df_filter = df.filter(df['age'] > 30)
print("Filtering rows where age > 30:")
df_filter.show()

# like()
df_like = df.filter(df['name'].like('%ali%'))
print("Filtering rows where name contains 'ali':")
df_like.show()

# describe()
df_describe = df.describe()
print("Describing DataFrame:")
df_describe.show()

# columns()
df_columns = df.columns
print("Column names:", df_columns)

# when() & otherwise()
df_when = df.withColumn('category', when(df['age'] < 30, 'Young').otherwise('Old'))
print("Applying when() & otherwise() to add category column:")
df_when.show()

# alias()
df_alias = df.alias('employee')
print("Alias DataFrame as 'employee':")
df_alias.show()

# orderBy() & sort()
df_orderBy = df.orderBy('age', ascending=False)
print("Sorting DataFrame by age in descending order:")
df_orderBy.show()

# groupBy() & agg()
df_groupBy = df.groupBy('age').agg({'id': 'count'})
print("Grouping by age and counting IDs:")
df_groupBy.show()
