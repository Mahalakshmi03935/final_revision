# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("JSONExample").getOrCreate()


json_data = [
    ('{"emp_name":"maha","department":"HR","salary":2000,"years_of_experience":5,"bonus":500}',),
    ('{"emp_name":"swetha","department":"IT","salary":3000,"years_of_experience":3,"bonus":1000}',),
    ('{"emp_name":"ramnesh","department":"HR","salary":1500,"years_of_experience":2,"bonus":300}',),
    ('{"emp_name":"sesha","department":"payroll","salary":3500,"years_of_experience":4,"bonus":700}',),
    ('{"emp_name":"viswa","department":"IT","salary":3000,"years_of_experience":6,"bonus":1200}',)
]

schema = StructType([
    StructField("emp_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("years_of_experience", IntegerType(), True),
    StructField("bonus", IntegerType(), True)
])


json_df = spark.createDataFrame(json_data, ["json_string"])


print("Original JSON DataFrame:")
json_df.show(truncate=False)


structured_df = json_df.withColumn("data", from_json(col("json_string"), schema)).select("data.*")


print("Structured DataFrame:")
structured_df.show()


structured_df = structured_df.withColumn("total_compensation", col("salary") + col("bonus"))


print("DataFrame with total_compensation:")
structured_df.show()

# Convert structured DataFrame back to JSON string column
# Used 'struct' to convert multiple columns back to a single struct column before applying to_json
json_again_df = structured_df.withColumn("json_string", to_json(struct([col(c) for c in structured_df.columns])))

# Show the DataFrame with JSON string column
print("DataFrame with JSON string column:")
json_again_df.show(truncate=False)

