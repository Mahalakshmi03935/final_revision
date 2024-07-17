# Databricks notebook source
# MAGIC %md
# MAGIC You have a dataset of employees with the following schema: employee_id, department, salary, hire_date. Write a PySpark query to find the average salary for each department, but only for employees hired after January 1, 2020.

# COMMAND ----------


import pyspark.sql.functions as F


data = [
    (1, 'HR', 5000, '2019-01-01'),
    (2, 'HR', 6000, '2021-05-01'),
    (3, 'HR', 4000, '2020-06-01'),
    (4, 'IT', 7000, '2021-01-15'),
    (5, 'IT', 8000, '2020-03-10'),
    (6, 'IT', 6000, '2019-08-20')
]


df = spark.createDataFrame(data, ['employee_id', 'department', 'salary', 'hire_date'])

# Convert hire_date to date type
df = df.withColumn('hire_date', F.to_date('hire_date', 'yyyy-MM-dd'))

# Filter employees hired after January 1, 2020
filtered_df = df.filter(df.hire_date > '2020-01-01')

# Calculate average salary per department
result_df = filtered_df.groupBy('department').agg(F.avg('salary').alias('avg_salary'))

result_df.show()

