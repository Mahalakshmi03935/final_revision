# Databricks notebook source
from pyspark.sql.functions import rank, dense_rank, row_number, avg, sum
from pyspark.sql.window import Window

# Employee data with two new columns: years_of_experience and bonus
employee_data = [
    ('maha', 'HR', 2000, 5, 500), ('swetha', 'IT', 3000, 3, 1000), ('ramnesh', 'HR', 1500, 2, 300), 
    ('sesha', 'payroll', 3500, 4, 700), ('viswa', 'IT', 3000, 6, 1200), ('leela', 'IT', 4000, 7, 1500),
    ('tharani', 'payroll', 2000, 1, 400), ('divya', 'IT', 2000, 2, 500), ('hemanth', 'HR', 2000, 3, 400), 
    ('krishna', 'IT', 2500, 4, 800)
]

# Define schema
employee_schema = ["emp_name", "department", "salary", "years_of_experience", "bonus"]

# Create DataFrame
df = spark.createDataFrame(employee_data, employee_schema)
df.show()

# Define window spec
window = Window.partitionBy('department').orderBy('salary')

# Apply window functions
df.withColumn('rowNumber', row_number().over(window)) \
  .withColumn('rank', rank().over(window)) \
  .withColumn('denseRank', dense_rank().over(window)) \
  .withColumn('avg_experience', avg('years_of_experience').over(window)) \
  .withColumn('total_bonus', sum('bonus').over(window)) \
  .show()

