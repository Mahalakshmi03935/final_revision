# Databricks notebook source
import pyspark.sql.functions as F

# Sample DataFrames
emp_data = spark.createDataFrame([
    ("Alice", 10, 5000),
    ("Bob", 20, 6000),
    ("Charlie", 30, 7000),
    ("David", 40, 8000)  
], ["name", "emp_id", "salary"])

dept_data = spark.createDataFrame([
    (10, "Sales"),
    (20, "Marketing"),
    (40, "Engineering")
], ["emp_dept_id", "department"])

# Inner Join (default)
joined_df_inner = emp_data.join(dept_data, on="emp_id")  

# Left Join
joined_df_left = emp_data.join(dept_data, on="emp_id", how="left")  

# Right Join
joined_df_right = emp_data.join(dept_data, on="emp_id", how="right") 

# Outer Join
joined_df_outer = emp_data.join(dept_data, on="emp_id", how="full")  

# Semi Join
joined_df_semi = emp_data.join(dept_data.select("emp_dept_id"), on="emp_id", how="leftsemi")  

# Anti Join
joined_df_anti = emp_data.join(dept_data.select("emp_dept_id"), on="emp_id", how="leftanti")  


print("Inner Join results:")
joined_df_inner.show()

print("\nLeft Join results:")
joined_df_left.show()

print("\nRight Join results:")
joined_df_right.show()

print("\nOuter Join results:")
joined_df_outer.show()

print("\nSemi Join results:")
joined_df_semi.show()

print("\nAnti Join results:")
joined_df_anti.show()

