# Databricks notebook source
# MAGIC %md
# MAGIC "List All Customers and Their Orders, Including Those Without Orders, Using Left Outer Join in PySpark"

# COMMAND ----------

# MAGIC %md
# MAGIC  You have a dataset of customers (customer_id, name, email) and another dataset of orders (order_id, customer_id, order_date, amount). Write a PySpark query to get a list of all customers and their orders, including customers who have not placed any orders.

# COMMAND ----------


customer_data = [
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com'),
    (3, 'Alice Brown', 'alice@example.com')
]

order_data = [
    (101, 1, '2024-01-01', 100.0),
    (102, 1, '2024-01-03', 150.0),
    (103, 2, '2024-01-02', 200.0)
]


customer_df = spark.createDataFrame(customer_data, ['customer_id', 'name', 'email'])
order_df = spark.createDataFrame(order_data, ['order_id', 'customer_id', 'order_date', 'amount'])

# left outer join
result_df = customer_df.join(order_df, on='customer_id', how='left')

result_df.show()

