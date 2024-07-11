# Databricks notebook source
num = [1,2,3,4,5,6,7,8,9,10]
res = list(map(lambda x : x * 2,num))
print(res)

# COMMAND ----------

from functools import reduce
numbers = [2,4,6,7,8,999,9.999]
result =reduce(lambda x,y:x+y,numbers)
print(result)

# COMMAND ----------

numbers = [567,897,99,908,7.99,9.87]
result = list(map(lambda x : x+2,numbers))
print(result)

# COMMAND ----------

num = (1,2,3,4,5,6,7,8,9,10,11,12,15,18,20,66)
filtered_num = filter(lambda x : x>5,num)
res = reduce(lambda x,y :x+y, filtered_num)
print(res)

# COMMAND ----------

numbers = [1, 2, 3, 4, 5]
squared_numbers = map(lambda x: x ** 2, numbers)
print(list(squared_numbers))

# COMMAND ----------

numbers = [1, 2, 3, 4, 5, 6]
even_numbers = filter(lambda x: x % 2 == 0, numbers)
print(list(even_numbers))
