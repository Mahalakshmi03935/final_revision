# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Merge tow dataframes").getOrCreate()

# COMMAND ----------

Data = [('1','Maha','Switzerland'),
        ('2','lakshmi','London')]
Columns = ["id","Name","City"]
df1= spark.createDataFrame(data = Data, schema = Columns)
df1.show()


# COMMAND ----------

Data1 = [(2,'Ammu'),
        (3,'Adhithya')]
Columns1 = ["id","Name"]
df2 = spark.createDataFrame(data = Data1,schema = Columns1)
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We have 2 dataframes with different number of columns , so union cannot be performed here, so we are adding one column in dataframe2 , so that 2 dataframes have same number of columns ,then we can perform union to merge both dataframes

# COMMAND ----------

df2 = df2.withColumn("City",lit("null"))
df2.show()

# COMMAND ----------

df = df1.union(df2)
df.show()
