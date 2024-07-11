# Databricks notebook source
df = spark.range(3)
df.show()
from pyspark.sql.functions import current_date,date_format,to_date
df1 = df.withColumn('CurrentDate',current_date())
df1.show()
df1.printSchema()

# COMMAND ----------

df2=df1.withColumn('CurrentDate',date_format(df1.CurrentDate,'yyyy.MM.dd'))
df2.show()
df2.printSchema()

# COMMAND ----------

df3 = df2.withColumn('CurrentDate',to_date(df2.CurrentDate,'yyyy.MM.dd'))
df3.show()
df3.printSchema()
