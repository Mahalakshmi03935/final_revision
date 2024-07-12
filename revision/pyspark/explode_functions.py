from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer

# Create a Spark session
spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

# Sample data
data = [
    (1, [1, 2, 3], {"a": 1, "b": 2}),
    (2, [], {"c": 3, "d": 4}),
    (3, None, None)
]

# Schema
schema = ["id", "array_col", "map_col"]

# Create a DataFrame
df = spark.createDataFrame(data, schema)
df.show(truncate=False)

# Using explode()
df_explode = df.select("id", explode("array_col").alias("exploded_array"))
df_explode.show(truncate=False)

# Using explode_outer()
df_explode_outer = df.select("id", explode_outer("array_col").alias("exploded_outer_array"))
df_explode_outer.show(truncate=False)

# Using posexplode()
df_posexplode = df.select("id", posexplode("array_col").alias("pos", "posexploded_array"))
df_posexplode.show(truncate=False)

# Using posexplode_outer()
df_posexplode_outer = df.select("id", posexplode_outer("array_col").alias("pos", "posexploded_outer_array"))
df_posexplode_outer.show(truncate=False)

# Using explode() with map_col
df_explode_map = df.select("id", explode("map_col").alias("key", "value"))
df_explode_map.show(truncate=False)

# Using explode_outer() with map_col
df_explode_outer_map = df.select("id", explode_outer("map_col").alias("key", "value"))
df_explode_outer_map.show(truncate=False)
