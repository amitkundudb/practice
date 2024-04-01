from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder \
    .appName("Explode Array and Maps Functions") \
    .getOrCreate()

data = [("Amit", ["apple", "banana", "cherry"], {"a": 1, "b": 2}),
        ("Suman", ["banana", "orange"], {"c": 3, "d": 4}),
        ("Rahul", ["grapes", "apple", "mango"], {"e": 5}),
        ("Sneha", ["kiwi", "banana", "apple"], {"f": 6}),
        ("Raj", ["orange", "cherry"], {"g": 7, "h": 8}),
        ("Neha", None, None)]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Fruits", ArrayType(StringType()), True),
    StructField("Details", MapType(StringType(), IntegerType()), True)
])

df = spark.createDataFrame(data, schema)

print("Original DataFrame:")
df.show(truncate=False)

exploded_df = df.selectExpr("Name", "explode(Fruits) as Fruit", "Details")
print("\nExploded DataFrame:")
exploded_df.show(truncate=False)

exploded_outer_df = df.selectExpr("Name", "explode_outer(Fruits) as Fruit", "Details")
print("\nExploded Outer DataFrame:")
exploded_outer_df.show(truncate=False)

posexploded_df = df.selectExpr("Name", "posexplode(Fruits) as (Position, Fruit)", "Details")
print("\nPosExploded DataFrame:")
posexploded_df.show(truncate=False)

posexploded_outer_df = df.selectExpr("Name", "posexplode_outer(Fruits) as (Position, Fruit)", "Details")
print("\nPosExploded Outer DataFrame:")
posexploded_outer_df.show(truncate=False)