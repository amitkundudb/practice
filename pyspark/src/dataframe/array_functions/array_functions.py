from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, array_position, array_remove
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Array Functions").getOrCreate()

data = [("Amit", ["apple", "banana", "cherry"]),
        ("Suman", ["banana", "orange"]),
        ("Rahul", ["grapes", "apple", "mango"]),
        ("Sneha", ["kiwi", "banana", "apple"]),
        ("Raj", ["orange", "cherry"])]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Fruits", ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)

def calculate_array_length(arr):
    if arr:
        return len(arr)
    else:
        return 0

array_length_udf = udf(calculate_array_length, IntegerType())

result_df = ((((df.withColumn("array_column", array("Fruits"))
             .withColumn("array_contains_banana", array_contains("Fruits", "banana")))
             .withColumn("array_length", array_length_udf("Fruits")))
             .withColumn("array_position_apple", array_position("Fruits", "apple")))
             .withColumn("array_remove_banana", array_remove("Fruits", "banana")))

result_df.show(truncate=False)
