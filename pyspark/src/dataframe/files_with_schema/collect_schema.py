from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()

data = [("Amit", 23), ("Ahana", 30), ("Anuj", 35)]

# schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Creating DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

# Collecting data as a python list
collected_data = df.collect()
print("Data collected using collect():", collected_data)

# Print schema
print("DataFrame schema:")
df.printSchema()