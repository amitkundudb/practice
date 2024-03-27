from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Dataset to DataFrame Example").getOrCreate()

# Defining the schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Creating a dataset
data = [("Amit", 23), ("Avijit", 20), ("Rahul", 30)]
# Converting the dataset into a DataFrame
df = spark.createDataFrame(data, schema)

df.show()