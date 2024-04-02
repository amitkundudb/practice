from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.appName("JSON Parsing").getOrCreate()
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)
])

json_string = '{"name": "Amit", "age": "23"}'

# JSON string to DataFrame
df = spark.createDataFrame([(json_string,)], ["value"]) \
    .select(from_json("value", schema).alias("data"))

df.show()
