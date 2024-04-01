from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder.appName("Custom JSON Schema").getOrCreate()

custom_schema = StructType([
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("streetAddress", StringType(), True)
    ]), True),
    StructField("age", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("phoneNumbers", ArrayType(StructType([
        StructField("number", StringType(), True),
        StructField("type", StringType(), True)
    ]), True))
])

json_data = "C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/sample_json_1.json"

df = spark.read.json(json_data, schema=custom_schema, multiLine=True)
df.printSchema()
df.show(5)

