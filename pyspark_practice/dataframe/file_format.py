from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Read Different files with Schema") \
    .getOrCreate()

csv_schema = StructType([
    StructField("Company", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Starting Year", IntegerType(), True)
])

csv_df = spark.read.csv("C:/Users/AmitKundu/Downloads/Startups1.csv", schema=csv_schema)

json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

json_df = spark.read.json("C:/Users/AmitKundu/Downloads/example.json",\
                          schema=json_schema, multiLine=True, allowComments=True)

parquet_df = spark.read.parquet("C:/Users/AmitKundu/Downloads/MT cars.parquet")

csv_df.show()
json_df.show()
parquet_df.show()

csv_df.printSchema()
json_df.printSchema()
parquet_df.printSchema()
