from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Remove Duplicates") \
    .getOrCreate()

csv_schema = StructType([
    StructField("User ID", IntegerType(), True),
    StructField("Name of the steam game", StringType(), True),
    StructField("behavior name (purchase/play)", StringType(), True),
    StructField("Hours if behavior is play, 1.0 if behavior is purchase", IntegerType(), True)
])

raw_csv_df = spark.read.csv("C:/Users/AmitKundu/Downloads/steam.csv",schema=csv_schema)
csv_df = raw_csv_df.orderBy('User ID')
csv_df.limit(10).show(truncate=False)
print(csv_df.count())

#Removing duplicate elements based on all columns
unique_df=csv_df.distinct().orderBy('User ID')
unique_df.limit(10).show(truncate=False)
print(unique_df.count())