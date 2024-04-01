from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum, desc

spark = SparkSession.builder \
    .appName("Remove Duplicates") \
    .getOrCreate()

csv_schema = StructType([
    StructField("User ID", IntegerType(), True),
    StructField("Name of the steam game", StringType(), True),
    StructField("purchase/play", StringType(), True),
    StructField("Hours play", IntegerType(), True)
])

csv_df = spark.read.csv("C:/Users/AmitKundu/Downloads/steam.csv",schema=csv_schema)
csv_df = csv_df.na.fill({'Hours play': 0})
csv_df.limit(10).show(truncate=False)
print(csv_df.count())

#Top 10 higest time spent user(1 is for purchasing so removing that also)
grouped_df = csv_df.filter(csv_df["Hours play"] > 1)
aggregated_df = grouped_df.groupBy("User ID").agg(sum("Hours play").alias("Total Hours Played"))
aggregated_df.sort(desc('Total Hours Played')).limit(10).show()
print(aggregated_df.count())
