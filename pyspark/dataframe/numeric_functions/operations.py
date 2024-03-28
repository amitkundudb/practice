from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum, avg, min, max, round, abs,filter
spark = SparkSession.builder \
    .appName("Operations") \
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

#1 is for purchasing so removing that from data for better Accuracy
grouped_df = csv_df.filter(csv_df["Hours play"] > 1)

#As we know the dataset has 1 only for purchases,
#so we will expect to return nothing in records
#And operator ( & )
and_df = csv_df.filter((csv_df["purchase/play"] == "purchase") & (csv_df["Hours play"] == "0"))
and_df.show()

#OR operator ( | )
or_df = csv_df.filter((csv_df["purchase/play"] == "purchase") | (csv_df["Hours play"] == "0"))
or_df.show()

#NOT operator (~)
not_df = csv_df.filter(~((csv_df["purchase/play"] == "purchase") | (csv_df["Hours play"] == 0)))
not_df.show()

