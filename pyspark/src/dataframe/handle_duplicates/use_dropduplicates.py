from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType, StructField,IntegerType

spark = SparkSession.builder.appName("dropDuplicates() use").getOrCreate()

schema = StructType([
    StructField("User ID", IntegerType(), True),
    StructField("Name of the steam game", StringType(), True),
    StructField("behavior name (purchase/play)", StringType(), True),
    StructField("Hours if behavior is play, 1.0 if behavior is purchase", IntegerType(), True)
])

df = spark.read.csv("C:/Users/AmitKundu/Downloads/steam.csv", schema=schema)
df.limit(10).show(truncate=False)
print(df.count())
#Using 3 column to find duplicate records
unique_df = df.dropDuplicates(subset=['User ID','Name of the steam game','behavior name (purchase/play)'])
unique_df.limit(10).show(truncate=False)
print(unique_df.count())

