from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataFrame to RDD") \
    .getOrCreate()

# Data
data = [(1, 'Amit'), (2, 'Alisha'), (3, 'Ahana')]
#Dataframe
df = spark.createDataFrame(data, ["ID", "Name"])

# Converting DataFrame to RDD
rdd = df.rdd

print(rdd.collect())
