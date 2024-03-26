from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD to DataFrame").getOrCreate()

rdd = spark.sparkContext.parallelize([(1, 'Amit'), (2, 'Alisha'), (3, 'Anuj')])

# Converting RDD to DataFrame
df = spark.createDataFrame(rdd)
#Converting DataFrame to Dataset
ds = df.alias("Person")
# Show DataFrame
ds.show()