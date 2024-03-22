from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RDD to DataFrame") \
    .getOrCreate()

# Sample RDD
rdd = spark.sparkContext.parallelize([(1, 'Amit'), (2, 'Alisha'), (3, 'Anuj')])

# Converting RDD to DataFrame using toDF()
df = rdd.toDF(["ID", "Name"])

# Show DataFrame
df.show()