from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dataset to DataFrame Example").getOrCreate()
data = [("Amit", 23), ("Avijit", 20), ("Rahul", 30)]

#Converting RDD from Dataset
rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())