from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dataframe and way of rename columns") \
    .getOrCreate()

#Reading csv file of Startups list
df = spark.read.csv("C:/Users/AmitKundu/Downloads/Startups1.csv")
df.printSchema()
df.show()