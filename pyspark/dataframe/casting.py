from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

data = [("Amit", 25),
        ("Rajesh", 30),
        ("Chandan", 35)]

df = spark.createDataFrame(data, ["Name", "Age"])

print("Before casting:")
df.printSchema()

#cast()
df = df.withColumn("Age", col("Age").cast("string"))

print("After casting:")
df.printSchema()
df.show()