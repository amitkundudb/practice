from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

data = [("Amit", 100),
        ("Suman", 200),
        ("Rahul", 150),
        ("Sneha", 300),
        ("Raj", 250)]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Score", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

window_spec = Window.orderBy("Score")

# window functions
result_df = df.withColumn("row_number", row_number().over(window_spec)) \
              .withColumn("rank", rank().over(window_spec)) \
              .withColumn("dense_rank", dense_rank().over(window_spec)) \
              .withColumn("lead", lead("Score", 1).over(window_spec)) \
              .withColumn("lag", lag("Score", 1).over(window_spec))

result_df.show()
