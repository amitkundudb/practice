from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("pivot for Region Wise sales").getOrCreate()

data_path = "C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/sample_pivot.csv"
schema = StructType([
    StructField("color", StringType(),True),
    StructField("region", StringType(), True),
    StructField("units", IntegerType(), True),
    StructField("sales", IntegerType(), True)
])

df = spark.read.format("csv").option("header",True).schema(schema).load(data_path)
df.show(5)
total_units_df = df.groupBy("color").agg(sum("units").alias("total_units"))
# total_units_df.show()

pivot_df = df.groupBy("color").pivot("region").sum("sales")
# pivot_df.show()

result_df = pivot_df.join(total_units_df, "color")
result_df.show()