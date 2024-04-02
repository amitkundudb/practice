from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.functions import sum, col, regexp, regexp_replace

spark = SparkSession.builder.appName("pivot for Region Wise sales").getOrCreate()

data_path = "C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/pivot table.csv"
schema = StructType([
    StructField("color", StringType(), True),
    StructField("region", StringType(), True),
    StructField("units", IntegerType(), True),
    StructField("sales", StringType(), True)  # read sales as StringType
])

df = spark.read.format("csv").option("header", True).schema(schema).load(data_path)
df.show(4)

df = df.withColumn("sales", regexp_replace(col("sales"),"[$,]",""))
# Convert sales column to IntegerType
df = df.withColumn("sales", col("sales").cast(IntegerType()))

# Calculate total units for each color
total_units_df = df.groupBy("color").agg(sum("units").alias("total_units"))

# Pivot the data to get region-wise total sales for each color
pivot_df = df.groupBy("color").pivot("region").sum("sales")

# Join the total units information with the pivoted DataFrame
result_df = pivot_df.join(total_units_df, "color")
result_df.show(5)
