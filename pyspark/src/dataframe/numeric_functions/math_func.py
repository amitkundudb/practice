from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum, avg, min, max, round, abs
spark = SparkSession.builder \
    .appName("Numeric Functions") \
    .getOrCreate()

csv_schema = StructType([
    StructField("User ID", IntegerType(), True),
    StructField("Name of the steam game", StringType(), True),
    StructField("purchase/play", StringType(), True),
    StructField("Hours play", IntegerType(), True)
])

csv_df = spark.read.csv("C:/Users/AmitKundu/Downloads/steam.csv",schema=csv_schema)
csv_df = csv_df.na.fill({'Hours play': 0})
csv_df.limit(10).show(truncate=False)
print(csv_df.count())

#1 is for purchasing so removing that from data for better Accuracy
grouped_df = csv_df.filter(csv_df["Hours play"] > 1)

# Sum
sum_result = grouped_df.agg(sum('Hours play').alias('sum_hours_play')).collect()[0]['sum_hours_play']
print("Sum of Hours play:", sum_result)

# Average
avg_result = grouped_df.agg(avg('Hours play').alias('avg_hours_play')).collect()[0]['avg_hours_play']
print("Average Hours play:", avg_result)

# Minimum
min_result = grouped_df.agg(min('Hours play').alias('min_hours_play')).collect()[0]['min_hours_play']
print("Minimum Hours play:", min_result)

# Maximum
max_result = grouped_df.agg(max('Hours play').alias('max_hours_play')).collect()[0]['max_hours_play']
print("Maximum Hours play:", max_result)

#Round
data = [(1.243453,),(4.2364562,),(8.436,)]
df = spark.createDataFrame(data, ["value"])
round_result = df.select(round(df['value'],2)).show()

#abs() for absolute value of the given value
data_abs = [(1,), (-2,), (3,), (-4,)]
df_abs = spark.createDataFrame(data_abs, ["value"])
from pyspark.sql.functions import abs
abs_df = df_abs.withColumn('value', abs(df_abs['value']))
abs_df.show(truncate=False)

