from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

spark = SparkSession.builder.appName("JSON Conversion").getOrCreate()

data = [("Amit", 23), ("Chandan", 35), ("Anita", 40)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# DataFrame columns to JSON format
df_json = df.withColumn("json_data", to_json(struct(col("name"), col("age"))))

df_json.show(truncate=False)
