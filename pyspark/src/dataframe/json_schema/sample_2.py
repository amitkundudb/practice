from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder.appName("Custom JSON Schema").getOrCreate()

custom_schema = StructType([
    StructField("users", ArrayType(StructType([
        StructField("emailAddress", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("phoneNumber", StringType(), True),
        StructField("userId", StringType(), True)
    ])), True)
])

json_data = "C:/Users/AmitKundu/PycharmProjects/practice/pyspark/resource/sample_json_2.json"

df = spark.read.json(json_data, schema=custom_schema, multiLine=True)
df.printSchema()
df.show(truncate=False)

# Explode the 'users' array
print("Explode the users Array")
df_exploded = df.select(explode("users").alias("user"))
df_exploded.show(truncate=False)

# Unnest the struct types
df_unnested = df_exploded.select(
    col("user.emailAddress").alias("emailAddress"),
    col("user.firstName").alias("firstName"),
    col("user.lastName").alias("lastName"),
    col("user.phoneNumber").alias("phoneNumber"),
    col("user.userId").alias("userId")
)

#unnested DataFrame
df_unnested.show()
