from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("df_to_dataset").getOrCreate()

# data
data = [("1", "Amit"), ("2", "Ravi"), ("3", "Sagar")]

# Creating DataFrame
df = spark.createDataFrame(data, ["id", "value"])

# StructType for schema definition
my_schema = StructType([
  StructField("id", StringType(), True),
  StructField("value", StringType(), True)
])

# Convert DataFrame to Dataset
dataset = df.rdd.map(lambda row: row.asDict()).toDF(schema=my_schema)

# Now you can use the dataset
dataset.show()