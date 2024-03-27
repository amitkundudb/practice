from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

file_path = "C:/Users/AmitKundu/Downloads/Startups1.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# string functions
df = df.withColumn("City_upper", upper(df["City"])) \
    .withColumn("Company_trimmed", trim(df["Company"])) \
    .withColumn("Founders_lpad", lpad(df["Founders"], 50, " ")) \
    .withColumn("Industries_length", length(df["Industries"])) \
    .withColumn("Description_lower", lower(df["Description"])) \
    .withColumn("City_replace", regexp_replace(df["City"], "Gurgaon", "Delhi")) \
    .withColumn("City_substr", substring(df["City"], 1, 3))

df.show()