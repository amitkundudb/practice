from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder \
    .appName("Handling Missing Values") \
    .getOrCreate()

data = [("Ahana", 25, None),
        ("Avik", None, 35000),
        ("Amit", 23, 40000),
        ("Raja", None, None)]

columns = ["Name", "Age", "Salary"]
df = spark.createDataFrame(data, columns)

# 1. Filtering Nulls — Cleaning Your Data (Filtering on multiple Columns)
df_filtered = df.filter(df.Age.isNotNull() & df.Salary.isNotNull())

print("DataFrame after filtering nulls:")
df_filtered.show()

# 2. Replacing Nulls — Filling the Gaps
# Replace null values in Age column with 0 and null values in Salary column with "Not Available"
df_replaced = df.fillna({"Age": 0, "Salary": -1})

print("DataFrame after replacing nulls:")
df_replaced.show()

# 3. Using Coalesce — Choosing Non-Null Values
# Create a new column with non-null values from Age and Salary columns
df_coalesced = df.withColumn("Non_Null_Value", coalesce(df.Age, df.Salary))

print("DataFrame after using coalesce:")
df_coalesced.show()

# 4. Using na.drop() to remove rows with null values
df_no_nulls = df.na.drop()

print("DataFrame after dropping rows with nulls:")
df_no_nulls.show()

df.printSchema()