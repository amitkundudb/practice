from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import *
# Create SparkSession
spark = SparkSession.builder \
    .appName("Change of Multiple Column Names") \
    .getOrCreate()

# Sample data
data = [('Amit', '', 'Kundu', '2001-03-14', 'M', 24666),
        ('Rahul', 'Sankar', '', '2000-05-19', 'M', 30000),
        ('Sachin', '', 'Srivastava', '1998-09-05', 'M', 40000),
        ('Priya', 'Devi', 'Sharma', '1997-12-01', 'F', 50000),
        ('Neha', 'Rani', 'Patel', '1989-02-17', 'F', 80000)]

# Define schema using StructType
schema = StructType([
    StructField("First_Name", StringType(), True),
    StructField("Middle_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Date_of_Birth", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame before renaming columns
print("DataFrame before renaming columns:")
df.show()

# Define new column names
new_column_names = ["New_First_Name", "New_Middle_Name", "New_Last_Name", "New_Date_of_Birth", "New_Gender", "New_Salary"]

# Rename columns
for i, col in enumerate(df.columns):
    df = df.withColumnRenamed(col, new_column_names[i])

# Show DataFrame after renaming columns
print("DataFrame after renaming columns:")
df.show()